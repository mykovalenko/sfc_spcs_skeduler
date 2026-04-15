import streamlit as st

st.set_page_config(page_title="SKEDULER Monitor", page_icon=":gear:", layout="wide")


def get_session():
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        from snowflake.snowpark import Session
        import os
        conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
        return Session.builder.config("connection_name", conn_name).create()


session = get_session()

DB = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
XMA = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]


def run_query(sql):
    return session.sql(sql).to_pandas()


st.title(":cyclone: SKEDULER")

tab_dash, tab_queue, tab_log, tab_config, tab_actions = st.tabs(
    ["Dashboard", "Queue", "Process Log", "Configuration", "Actions"]
)

with tab_dash:
    st.subheader("Queue Overview")

    status_counts = run_query(f"""
        SELECT STATUS, COUNT(*) AS CNT
        FROM {DB}.{XMA}.REQUEST_QUEUE
        GROUP BY STATUS
        ORDER BY STATUS
    """)

    status_map = dict(zip(status_counts["STATUS"], status_counts["CNT"])) if not status_counts.empty else {}

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Pending", status_map.get("PENDING", 0))
    m2.metric("Assigned", status_map.get("ASSIGNED", 0))
    m3.metric("Processing", status_map.get("PROCESSING", 0))
    m4.metric("Completed", status_map.get("COMPLETED", 0))
    m5.metric("Failed / Dead Letter", status_map.get("DEAD_LETTER", 0))

    st.subheader("Recent Batch Activity")
    recent_batches = run_query(f"""
        SELECT BATCH_ID,
               MIN(STARTED_AT) AS STARTED,
               MAX(FINISHED_AT) AS FINISHED,
               COUNT(DISTINCT REQUEST_ID) AS REQUESTS,
               SUM(CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS COMPLETED,
               SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED
        FROM {DB}.{XMA}.PROCESS_LOG
        WHERE BATCH_ID IS NOT NULL
        GROUP BY BATCH_ID
        ORDER BY STARTED DESC
        LIMIT 20
    """)
    st.dataframe(recent_batches, use_container_width=True)

    st.subheader("Task Status")
    task_info = run_query(f"""
        SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, NEXT_SCHEDULED_TIME,
               ERROR_CODE, ERROR_MESSAGE
        FROM TABLE({DB}.INFORMATION_SCHEMA.TASK_HISTORY(
            TASK_NAME => 'RUNNER_TASK',
            SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
        ))
        ORDER BY SCHEDULED_TIME DESC
        LIMIT 10
    """)
    if not task_info.empty:
        st.dataframe(task_info, use_container_width=True)
    else:
        st.info("No task history found in the last 24 hours.")

with tab_queue:
    st.subheader("Request Queue")

    col_filter, _ = st.columns([1, 3])
    with col_filter:
        status_filter = st.multiselect(
            "Filter by status",
            ["PENDING", "ASSIGNED", "PROCESSING", "COMPLETED", "DEAD_LETTER"],
            default=["PENDING", "ASSIGNED", "PROCESSING"]
        )

    if status_filter:
        placeholders = ", ".join([f"'{s}'" for s in status_filter])
        queue_data = run_query(f"""
            SELECT REQUEST_ID, STATUS, PRIORITY, PAYLOAD::VARCHAR AS PAYLOAD, INSTANCE_ID,
                   ATTEMPT_COUNT, MAX_RETRIES, ERROR_MESSAGE,
                   CREATED_AT, CLAIMED_AT, COMPLETED_AT
            FROM {DB}.{XMA}.REQUEST_QUEUE
            WHERE STATUS IN ({placeholders})
            ORDER BY CREATED_AT DESC
            LIMIT 200
        """)
        st.dataframe(queue_data, use_container_width=True)
    else:
        st.info("Select at least one status to view queue entries.")

with tab_log:
    st.subheader("Process Log")
    log_data = run_query(f"""
        SELECT BATCH_ID, REQUEST_ID, INSTANCE_ID, STATUS,
               STARTED_AT, FINISHED_AT, ERROR_MESSAGE
        FROM {DB}.{XMA}.PROCESS_LOG
        ORDER BY STARTED_AT DESC
        LIMIT 200
    """)
    st.dataframe(log_data, use_container_width=True)

with tab_config:
    st.subheader("Scheduler Configuration")

    config_data = run_query(f"""
        SELECT CONFIG_KEY, CONFIG_VALUE
        FROM {DB}.{XMA}.RUNNER_CONFIG
        ORDER BY CONFIG_KEY
    """)

    config_map = dict(zip(config_data["CONFIG_KEY"], config_data["CONFIG_VALUE"])) if not config_data.empty else {}

    with st.form("config_form"):
        st.markdown("**Edit configuration values:**")

        new_rpi = st.number_input(
            "Requests per instance",
            min_value=1, max_value=100,
            value=int(config_map.get("REQUESTS_PER_INSTANCE", 2)),
        )
        new_max = st.number_input(
            "Max instances",
            min_value=1, max_value=50,
            value=int(config_map.get("MAX_INSTANCES", 10)),
        )
        new_min = st.number_input(
            "Min instances",
            min_value=1, max_value=10,
            value=int(config_map.get("MIN_INSTANCES", 1)),
        )
        new_timeout = st.number_input(
            "Job timeout (seconds)",
            min_value=60, max_value=86400,
            value=int(config_map.get("JOB_TIMEOUT_SECS", 3600)),
        )
        new_retries = st.number_input(
            "Max retries",
            min_value=0, max_value=20,
            value=int(config_map.get("MAX_RETRIES", 3)),
        )
        new_pool = st.text_input(
            "Compute pool",
            value=config_map.get("COMPUTE_POOL", "SKEDULER_POOL"),
        )
        new_image = st.text_input(
            "Image repo URL",
            value=config_map.get("IMAGE_REPO", ""),
        )

        submitted = st.form_submit_button("Save Configuration")

        if submitted:
            updates = {
                "REQUESTS_PER_INSTANCE": str(new_rpi),
                "MAX_INSTANCES": str(new_max),
                "MIN_INSTANCES": str(new_min),
                "JOB_TIMEOUT_SECS": str(new_timeout),
                "MAX_RETRIES": str(new_retries),
                "COMPUTE_POOL": new_pool,
                "IMAGE_REPO": new_image,
            }
            for key, val in updates.items():
                session.sql(
                    f"UPDATE {DB}.{XMA}.RUNNER_CONFIG SET CONFIG_VALUE = '{val}' WHERE CONFIG_KEY = '{key}'"
                ).collect()
            st.success("Configuration saved.")
            st.experimental_rerun()

with tab_actions:
    st.subheader("Manual Actions")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Trigger Batch**")
        st.caption("Manually trigger a processing cycle (calls ORCHESTRATE_BATCH).")
        if st.button("Run Batch Now", type="primary"):
            with st.spinner("Running orchestrator..."):
                result = run_query(f"CALL {DB}.{XMA}.ORCHESTRATE_BATCH()")
            st.success(result.iloc[0, 0])

    with col2:
        st.markdown("**Enqueue Test Request**")
        test_payload = st.text_input("Payload", value='{"test": true}')
        test_priority = st.slider("Priority", 0, 10, 5)
        if st.button("Enqueue"):
            session.sql(
                f"CALL {DB}.{XMA}.ENQUEUE_REQUEST('{test_payload}', {test_priority})"
            ).collect()
            st.success("Request enqueued.")
            st.experimental_rerun()

    st.divider()

    st.markdown("**Task Control**")
    tc1, tc2, tc3 = st.columns(3)
    with tc1:
        if st.button("Resume Task"):
            session.sql(f"ALTER TASK {DB}.{XMA}.RUNNER_TASK RESUME").collect()
            st.success("Task resumed.")
    with tc2:
        if st.button("Suspend Task"):
            session.sql(f"ALTER TASK {DB}.{XMA}.RUNNER_TASK SUSPEND").collect()
            st.success("Task suspended.")
    with tc3:
        if st.button("Check Task State"):
            state = run_query(f"SHOW TASKS LIKE 'RUNNER_TASK' IN SCHEMA {DB}.{XMA}")
            if not state.empty:
                state_col = [c for c in state.columns if 'state' in c.lower()][0]
                st.info(f"Task state: {state[state_col].iloc[0]}")

    st.divider()

    st.markdown("**Requeue Dead Letters**")
    st.caption("Reset DEAD_LETTER requests back to PENDING for reprocessing.")
    if st.button("Requeue All Dead Letters"):
        session.sql(f"""
            UPDATE {DB}.{XMA}.REQUEST_QUEUE
            SET STATUS = 'PENDING', ATTEMPT_COUNT = 0,
                INSTANCE_ID = NULL, CLAIMED_AT = NULL, ERROR_MESSAGE = NULL
            WHERE STATUS = 'DEAD_LETTER'
        """).collect()
        st.success("Dead letters requeued.")
        st.experimental_rerun()

    st.divider()

    st.markdown("**Purge Completed**")
    st.caption("Delete all COMPLETED requests from the queue.")
    if st.button("Purge Completed Requests", type="secondary"):
        session.sql(f"DELETE FROM {DB}.{XMA}.REQUEST_QUEUE WHERE STATUS = 'COMPLETED'").collect()
        st.success("Completed requests purged.")
        st.experimental_rerun()
