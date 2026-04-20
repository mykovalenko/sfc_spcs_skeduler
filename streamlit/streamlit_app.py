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

tab_dash, tab_queue, tab_config = st.tabs(
    ["Dashboard", "Queue", "Configuration"]
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
    pool_name = run_query(f"""
        SELECT CONFIG_VALUE FROM {DB}.{XMA}.RUNNER_CONFIG WHERE CONFIG_KEY = 'COMPUTE_POOL'
    """)
    if not pool_name.empty:
        import pandas as pd
        p_name = pool_name["CONFIG_VALUE"].iloc[0]
        pool_info = run_query(f"DESCRIBE COMPUTE POOL {p_name}")
        if not pool_info.empty:
            def pcol(name):
                return [c for c in pool_info.columns if name in c.lower()][0]
            active = int(pool_info[pcol("active_nodes")].iloc[0] or 0)
            max_nodes = int(pool_info[pcol("max_nodes")].iloc[0])
            state = pool_info[pcol("state")].iloc[0]
            family = pool_info[pcol("instance_family")].iloc[0]
            error_cols = [c for c in pool_info.columns if "error_code" in c.lower()]
            error_code = pool_info[error_cols[0]].iloc[0] if error_cols else None
            status_cols = [c for c in pool_info.columns if "status_message" in c.lower()]
            status_msg = pool_info[status_cols[0]].iloc[0] if status_cols else None
            resumed_cols = [c for c in pool_info.columns if "resumed" in c.lower()]
            raw_resumed = pool_info[resumed_cols[0]].iloc[0] if resumed_cols else None
            if raw_resumed and str(raw_resumed) not in ("None", ""):
                resumed_at = pd.Timestamp(raw_resumed).strftime("%Y-%m-%d %H:%M:%S")
            else:
                resumed_at = "N/A"

            cp_col, tc_col, ma_col = st.columns([4, 2, 2])
            with cp_col:
                st.subheader(f"Compute Pool ({p_name})")
                st.markdown(f"**Status:** {state} &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; **Instance family:** {family} &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; **Last resumed:** {resumed_at}")
                if error_code and str(error_code).strip():
                    st.error(f"Error {error_code}: {status_msg or ''}")
                pct = active / max_nodes if max_nodes > 0 else 0
                st.progress(pct)
                st.caption(f"{active} / {max_nodes} nodes")
                if st.button("Refresh", key="refresh_pool"):
                    st.experimental_rerun()
            with tc_col:
                st.subheader("Task Control")
                tb1, tb2, tb3 = st.columns(3)
                with tb1:
                    if st.button("Suspend Task"):
                        session.sql(f"ALTER TASK {DB}.{XMA}.RUNNER_TASK SUSPEND").collect()
                        st.success("Task suspended.")
                with tb2:
                    if st.button("Resume Task"):
                        session.sql(f"ALTER TASK {DB}.{XMA}.RUNNER_TASK RESUME").collect()
                        st.success("Task resumed.")
                with tb3:
                    if st.button("Check Status"):
                        ts_state = run_query(f"SHOW TASKS LIKE 'RUNNER_TASK' IN SCHEMA {DB}.{XMA}")
                        if not ts_state.empty:
                            state_col = [c for c in ts_state.columns if 'state' in c.lower()][0]
                            task_state = ts_state[state_col].iloc[0]
                            st.info(f"Task status: {task_state}")
                            if task_state.lower() == 'started':
                                next_run = run_query(f"""
                                    SELECT SCHEDULED_TIME,
                                           DATEDIFF('second', CURRENT_TIMESTAMP(), SCHEDULED_TIME) AS DDIFF
                                    FROM TABLE({DB}.INFORMATION_SCHEMA.TASK_HISTORY(
                                        TASK_NAME => 'RUNNER_TASK',
                                        SCHEDULED_TIME_RANGE_START => CURRENT_TIMESTAMP(),
                                        SCHEDULED_TIME_RANGE_END => DATEADD('minute', 15, CURRENT_TIMESTAMP())
                                    ))
                                    WHERE STATE = 'SCHEDULED'
                                    ORDER BY SCHEDULED_TIME ASC
                                    LIMIT 1
                                """)
                                if not next_run.empty:
                                    total_secs = int(next_run["DDIFF"].iloc[0])
                                    if total_secs > 0:
                                        mins, secs = divmod(total_secs, 60)
                                        st.metric("Next run in", f"{mins}m {secs}s")
                                    else:
                                        st.info("Task is running now or just completed.")
                                else:
                                    st.caption("No upcoming run found. Task may have just fired.")
                        else:
                            st.warning("Task not found.")
                st.markdown("**Trigger Batch**")
                st.caption("Manually trigger a processing cycle.")
                if st.button("Run Batch Now", type="primary"):
                    with st.spinner("Running orchestrator..."):
                        result = run_query(f"CALL {DB}.{XMA}.ORCHESTRATE_BATCH()")
                    st.success(result.iloc[0, 0])
            with ma_col:
                st.subheader("Enqueue Test Request")
                test_payload = st.text_input("Payload", value='{"test": true}')
                test_priority = st.slider("Priority", 0, 10, 5)
                if st.button("Enqueue"):
                    session.sql(
                        f"CALL {DB}.{XMA}.ENQUEUE_REQUEST('{test_payload}', {test_priority})"
                    ).collect()
                    st.success("Request enqueued.")
                    st.experimental_rerun()
        else:
            st.subheader("Compute Pool")
            st.warning(f"Compute pool '{p_name}' not found.")

    st.subheader("Request Queue")
    rq_act, _, rq_filter = st.columns([1, 2, 1])
    with rq_act:
        st.caption("Reset DEAD_LETTER requests back to PENDING.")
        if st.button("Requeue All Dead Letters"):
            session.sql(f"""
                UPDATE {DB}.{XMA}.REQUEST_QUEUE
                SET STATUS = 'PENDING', ATTEMPT_COUNT = 0,
                    INSTANCE_ID = NULL, CLAIMED_AT = NULL, ERROR_MESSAGE = NULL
                WHERE STATUS = 'DEAD_LETTER'
            """).collect()
            st.success("Dead letters requeued.")
            st.experimental_rerun()
    with rq_filter:
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

    st.subheader("Process Log")
    pl_act, _, pl_filter = st.columns([1, 2, 1])
    with pl_act:
        st.caption("Delete all COMPLETED requests from the queue.")
        if st.button("Purge Completed Requests", type="secondary"):
            session.sql(f"DELETE FROM {DB}.{XMA}.REQUEST_QUEUE WHERE STATUS = 'COMPLETED'").collect()
            st.success("Completed requests purged.")
            st.experimental_rerun()
    with pl_filter:
        log_status_filter = st.multiselect(
            "Filter by status",
            ["COMPLETED", "FAILED", "PROCESSING"],
            default=["COMPLETED", "FAILED", "PROCESSING"],
            key="log_status_filter",
        )

    if log_status_filter:
        log_placeholders = ", ".join([f"'{s}'" for s in log_status_filter])
        log_data = run_query(f"""
            SELECT BATCH_ID, REQUEST_ID, INSTANCE_ID, STATUS,
                   STARTED_AT, FINISHED_AT, ERROR_MESSAGE
            FROM {DB}.{XMA}.PROCESS_LOG
            WHERE STATUS IN ({log_placeholders})
            ORDER BY STARTED_AT DESC
            LIMIT 200
        """)
        st.dataframe(log_data, use_container_width=True)
    else:
        st.info("Select at least one status to view log entries.")

with tab_config:
    st.subheader("Scheduler Configuration")

    config_data = run_query(f"""
        SELECT CONFIG_KEY, CONFIG_VALUE
        FROM {DB}.{XMA}.RUNNER_CONFIG
        ORDER BY CONFIG_KEY
    """)

    config_map = dict(zip(config_data["CONFIG_KEY"], config_data["CONFIG_VALUE"])) if not config_data.empty else {}

    families_df = run_query("SHOW COMPUTE POOL INSTANCE FAMILIES")
    family_list = families_df.iloc[:, 0].tolist() if not families_df.empty else ["CPU_X64_S"]

    images_df = run_query(f"SHOW IMAGES IN IMAGE REPOSITORY {DB}.{XMA}.IMAGES")
    current_image_repo = config_map.get("IMAGE_REPO", "")
    if not images_df.empty:
        path_col = [c for c in images_df.columns if "image_path" in c.lower()]
        if path_col:
            acct_prefix = current_image_repo.split(".registry.snowflakecomputing.com/")[0] if ".registry.snowflakecomputing.com/" in current_image_repo else ""
            image_list = [
                acct_prefix + ".registry.snowflakecomputing.com/" + p if acct_prefix else p
                for p in images_df[path_col[0]].tolist()
            ]
        else:
            image_list = [current_image_repo] if current_image_repo else []
    else:
        image_list = [current_image_repo] if current_image_repo else []

    pool_for_family = config_map.get("COMPUTE_POOL", "SKEDULER_POOL")
    pool_desc = run_query(f"DESCRIBE COMPUTE POOL {pool_for_family}")
    current_family = "CPU_X64_S"
    if not pool_desc.empty:
        fam_col = [c for c in pool_desc.columns if "instance_family" in c.lower()]
        if fam_col:
            current_family = pool_desc[fam_col[0]].iloc[0]

    with st.form("config_form"):
        st.markdown("**Edit configuration values:**")

        cp_col1, cp_col2, cp_col3 = st.columns([4, 1, 1])
        with cp_col1:
            new_pool = st.text_input(
                "Compute pool",
                value=config_map.get("COMPUTE_POOL", "SKEDULER_POOL"),
            )
        with cp_col2:
            family_idx = family_list.index(current_family) if current_family in family_list else 0
            new_family = st.selectbox(
                "Instance family",
                options=family_list,
                index=family_idx,
            )
        with cp_col3:
            st.markdown("")
            st.markdown("")
            alter_family = st.form_submit_button("Change Type")
        image_idx = image_list.index(current_image_repo) if current_image_repo in image_list else 0
        new_image = st.selectbox(
            "Image",
            options=image_list,
            index=image_idx,
        )
        new_min = st.number_input(
            "Min instances",
            min_value=1, max_value=10,
            value=int(config_map.get("MIN_INSTANCES", 1)),
        )
        new_max = st.number_input(
            "Max instances",
            min_value=1, max_value=50,
            value=int(config_map.get("MAX_INSTANCES", 10)),
        )
        new_rpi = st.number_input(
            "Requests per instance",
            min_value=1, max_value=100,
            value=int(config_map.get("REQUESTS_PER_INSTANCE", 2)),
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

        submitted = st.form_submit_button("Save Configuration", type="primary")

        if alter_family:
            session.sql(
                f"ALTER COMPUTE POOL {new_pool} SET INSTANCE_FAMILY = {new_family}"
            ).collect()
            st.success(f"Compute pool updated to {new_family}.")
            st.experimental_rerun()

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
                session.sql(f"""
                    MERGE INTO {DB}.{XMA}.RUNNER_CONFIG t
                    USING (SELECT '{key}' AS CONFIG_KEY, '{val}' AS CONFIG_VALUE) s
                    ON t.CONFIG_KEY = s.CONFIG_KEY
                    WHEN MATCHED THEN UPDATE SET CONFIG_VALUE = s.CONFIG_VALUE
                    WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE) VALUES (s.CONFIG_KEY, s.CONFIG_VALUE)
                """).collect()
            st.success("Configuration saved.")
            st.experimental_rerun()


