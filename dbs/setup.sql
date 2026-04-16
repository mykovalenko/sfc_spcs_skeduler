SET TARGET_DATABASE = '&{ dbsname }';
SET DEPLOY_SCHEMA   = '&{ xmaname }';

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS IDENTIFIER($TARGET_DATABASE);
USE DATABASE IDENTIFIER($TARGET_DATABASE);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($DEPLOY_SCHEMA);
USE SCHEMA IDENTIFIER($DEPLOY_SCHEMA);

CREATE STAGE IF NOT EXISTS SPECS
    ENCRYPTION = (TYPE='SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

CREATE STAGE IF NOT EXISTS VOLUMES
    ENCRYPTION = (TYPE='SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

CREATE STAGE IF NOT EXISTS STREAMLITS
    ENCRYPTION = (TYPE='SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE);

CREATE IMAGE REPOSITORY IF NOT EXISTS IMAGES;

CREATE COMPUTE POOL IF NOT EXISTS &{ xmaname }_POOL
    MIN_NODES = 1
    MAX_NODES = 5
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 300
    INSTANCE_FAMILY = CPU_X64_S;

CREATE TABLE IF NOT EXISTS REQUEST_QUEUE (
    REQUEST_ID    VARCHAR DEFAULT UUID_STRING(),
    PAYLOAD       VARIANT,
    STATUS        VARCHAR DEFAULT 'PENDING',
    PRIORITY      INT DEFAULT 5,
    INSTANCE_ID   INT,
    ATTEMPT_COUNT INT DEFAULT 0,
    MAX_RETRIES   INT DEFAULT 3,
    ERROR_MESSAGE VARCHAR,
    CREATED_AT    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CLAIMED_AT    TIMESTAMP_NTZ,
    COMPLETED_AT  TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS PROCESS_LOG (
    LOG_ID        VARCHAR DEFAULT UUID_STRING(),
    BATCH_ID      VARCHAR,
    REQUEST_ID    VARCHAR,
    INSTANCE_ID   INT,
    STATUS        VARCHAR,
    STARTED_AT    TIMESTAMP_NTZ,
    FINISHED_AT   TIMESTAMP_NTZ,
    ERROR_MESSAGE VARCHAR
);

CREATE TABLE IF NOT EXISTS RUNNER_CONFIG (
    CONFIG_KEY   VARCHAR PRIMARY KEY,
    CONFIG_VALUE VARCHAR
);

MERGE INTO RUNNER_CONFIG t USING (
    SELECT * FROM (VALUES
        ('REQUESTS_PER_INSTANCE', '2'),
        ('MAX_INSTANCES', '10'),
        ('MIN_INSTANCES', '1'),
        ('JOB_TIMEOUT_SECS', '3600'),
        ('COMPUTE_POOL', '&{ xmaname }_POOL'),
        ('MAX_RETRIES', '3'),
        ('IMAGE_REPO', 'placeholder')
    ) AS v(CONFIG_KEY, CONFIG_VALUE)
) s ON t.CONFIG_KEY = s.CONFIG_KEY
WHEN NOT MATCHED THEN INSERT (CONFIG_KEY, CONFIG_VALUE) VALUES (s.CONFIG_KEY, s.CONFIG_VALUE);

CREATE OR REPLACE PROCEDURE ENQUEUE_REQUEST(p_payload VARCHAR, p_priority INT)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
BEGIN
    INSERT INTO REQUEST_QUEUE (PAYLOAD, PRIORITY)
    SELECT PARSE_JSON(:p_payload), :p_priority;
    RETURN ''Request enqueued'';
END;
';

CREATE OR REPLACE PROCEDURE QUEUE_STATUS()
RETURNS TABLE(STATUS VARCHAR, CNT INT)
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
BEGIN
    LET rs RESULTSET := (SELECT STATUS, COUNT(*) AS CNT FROM REQUEST_QUEUE GROUP BY STATUS);
    RETURN TABLE(rs);
END;
';

CREATE OR REPLACE PROCEDURE UPDATE_CONFIG(p_key VARCHAR, p_value VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
BEGIN
    UPDATE RUNNER_CONFIG SET CONFIG_VALUE = :p_value WHERE CONFIG_KEY = :p_key;
    RETURN ''Config updated: '' || :p_key || '' = '' || :p_value;
END;
';

CREATE OR REPLACE PROCEDURE ORCHESTRATE_BATCH()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
'
DECLARE
    v_pending_count INT;
    v_requests_per_instance INT;
    v_max_instances INT;
    v_min_instances INT;
    v_job_timeout INT;
    v_compute_pool VARCHAR;
    v_num_replicas INT;
    v_batch_id VARCHAR;
    v_job_name VARCHAR;
    v_requeued_count INT;
    v_assigned_count INT;
    v_max_to_assign INT;
    v_database VARCHAR;
    v_schema VARCHAR;
BEGIN

    v_database := CURRENT_DATABASE();
    v_schema := CURRENT_SCHEMA();

    SELECT COUNT(*) INTO v_pending_count
    FROM REQUEST_QUEUE
    WHERE STATUS = ''PENDING'';

    IF (v_pending_count = 0) THEN
        RETURN ''No pending requests. Skipping.'';
    END IF;

    SELECT CONFIG_VALUE::INT INTO v_requests_per_instance
    FROM RUNNER_CONFIG WHERE CONFIG_KEY = ''REQUESTS_PER_INSTANCE'';

    SELECT CONFIG_VALUE::INT INTO v_max_instances
    FROM RUNNER_CONFIG WHERE CONFIG_KEY = ''MAX_INSTANCES'';

    SELECT CONFIG_VALUE::INT INTO v_min_instances
    FROM RUNNER_CONFIG WHERE CONFIG_KEY = ''MIN_INSTANCES'';

    SELECT CONFIG_VALUE::INT INTO v_job_timeout
    FROM RUNNER_CONFIG WHERE CONFIG_KEY = ''JOB_TIMEOUT_SECS'';

    SELECT CONFIG_VALUE INTO v_compute_pool
    FROM RUNNER_CONFIG WHERE CONFIG_KEY = ''COMPUTE_POOL'';

    v_num_replicas := CEIL(v_pending_count / v_requests_per_instance);
    IF (v_num_replicas < v_min_instances) THEN
        v_num_replicas := v_min_instances;
    END IF;
    IF (v_num_replicas > v_max_instances) THEN
        v_num_replicas := v_max_instances;
    END IF;
    IF (v_num_replicas > v_pending_count) THEN
        v_num_replicas := v_pending_count;
    END IF;

    v_batch_id := REPLACE(UUID_STRING(), ''-'', '''');
    v_job_name := v_database || ''.'' || v_schema || ''.WORKER_JOB_'' || v_batch_id;
    v_max_to_assign := v_num_replicas * v_requests_per_instance;

    CREATE OR REPLACE TEMPORARY TABLE _ASSIGNMENT AS
    SELECT REQUEST_ID,
           MOD(ROW_NUMBER() OVER (ORDER BY PRIORITY DESC, CREATED_AT ASC) - 1, :v_num_replicas) AS REPLICA_ID
    FROM REQUEST_QUEUE
    WHERE STATUS = ''PENDING''
    ORDER BY PRIORITY DESC, CREATED_AT ASC
    LIMIT :v_max_to_assign;

    UPDATE REQUEST_QUEUE rq
    SET rq.STATUS = ''ASSIGNED'',
        rq.CLAIMED_AT = CURRENT_TIMESTAMP(),
        rq.ATTEMPT_COUNT = rq.ATTEMPT_COUNT + 1,
        rq.INSTANCE_ID = a.REPLICA_ID
    FROM _ASSIGNMENT a
    WHERE rq.REQUEST_ID = a.REQUEST_ID;

    SELECT COUNT(*) INTO v_assigned_count
    FROM REQUEST_QUEUE WHERE STATUS = ''ASSIGNED'';

    INSERT INTO PROCESS_LOG (BATCH_ID, STATUS, STARTED_AT)
    VALUES (:v_batch_id, ''BATCH_STARTED'', CURRENT_TIMESTAMP());

    EXECUTE IMMEDIATE
        ''EXECUTE JOB SERVICE
           IN COMPUTE POOL '' || v_compute_pool || ''
           NAME = '' || v_job_name || ''
           REPLICAS = '' || v_num_replicas::VARCHAR || ''
           QUERY_WAREHOUSE = COMPUTE_WH
           FROM @'' || v_database || ''.'' || v_schema || ''.SPECS
           SPECIFICATION_TEMPLATE_FILE = ''''worker_spec.yaml''''
           USING (
               batchid => '''''' || v_batch_id || ''''''
           )'';

    UPDATE PROCESS_LOG
    SET STATUS = ''BATCH_COMPLETED'', FINISHED_AT = CURRENT_TIMESTAMP()
    WHERE BATCH_ID = :v_batch_id AND REQUEST_ID IS NULL;

    UPDATE REQUEST_QUEUE
    SET STATUS = ''PENDING'',
        INSTANCE_ID = NULL,
        CLAIMED_AT = NULL,
        ERROR_MESSAGE = ''Job did not process this request - re-queued by orchestrator''
    WHERE STATUS IN (''ASSIGNED'', ''PROCESSING'');

    SELECT COUNT(*) INTO v_requeued_count
    FROM REQUEST_QUEUE
    WHERE STATUS = ''PENDING''
      AND ERROR_MESSAGE LIKE ''%re-queued%'';

    RETURN ''Batch '' || v_batch_id || ''. Pending='' || v_pending_count ||
           '' Assigned='' || v_assigned_count ||
           '' Replicas='' || v_num_replicas || '' Requeued='' || v_requeued_count;
END;
';

CREATE OR REPLACE TASK RUNNER_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    ALLOW_OVERLAPPING_EXECUTION = FALSE
AS
    CALL ORCHESTRATE_BATCH();

ALTER TASK RUNNER_TASK RESUME;
