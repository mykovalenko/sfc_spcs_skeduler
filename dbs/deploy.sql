SET TARGET_DATABASE = '&{ dbsname }';
SET DEPLOY_SCHEMA   = '&{ xmaname }';

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($TARGET_DATABASE);
USE SCHEMA IDENTIFIER($DEPLOY_SCHEMA);

-- Upload rendered spec
REMOVE @SPECS PATTERN='.*yaml';
PUT file://.build/worker_spec.yaml @SPECS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Upload app code
REMOVE @VOLUMES PATTERN='.*gz';
PUT file://.build/app.tar.gz @VOLUMES AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Upload Streamlit app
REMOVE @STREAMLITS PATTERN='.*py';
PUT file://streamlit/streamlit_app.py @STREAMLITS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Resume compute pool
ALTER COMPUTE POOL IF EXISTS &{ xmaname }_POOL RESUME IF SUSPENDED;

-- Recreate Streamlit app
CREATE OR REPLACE STREAMLIT &{ xmaname }_MONITOR
    ROOT_LOCATION = '@STREAMLITS'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH'
    TITLE = '&{ xmaname } Monitor'
    COMMENT = 'Queue processor monitoring and configuration dashboard';

-- Resume task
ALTER TASK RUNNER_TASK RESUME;
