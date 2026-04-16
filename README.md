# SKEDULER

Queue-based batch processing on Snowflake using Snowpark Container Services (SPCS).

External processes INSERT requests into a Snowflake table. A scheduled Task triggers
an orchestrator that assigns requests to SPCS worker replicas, auto-scaling based on
queue depth. Failed requests are automatically retried or moved to a dead-letter queue.

---

## Architecture

```
                          +-----------------+
                          |  External       |
                          |  Producers      |
                          +--------+--------+
                                   |
                            INSERT / CALL
                            ENQUEUE_REQUEST()
                                   |
                                   v
+----------+   10 min    +-------------------+     EXECUTE JOB SERVICE    +------------------+
|  RUNNER  +------------>| ORCHESTRATE_BATCH +--------------------------->|  SPCS Workers    |
|  _TASK   |  schedule   |  (stored proc)    |     N replicas             |  (containers)    |
+----------+             +---------+---------+                            +--------+---------+
                                   |                                               |
                          reads config from                                 OAuth via token
                          RUNNER_CONFIG                                     file on container
                                   |                                               |
                                   v                                               v
                         +-------------------+                            +------------------+
                         |  REQUEST_QUEUE    |<------ read/update ------->|  handler.py      |
                         +-------------------+                            |  (custom logic)  |
                         |  PROCESS_LOG      |                            +------------------+
                         +-------------------+
                         |  RUNNER_CONFIG    |
                         +-------------------+
```

### Processing Cycle

1. **RUNNER_TASK** fires every 10 minutes (`ALLOW_OVERLAPPING_EXECUTION = FALSE`).
2. **ORCHESTRATE_BATCH** stored procedure:
   - Counts PENDING requests.
   - Reads scaling config from RUNNER_CONFIG.
   - Computes replica count: `CEIL(pending / REQUESTS_PER_INSTANCE)`, clamped to `[MIN_INSTANCES, MAX_INSTANCES]`.
   - Pre-assigns each request to a specific replica via round-robin (`INSTANCE_ID = 0..N-1`).
   - Launches an SPCS Job Service with N replicas.
   - After the job completes, re-queues any requests still in ASSIGNED/PROCESSING state.
3. **Worker containers** (one per replica):
   - Bootstrap via `main.sh` — extracts `app.tar.gz` from a mounted Snowflake stage.
   - `worker.py` discovers its instance ID from the container hostname.
   - Fetches only the requests assigned to its instance ID.
   - Calls `handler.process_request(request, conn)` for each request.
   - Marks requests COMPLETED or FAILED (with retry / dead-letter logic).

### Key Design Decisions

- **Pre-assignment pattern**: requests are assigned to specific replicas *before* the job
  launches, avoiding contention between workers.
- **Stage-mounted code**: the Docker image contains only Python dependencies. Actual
  application code (`app/`) is packaged as a tarball, uploaded to a Snowflake stage, and
  extracted at container startup. This allows code changes without rebuilding the image.
- **Separated handler**: custom processing logic lives in `app/handler.py`. Modify
  `process_request(request, conn)` to implement your workload. The Snowflake connection
  is passed in for data access.
- **Dead-letter queue**: requests exceeding `MAX_RETRIES` are moved to `DEAD_LETTER`
  status instead of being retried forever.

---

## Project Structure

```
.
├── Makefile                    # Deployment automation
├── README.md
├── app/
│   ├── worker.py               # Worker framework (queue management, lifecycle)
│   └── handler.py              # Custom processing logic (edit this file)
├── img/
│   ├── Dockerfile              # Container image (dependencies only)
│   ├── main.sh                 # Entrypoint — loads app code from stage mount
│   └── requirements.txt        # Python dependencies for the container
│   └── worker_spec.yaml        # SPCS job spec template
├── dbs/
│   ├── setup.sql               # Creates all Snowflake objects
│   ├── deploy.sql              # Uploads artifacts and creates Streamlit app
│   └── reset.sql               # Tears down all Snowflake objects
├── streamlit/
│   └── streamlit_app.py        # Monitoring dashboard (Streamlit in Snowsight)
├── .build/                     # Generated artifacts (gitignore)
└── log/                        # Deployment logs (gitignore)
```

---

## Snowflake Objects Created

| Object | Type | Description |
|---|---|---|
| `REQUEST_QUEUE` | Table | Incoming requests with status, priority, retry tracking |
| `PROCESS_LOG` | Table | Audit log of all batch runs and per-request outcomes |
| `RUNNER_CONFIG` | Table | Key-value configuration (scaling, compute pool, image URL) |
| `SPECS` | Stage | Rendered SPCS job specification YAML |
| `VOLUMES` | Stage | Application code tarball (`app.tar.gz`) |
| `STREAMLITS` | Stage | Streamlit application files |
| `IMAGES` | Image Repository | Docker images for worker containers |
| `{XMA_NAME}_POOL` | Compute Pool | SPCS compute pool (CPU_X64_S, 1-5 nodes) |
| `RUNNER_TASK` | Task | Scheduled trigger (10 min, no overlap) |
| `{XMA_NAME}_MONITOR` | Streamlit | Monitoring and configuration dashboard |
| `ORCHESTRATE_BATCH` | Stored Procedure | Batch orchestration logic |
| `ENQUEUE_REQUEST` | Stored Procedure | Insert requests into the queue |
| `QUEUE_STATUS` | Stored Procedure | Queue status summary |
| `UPDATE_CONFIG` | Stored Procedure | Update configuration values |

---

## Configuration (RUNNER_CONFIG)

| Key | Default | Description |
|---|---|---|
| `REQUESTS_PER_INSTANCE` | `2` | Number of requests assigned per worker replica |
| `MAX_INSTANCES` | `10` | Maximum number of worker replicas per batch |
| `MIN_INSTANCES` | `1` | Minimum number of worker replicas per batch |
| `JOB_TIMEOUT_SECS` | `3600` | Job service timeout |
| `COMPUTE_POOL` | `{XMA_NAME}_POOL` | Compute pool for worker containers |
| `MAX_RETRIES` | `3` | Retries before moving to dead-letter |
| `IMAGE_REPO` | *(auto-generated)* | Full image URL for the worker container |

Configuration can be modified via the Streamlit dashboard or:
```sql
CALL UPDATE_CONFIG('REQUESTS_PER_INSTANCE', '5');
```

---

## Prerequisites

- **Snowflake account** with ACCOUNTADMIN access
- **Snowflake CLI** (`snow`) installed and configured with a named connection
- **Docker** or **Podman** for building container images
- **GNU Make**
- **sed** (standard on macOS/Linux)

---

## Makefile Variables

| Variable | Default | Description |
|---|---|---|
| `DBS_NAME` | `apps` | Target Snowflake database |
| `XMA_NAME` | `skeduler` | Target Snowflake schema |
| `CNX_NAME` | `cxname` | Snowflake CLI connection name |
| `ACC_NAME` | `orgname-accname` | Snowflake account identifier |
| `IMG_NAME` | `skeduler_worker` | Docker image name |

Override any variable at invocation:
```bash
make all DBS_NAME=my_db XMA_NAME=my_schema CNX_NAME=myconn ACC_NAME=myorg-myaccount
```

---

## Makefile Targets

| Target | Description |
|---|---|
| `make help` | Show configuration and available targets |
| `make all` | Full deployment: setup + image push + deploy |
| `make setup` | Create all Snowflake objects (schema, tables, SPs, task, pool) |
| `make deploy` | Build image, package app, upload artifacts, create Streamlit |
| `make reset` | Tear down all Snowflake objects |
| `make reset all` | Full teardown and redeploy from scratch |
| `make img_build` | Build Docker image locally |
| `make img_push` | Build, tag, and push image to Snowflake registry |
| `make app_pack` | Package `app/` directory as tarball in `.build/` |
| `make spec_render` | Render spec template with variable substitution |
| `make reg_auth` | Authenticate Docker client with Snowflake image registry |

---

## Quick Start

```bash
# 1. Full deployment (creates objects, builds/pushes image, deploys artifacts)
make all

# 2. Enqueue some test requests
snow sql -c <connection_name> -q "CALL <DB>.<SCHEMA>.ENQUEUE_REQUEST('{\"task\": \"hello\"}', 5)"

# 3. Trigger a batch manually (or wait for the 10-minute schedule)
snow sql -c <connection_name> -q "CALL <DB>.<SCHEMA>.ORCHESTRATE_BATCH()"

# 4. Check results
snow sql -c <connection_name> -q "SELECT * FROM <DB>.<SCHEMA>.PROCESS_LOG ORDER BY STARTED_AT DESC LIMIT 10"
```

---

## Customizing Processing Logic

Edit `app/handler.py` to implement your workload:

```python
def process_request(request, conn):
    # request["request_id"] — unique request identifier
    # request["payload"]    — VARIANT payload (dict/list)
    # conn                  — active Snowflake connection

    payload = request["payload"]
    cursor = conn.cursor()
    cursor.execute("SELECT ...")
    # ... your logic here ...
```

After modifying `handler.py`, redeploy the app code (no image rebuild needed):
```bash
make app_pack snow_deploy
```

If you change Python dependencies in `img/requirements.txt`, rebuild the image:
```bash
make img_push snow_deploy
```

---

## Streamlit Dashboard

The monitoring dashboard is deployed as a Streamlit in Snowsight application. It provides:

- **Dashboard tab** — queue status metrics, recent batch activity, task execution history
- **Queue tab** — browse and filter request queue entries
- **Process Log tab** — audit trail of all processing outcomes
- **Configuration tab** — edit scaling parameters, compute pool, image URL
- **Actions tab** — manually trigger batches, enqueue test requests, control task state, requeue dead letters, purge completed requests

Access it in Snowsight under **Streamlit Apps** or via:
```
https://app.snowflake.com/<account>/#/streamlit-apps/<DB>.<SCHEMA>.<SCHEMA>_STAPP
```

---

## Request Lifecycle

```
PENDING ──> ASSIGNED ──> PROCESSING ──> COMPLETED
   ^            |              |
   |            v              v
   +──── (re-queued) ◄── FAILED (attempts < max_retries)
                              |
                              v
                         DEAD_LETTER (attempts >= max_retries)
```

---

## Teardown

```bash
make reset
```

This drops all objects including the schema, compute pool, image repository, and Streamlit app.
