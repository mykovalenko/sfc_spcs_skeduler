import os
import sys
import logging
import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("skeduler-worker")

DATABASE = os.getenv("SKEDULER_DATABASE", "MK_LABS")
SCHEMA = os.getenv("SKEDULER_SCHEMA", "SKEDULER")

SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SERVER_SIDE_TOKEN_PATH = "/snowflake/session/token"


def get_login_token():
    with open(SERVER_SIDE_TOKEN_PATH) as f:
        return f.read().strip()


def get_connection():
    return snowflake.connector.connect(
        host=SNOWFLAKE_HOST,
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        authenticator="oauth",
        token=get_login_token(),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=DATABASE,
        schema=SCHEMA,
    )


def get_instance_id():
    for path in ["/etc/snowflake/server_instance_id"]:
        try:
            with open(path) as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            pass
    hostname = os.popen("hostname").read().strip()
    parts = hostname.rsplit("-", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return int(parts[1])
    return 0


def fetch_assigned_requests(conn, instance_id):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT REQUEST_ID, PAYLOAD
        FROM REQUEST_QUEUE
        WHERE STATUS = 'ASSIGNED' AND INSTANCE_ID = %s
        ORDER BY PRIORITY DESC, CREATED_AT ASC
        """,
        (instance_id,),
    )
    return [{"request_id": r[0], "payload": r[1]} for r in cur.fetchall()]


def mark_processing(conn, request_id):
    conn.cursor().execute(
        """
        UPDATE REQUEST_QUEUE
        SET STATUS = 'PROCESSING', CLAIMED_AT = CURRENT_TIMESTAMP()
        WHERE REQUEST_ID = %s
        """,
        (request_id,),
    )



def mark_completed(conn, request_id, batch_id, instance_id):
    conn.cursor().execute(
        """
        UPDATE REQUEST_QUEUE
        SET STATUS = 'COMPLETED', COMPLETED_AT = CURRENT_TIMESTAMP()
        WHERE REQUEST_ID = %s
        """,
        (request_id,),
    )
    conn.cursor().execute(
        """
        INSERT INTO PROCESS_LOG (BATCH_ID, REQUEST_ID, INSTANCE_ID, STATUS, STARTED_AT, FINISHED_AT)
        SELECT %s, REQUEST_ID, INSTANCE_ID, 'COMPLETED', CLAIMED_AT, CURRENT_TIMESTAMP()
        FROM REQUEST_QUEUE WHERE REQUEST_ID = %s
        """,
        (batch_id, request_id),
    )


def mark_failed(conn, request_id, batch_id, instance_id, error_msg):
    cur = conn.cursor()
    cur.execute(
        "SELECT ATTEMPT_COUNT, MAX_RETRIES FROM REQUEST_QUEUE WHERE REQUEST_ID = %s",
        (request_id,),
    )
    row = cur.fetchone()
    attempts, max_retries = row[0], row[1]

    new_status = "PENDING" if attempts < max_retries else "DEAD_LETTER"

    cur.execute(
        """
        UPDATE REQUEST_QUEUE
        SET STATUS = %s,
            ERROR_MESSAGE = %s,
            INSTANCE_ID = NULL,
            CLAIMED_AT = NULL
        WHERE REQUEST_ID = %s
        """,
        (new_status, error_msg, request_id),
    )
    cur.execute(
        """
        INSERT INTO PROCESS_LOG (BATCH_ID, REQUEST_ID, INSTANCE_ID, STATUS, STARTED_AT, FINISHED_AT, ERROR_MESSAGE)
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), %s)
        """,
        (batch_id, request_id, instance_id, "FAILED", error_msg),
    )


def main():
    batch_id = os.getenv("BATCH_ID", "unknown")
    instance_id = get_instance_id()

    log.info("Worker starting — batch_id=%s instance_id=%d db=%s schema=%s", batch_id, instance_id, DATABASE, SCHEMA)

    conn = get_connection()

    requests = fetch_assigned_requests(conn, instance_id)
    if not requests:
        log.info("No assigned requests for instance %d. Exiting.", instance_id)
        conn.close()
        sys.exit(0)

    log.info("Found %d assigned requests for instance %d", len(requests), instance_id)

    failed = False
    for request in requests:
        try:
            mark_processing(conn, request["request_id"])
            from handler import process_request
            process_request(request, conn)
            mark_completed(conn, request["request_id"], batch_id, instance_id)
            log.info("Request %s completed successfully.", request["request_id"])
        except Exception as e:
            log.error("Request %s failed: %s", request["request_id"], e)
            mark_failed(conn, request["request_id"], batch_id, instance_id, str(e))
            failed = True

    conn.close()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
