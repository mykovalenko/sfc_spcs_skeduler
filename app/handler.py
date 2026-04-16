import logging
import time

log = logging.getLogger("skeduler-handler")


def process_request(request, conn):
    log.info("Processing request %s with payload: %s", request["request_id"], request["payload"])
    time.sleep(5)
    log.info("Finished processing request %s", request["request_id"])
