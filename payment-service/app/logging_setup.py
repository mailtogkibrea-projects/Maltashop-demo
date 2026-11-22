"""Structured JSON logging. Every line carries the mandatory fields below.

Mandatory schema: timestamp, level, service, correlation_id, message
(plus optional business context passed as extra={"ctx": {...}}).
The correlation ID lives in a context variable, so any code that runs while
handling a request or an event logs it without having to pass it around.
"""
import contextvars
import json
import logging
import os
import sys
from datetime import datetime, timezone

correlation_id = contextvars.ContextVar("correlation_id", default="-")


class JsonFormatter(logging.Formatter):
    def __init__(self, service):
        super().__init__()
        self.service = service

    def format(self, record):
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "correlation_id": correlation_id.get(),
            "message": record.getMessage(),
        }
        entry.update(getattr(record, "ctx", {}))
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


def setup_logging(service):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    # Route uvicorn's own logs through the same JSON handler.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        lg = logging.getLogger(name)
        lg.handlers = []
        lg.propagate = True
    return logging.getLogger(service)
