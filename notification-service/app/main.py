"""MaltaShop Notification Service HTTP API (probes and a read-only demo endpoint) and event-consumer wiring."""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response

from . import logic, messaging
from .logging_setup import setup_logging

BROKER_URL = os.environ["BROKER_URL"]
log = setup_logging("notification-service")


def publish(routing_key, payload):
    messaging.publish(BROKER_URL, routing_key, payload)


@asynccontextmanager
async def lifespan(app):
    logic.init_db()
    messaging.start_consumer(
        BROKER_URL, "notification", logic.BINDINGS,
        lambda key, payload: logic.handle_event(key, payload, publish))
    yield


app = FastAPI(title="MaltaShop Notification Service", lifespan=lifespan)


@app.get("/notifications")
def list_data():
    return logic.list_notifications()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ready")
def ready(response: Response):
    if not messaging.ready.is_set():
        response.status_code = 503
        return {"status": "not ready"}
    return {"status": "ready"}
