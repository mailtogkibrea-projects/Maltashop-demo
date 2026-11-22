"""Order service HTTP API (FastAPI) and event-consumer wiring."""
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException, Response
from pydantic import BaseModel, Field

from . import logic, messaging
from .logging_setup import correlation_id, setup_logging

BROKER_URL = os.environ["BROKER_URL"]
log = setup_logging("order-service")


def publish(routing_key, payload):
    messaging.publish(BROKER_URL, routing_key, payload)


@asynccontextmanager
async def lifespan(app):
    logic.init_db()
    messaging.start_consumer(
        BROKER_URL, "order", logic.BINDINGS,
        lambda key, payload: logic.handle_event(key, payload, publish))
    yield


app = FastAPI(title="MaltaShop Order Service", lifespan=lifespan)


class Item(BaseModel):
    sku: str = Field(min_length=1, max_length=40)
    quantity: int = Field(gt=0, le=100)
    unit_price_cents: int = Field(gt=0)


class OrderIn(BaseModel):
    customer_email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    items: list[Item] = Field(min_length=1, max_length=50)
    card_token: str = Field(min_length=1, max_length=64)  # provider token, never a card number


@app.post("/orders", status_code=202)
def place_order(
    body: OrderIn,
    response: Response,
    idempotency_key: str | None = Header(default=None),
    x_correlation_id: str | None = Header(default=None),
):
    """Accept an order. 202 = accepted, the saga completes asynchronously.
    Poll GET /orders/{id} for the outcome."""
    cid = x_correlation_id or str(uuid.uuid4())
    correlation_id.set(cid)
    response.headers["X-Correlation-ID"] = cid
    log.info("place order request", extra={"ctx": {"items": len(body.items)}})
    try:
        order, created = logic.create_order(
            body.customer_email, [i.model_dump() for i in body.items],
            body.card_token, idempotency_key, publish)
    except Exception:
        log.exception("could not place order")
        raise HTTPException(status_code=503, detail="Order could not be placed, please retry")
    if not created:
        response.status_code = 200  # replay of an earlier request
    return order


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    order = logic.get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


@app.get("/health")
def health():
    """Liveness: the process is up."""
    return {"status": "ok"}


@app.get("/ready")
def ready(response: Response):
    """Readiness: the broker consumer is bound and the service can take traffic."""
    if not messaging.ready.is_set():
        response.status_code = 503
        return {"status": "not ready"}
    return {"status": "ready"}
