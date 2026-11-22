"""Order service: persistence and saga orchestration.

The Order service is the saga orchestrator. It owns the order record and the
saga state (PENDING -> STOCK_RESERVED -> CONFIRMED, or CANCELLED) and tells the
other services what to do by publishing events. Inventory and Payment never
learn anything about orders beyond what the event they receive contains.

This module is framework-free so it can be tested without HTTP or a broker.
`publish(routing_key, payload)` is injected by the caller.

Delivery is at-least-once, so every handler is idempotent in two ways:
  1. state transitions are guarded (an event is only applied from a legal state);
  2. when a duplicate arrives, the outgoing event is published again, so a crash
     between "commit" and "publish" never leaves the saga stuck.
"""
import json
import logging
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

log = logging.getLogger("order")

DB_PATH = os.getenv("DB_PATH", "/data/orders.db")

# Routing keys this service consumes.
BINDINGS = ["inventory.reserved", "inventory.rejected", "payment.succeeded", "payment.failed"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id               TEXT PRIMARY KEY,
    idempotency_key  TEXT UNIQUE,
    customer_email   TEXT NOT NULL,
    items_json       TEXT NOT NULL,
    total_cents      INTEGER NOT NULL,
    payment_token    TEXT NOT NULL,
    status           TEXT NOT NULL,
    reason           TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
"""


def _now():
    return datetime.now(timezone.utc).isoformat()


def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    conn.close()


@contextmanager
def tx():
    """One short transaction. BEGIN IMMEDIATE takes the write lock up front, so
    'check then insert' sequences cannot interleave between two requests."""
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise
    finally:
        conn.close()


def _public(row):
    """The API view of an order. The payment token is deliberately left out."""
    return {
        "order_id": row["id"],
        "status": row["status"],
        "customer_email": row["customer_email"],
        "items": json.loads(row["items_json"]),
        "total_cents": row["total_cents"],
        "reason": row["reason"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def get_order(order_id):
    with tx() as db:
        row = db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    return _public(row) if row else None


def create_order(customer_email, items, card_token, idempotency_key, publish):
    """Create an order and emit OrderPlaced. Returns (order, created).

    A repeated request with the same Idempotency-Key returns the original order
    and publishes nothing, so a client retry can never create a second order.
    """
    order_id = str(uuid.uuid4())
    total = sum(i["quantity"] * i["unit_price_cents"] for i in items)
    now = _now()

    with tx() as db:
        if idempotency_key:
            existing = db.execute(
                "SELECT * FROM orders WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if existing:
                log.info("duplicate request ignored", extra={"ctx": {"order_id": existing["id"]}})
                return _public(existing), False
        db.execute(
            "INSERT INTO orders (id, idempotency_key, customer_email, items_json, total_cents,"
            " payment_token, status, reason, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, 'PENDING', NULL, ?, ?)",
            (order_id, idempotency_key, customer_email, json.dumps(items), total,
             card_token, now, now),
        )
    log.info("order created", extra={"ctx": {"order_id": order_id, "total_cents": total}})

    try:
        publish("order.placed", {
            "order_id": order_id,
            "customer_email": customer_email,
            "items": items,
            "total_cents": total,
        })
    except Exception:
        # Dual-write gap (see README): the broker was unreachable, so fail the order
        # explicitly rather than leave it PENDING forever.
        with tx() as db:
            db.execute(
                "UPDATE orders SET status = 'CANCELLED', reason = 'broker unavailable',"
                " updated_at = ? WHERE id = ?", (_now(), order_id))
        raise
    return get_order(order_id), True


def _move(db, order_id, new_status, reason=None):
    db.execute(
        "UPDATE orders SET status = ?, reason = COALESCE(?, reason), updated_at = ? WHERE id = ?",
        (new_status, reason, _now(), order_id))


def handle_event(key, payload, publish):
    """Apply one saga event. Decide inside the transaction, publish after commit."""
    order_id = payload["order_id"]
    outgoing = []

    with tx() as db:
        row = db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if row is None:
            log.warning("event for unknown order ignored",
                        extra={"ctx": {"order_id": order_id, "event": key}})
            return
        status = row["status"]
        email = row["customer_email"]

        if key == "inventory.reserved" and status in ("PENDING", "STOCK_RESERVED"):
            new_status = "STOCK_RESERVED"
            _move(db, order_id, new_status)
            # Next saga step: ask Payment to charge the order.
            outgoing.append(("payment.requested", {
                "order_id": order_id,
                "amount_cents": row["total_cents"],
                "card_token": row["payment_token"],
            }))

        elif key == "inventory.rejected" and status in ("PENDING", "CANCELLED"):
            new_status = "CANCELLED"
            _move(db, order_id, new_status, payload.get("reason"))
            outgoing.append(("order.cancelled", {
                "order_id": order_id, "customer_email": email,
                "reason": payload.get("reason")}))

        elif key == "payment.succeeded" and status in ("STOCK_RESERVED", "CONFIRMED"):
            new_status = "CONFIRMED"
            _move(db, order_id, new_status)
            outgoing.append(("order.confirmed", {
                "order_id": order_id, "customer_email": email,
                "total_cents": row["total_cents"]}))

        elif key == "payment.failed" and status in ("STOCK_RESERVED", "CANCELLED"):
            new_status = "CANCELLED"
            _move(db, order_id, new_status, payload.get("reason"))
            # Compensating action: put the reserved stock back.
            outgoing.append(("inventory.release", {"order_id": order_id}))
            outgoing.append(("order.cancelled", {
                "order_id": order_id, "customer_email": email,
                "reason": payload.get("reason")}))

        else:
            log.warning("stale or out-of-order event ignored", extra={
                "ctx": {"order_id": order_id, "event": key, "status": status}})
            return

    log.info("saga step applied", extra={"ctx": {
        "order_id": order_id, "event": key, "from": status, "to": new_status}})
    for routing_key, body in outgoing:
        publish(routing_key, body)
