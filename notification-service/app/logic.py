"""Notification service: customer emails (simulated by writing to its own table).

It only listens; nothing in the checkout path waits for it. If this service is
slow or crashes, events simply queue up in the broker, which is exactly the
isolation the March memory-leak incident lacked in the monolith.

Idempotency: (order_id, kind) is the primary key, so a redelivered event cannot
send the same email twice.
"""
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

log = logging.getLogger("notification")

DB_PATH = os.getenv("DB_PATH", "/data/notifications.db")

BINDINGS = ["order.placed", "order.confirmed", "order.cancelled"]

KINDS = {"order.placed": "received", "order.confirmed": "confirmed", "order.cancelled": "cancelled"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS notifications (
    order_id    TEXT NOT NULL,
    kind        TEXT NOT NULL,   -- received | confirmed | cancelled
    recipient   TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (order_id, kind)
);
"""


def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    conn.close()


@contextmanager
def tx():
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


def list_notifications():
    with tx() as db:
        rows = db.execute("SELECT * FROM notifications ORDER BY created_at, kind").fetchall()
    return [dict(r) for r in rows]


def handle_event(key, payload, publish=None):
    order_id = payload["order_id"]
    kind = KINDS[key]
    with tx() as db:
        cursor = db.execute(
            "INSERT OR IGNORE INTO notifications (order_id, kind, recipient, created_at)"
            " VALUES (?, ?, ?, ?)",
            (order_id, kind, payload["customer_email"], datetime.now(timezone.utc).isoformat()))
        created = cursor.rowcount == 1

    if created:
        log.info("email sent (simulated)", extra={"ctx": {
            "order_id": order_id, "kind": kind, "recipient": payload["customer_email"]}})
    else:
        log.info("duplicate notification ignored", extra={"ctx": {"order_id": order_id, "kind": kind}})
