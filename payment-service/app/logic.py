"""Payment service: simulated card payments through an external provider.

Only a card *token* ever reaches this system, never a card number, which keeps
the service out of the heaviest PCI DSS scope and limits GDPR exposure.
The mock provider declines the token "tok_declined" and accepts everything else.

Idempotency: the payments table is keyed by order_id, so a redelivered
payment.requested never charges twice; it replays the stored outcome instead.
In production the order_id would also be sent to the real provider as its
idempotency key, and the provider call would happen outside the DB transaction.
"""
import logging
import os
import sqlite3
import uuid
from contextlib import contextmanager

log = logging.getLogger("payment")

DB_PATH = os.getenv("DB_PATH", "/data/payments.db")

BINDINGS = ["payment.requested"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS payments (
    order_id      TEXT PRIMARY KEY,
    amount_cents  INTEGER NOT NULL,
    status        TEXT NOT NULL,   -- SUCCEEDED | FAILED
    provider_ref  TEXT,
    reason        TEXT
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


def list_payments():
    with tx() as db:
        rows = db.execute("SELECT * FROM payments ORDER BY order_id").fetchall()
    return [dict(r) for r in rows]


def _charge(card_token, amount_cents):
    """Stand-in for the external provider. Returns (ok, provider_ref, reason)."""
    if card_token == "tok_declined":
        return False, None, "card_declined"
    return True, "pay_" + uuid.uuid4().hex[:12], None


def handle_event(key, payload, publish):
    order_id = payload["order_id"]

    with tx() as db:
        row = db.execute("SELECT * FROM payments WHERE order_id = ?", (order_id,)).fetchone()
        if row is None:
            ok, ref, reason = _charge(payload["card_token"], payload["amount_cents"])
            db.execute(
                "INSERT INTO payments (order_id, amount_cents, status, provider_ref, reason)"
                " VALUES (?, ?, ?, ?, ?)",
                (order_id, payload["amount_cents"], "SUCCEEDED" if ok else "FAILED", ref, reason))
            row = db.execute("SELECT * FROM payments WHERE order_id = ?", (order_id,)).fetchone()
            log.info("payment processed", extra={"ctx": {
                "order_id": order_id, "status": row["status"], "amount_cents": row["amount_cents"]}})
        else:
            log.info("duplicate payment.requested: replaying outcome",
                     extra={"ctx": {"order_id": order_id, "status": row["status"]}})

    if row["status"] == "SUCCEEDED":
        publish("payment.succeeded", {"order_id": order_id, "provider_ref": row["provider_ref"]})
    else:
        publish("payment.failed", {"order_id": order_id, "reason": row["reason"]})
