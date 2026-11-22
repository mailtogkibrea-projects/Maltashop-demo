"""Inventory service: stock levels and reservations.

Owns its own data (stock + reservations). A reservation row keyed by order_id is
what makes the handler idempotent: a redelivered OrderPlaced finds the existing
row, leaves stock untouched and simply re-announces the original outcome.
"""
import json
import logging
import os
import sqlite3
from contextlib import contextmanager

log = logging.getLogger("inventory")

DB_PATH = os.getenv("DB_PATH", "/data/inventory.db")

BINDINGS = ["order.placed", "inventory.release"]

# Demo catalogue, loaded on first start only.
SEED_STOCK = {"SKU-TV-55": 10, "SKU-KETTLE": 25, "SKU-LAMP": 5}

SCHEMA = """
CREATE TABLE IF NOT EXISTS stock (
    sku        TEXT PRIMARY KEY,
    available  INTEGER NOT NULL CHECK (available >= 0)
);
CREATE TABLE IF NOT EXISTS reservations (
    order_id    TEXT PRIMARY KEY,
    items_json  TEXT NOT NULL,
    status      TEXT NOT NULL,   -- RESERVED | REJECTED | RELEASED
    reason      TEXT
);
"""


def init_db():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    conn.executemany("INSERT OR IGNORE INTO stock (sku, available) VALUES (?, ?)",
                     SEED_STOCK.items())
    conn.commit()
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


def get_stock():
    with tx() as db:
        rows = db.execute("SELECT sku, available FROM stock ORDER BY sku").fetchall()
    return {r["sku"]: r["available"] for r in rows}


def _reserve(db, order_id, items):
    """All-or-nothing reservation inside the caller's transaction."""
    wanted = {}
    for item in items:
        wanted[item["sku"]] = wanted.get(item["sku"], 0) + item["quantity"]

    short = []
    for sku, qty in wanted.items():
        row = db.execute("SELECT available FROM stock WHERE sku = ?", (sku,)).fetchone()
        if row is None or row["available"] < qty:
            short.append(sku)

    if short:
        status, reason = "REJECTED", "out of stock: " + ", ".join(sorted(short))
    else:
        for sku, qty in wanted.items():
            db.execute("UPDATE stock SET available = available - ? WHERE sku = ?", (qty, sku))
        status, reason = "RESERVED", None

    db.execute("INSERT INTO reservations (order_id, items_json, status, reason) VALUES (?, ?, ?, ?)",
               (order_id, json.dumps(wanted), status, reason))
    return status, reason


def handle_event(key, payload, publish):
    order_id = payload["order_id"]

    if key == "order.placed":
        with tx() as db:
            row = db.execute("SELECT status, reason FROM reservations WHERE order_id = ?",
                             (order_id,)).fetchone()
            if row is None:
                status, reason = _reserve(db, order_id, payload["items"])
                log.info("reservation decided", extra={"ctx": {
                    "order_id": order_id, "status": status, "reason": reason}})
            else:
                status, reason = row["status"], row["reason"]
                log.info("duplicate order.placed: replaying outcome",
                         extra={"ctx": {"order_id": order_id, "status": status}})

        if status == "RESERVED":
            publish("inventory.reserved", {"order_id": order_id})
        elif status == "REJECTED":
            publish("inventory.rejected", {"order_id": order_id, "reason": reason})
        # RELEASED: the saga already compensated this order, nothing to announce.

    elif key == "inventory.release":
        # Compensating action for a failed payment.
        with tx() as db:
            row = db.execute("SELECT items_json, status FROM reservations WHERE order_id = ?",
                             (order_id,)).fetchone()
            if row is None or row["status"] != "RESERVED":
                log.info("release ignored (nothing reserved)", extra={"ctx": {"order_id": order_id}})
                return
            for sku, qty in json.loads(row["items_json"]).items():
                db.execute("UPDATE stock SET available = available + ? WHERE sku = ?", (qty, sku))
            db.execute("UPDATE reservations SET status = 'RELEASED' WHERE order_id = ?", (order_id,))
        log.info("stock released (compensation)", extra={"ctx": {"order_id": order_id}})
