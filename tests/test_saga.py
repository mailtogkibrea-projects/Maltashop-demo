"""End-to-end saga tests that run without Docker, RabbitMQ or any third-party package.

The four services' framework-free `logic.py` modules are loaded side by side and
connected by a tiny in-memory broker that mimics topic routing. The same code runs
in the containers, so these tests prove the saga, the compensation and the
idempotency rules; the Compose smoke test proves the wiring.

Run:  python -m unittest discover -s tests -v
"""
import importlib.util
import io
import json
import logging
import tempfile
import unittest
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


order = load("order_logic", "order-service/app/logic.py")
inventory = load("inventory_logic", "inventory-service/app/logic.py")
payment = load("payment_logic", "payment-service/app/logic.py")
notification = load("notification_logic", "notification-service/app/logic.py")
SERVICES = [order, inventory, payment, notification]


class InMemoryBroker:
    """Routes each event to every service bound to its routing key."""

    def __init__(self, deliver_twice=False):
        self.queue = deque()
        self.deliver_twice = deliver_twice  # simulates at-least-once redelivery
        self.log = []

    def publish(self, key, payload):
        self.log.append(key)
        self.queue.append((key, json.loads(json.dumps(payload))))

    def run(self):
        while self.queue:
            key, payload = self.queue.popleft()
            for service in SERVICES:
                if key in service.BINDINGS:
                    for _ in range(2 if self.deliver_twice else 1):
                        service.handle_event(key, payload, self.publish)


class SagaTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        for service in SERVICES:
            service.DB_PATH = str(Path(self.tmp.name) / f"{service.__name__}.db")
            service.init_db()
        self.broker = InMemoryBroker()

    def tearDown(self):
        self.tmp.cleanup()

    def place(self, items, token="tok_visa", key=None):
        created, _ = order.create_order("maria@example.mt", items, token, key, self.broker.publish)
        self.broker.run()
        return order.get_order(created["order_id"])

    @staticmethod
    def kettle(qty=1):
        return [{"sku": "SKU-KETTLE", "quantity": qty, "unit_price_cents": 2999}]

    def kinds(self):
        return sorted(n["kind"] for n in notification.list_notifications())


class HappyPath(SagaTestCase):
    def test_order_is_confirmed_and_stock_decremented(self):
        result = self.place(self.kettle(2))
        self.assertEqual(result["status"], "CONFIRMED")
        self.assertEqual(result["total_cents"], 5998)
        self.assertEqual(inventory.get_stock()["SKU-KETTLE"], 23)
        self.assertEqual([p["status"] for p in payment.list_payments()], ["SUCCEEDED"])
        self.assertEqual(self.kinds(), ["confirmed", "received"])

    def test_events_flow_in_saga_order(self):
        self.place(self.kettle())
        self.assertEqual(self.broker.log, [
            "order.placed", "inventory.reserved", "payment.requested",
            "payment.succeeded", "order.confirmed"])


class CompensationPaths(SagaTestCase):
    def test_out_of_stock_cancels_without_charging(self):
        result = self.place([{"sku": "SKU-LAMP", "quantity": 6, "unit_price_cents": 1999}])
        self.assertEqual(result["status"], "CANCELLED")
        self.assertIn("out of stock", result["reason"])
        self.assertEqual(inventory.get_stock()["SKU-LAMP"], 5)  # untouched
        self.assertEqual(payment.list_payments(), [])  # never charged
        self.assertEqual(self.kinds(), ["cancelled", "received"])

    def test_declined_card_releases_the_reserved_stock(self):
        result = self.place(self.kettle(3), token="tok_declined")
        self.assertEqual(result["status"], "CANCELLED")
        self.assertEqual(result["reason"], "card_declined")
        self.assertEqual(inventory.get_stock()["SKU-KETTLE"], 25)  # compensated
        self.assertEqual([p["status"] for p in payment.list_payments()], ["FAILED"])
        self.assertIn("inventory.release", self.broker.log)
        self.assertEqual(self.kinds(), ["cancelled", "received"])


class Idempotency(SagaTestCase):
    def test_duplicate_delivery_changes_nothing(self):
        self.broker = InMemoryBroker(deliver_twice=True)
        result = self.place(self.kettle(2))
        self.assertEqual(result["status"], "CONFIRMED")
        self.assertEqual(inventory.get_stock()["SKU-KETTLE"], 23)  # decremented once
        self.assertEqual(len(payment.list_payments()), 1)  # charged once
        self.assertEqual(self.kinds(), ["confirmed", "received"])  # one email of each kind

    def test_duplicate_delivery_on_failure_path_releases_stock_once(self):
        self.broker = InMemoryBroker(deliver_twice=True)
        result = self.place(self.kettle(3), token="tok_declined")
        self.assertEqual(result["status"], "CANCELLED")
        self.assertEqual(inventory.get_stock()["SKU-KETTLE"], 25)  # not 28

    def test_same_idempotency_key_returns_the_same_order(self):
        first, created_first = order.create_order(
            "maria@example.mt", self.kettle(), "tok_visa", "key-123", self.broker.publish)
        second, created_second = order.create_order(
            "maria@example.mt", self.kettle(), "tok_visa", "key-123", self.broker.publish)
        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first["order_id"], second["order_id"])
        self.assertEqual(self.broker.log, ["order.placed"])  # published once only

    def test_event_for_unknown_order_is_ignored(self):
        order.handle_event("payment.succeeded", {"order_id": "does-not-exist"}, self.broker.publish)
        self.assertEqual(self.broker.log, [])


class Resilience(SagaTestCase):
    def test_broker_outage_fails_the_order_instead_of_leaving_it_pending(self):
        def broken_publish(key, payload):
            raise ConnectionError("broker down")

        with self.assertRaises(ConnectionError):
            order.create_order("maria@example.mt", self.kettle(), "tok_visa", None, broken_publish)
        with order.tx() as db:
            row = db.execute("SELECT status, reason FROM orders").fetchone()
        self.assertEqual((row["status"], row["reason"]), ("CANCELLED", "broker unavailable"))


class LogSchema(unittest.TestCase):
    def test_every_line_carries_the_mandatory_fields_and_correlation_id(self):
        logging_setup = load("logging_setup", "order-service/app/logging_setup.py")
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging_setup.JsonFormatter("order-service"))
        logger = logging.getLogger("schema-test")
        logger.handlers = [handler]
        logger.propagate = False
        logger.setLevel(logging.INFO)

        token = logging_setup.correlation_id.set("demo-123")
        try:
            logger.info("order created", extra={"ctx": {"order_id": "o-1"}})
        finally:
            logging_setup.correlation_id.reset(token)

        entry = json.loads(stream.getvalue())
        for field in ("timestamp", "level", "service", "correlation_id", "message"):
            self.assertIn(field, entry)
        self.assertEqual(entry["correlation_id"], "demo-123")
        self.assertEqual(entry["order_id"], "o-1")


if __name__ == "__main__":
    unittest.main()
