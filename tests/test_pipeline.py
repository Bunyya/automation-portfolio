import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from automation_portfolio.pipeline import InputMessage, Kind, NotificationService, SimulatedSink


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name) / "state.sqlite3"

    def test_classify_plan_and_prepare(self):
        with NotificationService(self.state) as service:
            sink = SimulatedSink()
            result = service.process(InputMessage("m1", "Срочно: Тестовая встреча", "staff"), sink)
        self.assertEqual(result.kind, Kind.URGENT)
        self.assertEqual(result.emitted, ["demo-staff", "demo-operators"])
        self.assertEqual([n.text for n in sink.sent], ["[Срочное сообщение] Тестовая встреча"] * 2)

    def test_replay_after_restart_does_not_emit_again(self):
        message = InputMessage("m1", "Напоминание: Тест", "students")
        with NotificationService(self.state) as service:
            service.process(message, SimulatedSink())
        with NotificationService(self.state) as service:
            sink = SimulatedSink()
            replay = service.process(message, sink)
        self.assertEqual(replay.skipped, ["demo-students"])
        self.assertEqual(sink.sent, [])

    def test_failed_recipient_can_retry_without_repeating_success(self):
        message = InputMessage("m1", "Срочно: Тест", "students")
        with NotificationService(self.state) as service:
            first = service.process(message, SimulatedSink("demo-operators"))
            second_sink = SimulatedSink()
            second = service.process(message, second_sink)
            attempts = service.connection.execute(
                "SELECT recipient, status, attempts FROM delivery_attempts ORDER BY recipient"
            ).fetchall()
        self.assertEqual(first.failed, ["demo-operators"])
        self.assertEqual(second.skipped, ["demo-students"])
        self.assertEqual(second.emitted, ["demo-operators"])
        self.assertEqual([n.recipient for n in second_sink.sent], ["demo-operators"])
        self.assertEqual(attempts, [("demo-operators", "emitted", 2), ("demo-students", "emitted", 1)])

    def test_same_id_with_changed_content_is_rejected(self):
        with NotificationService(self.state) as service:
            service.process(InputMessage("m1", "Объявление: Первое", "staff"), SimulatedSink())
            with self.assertRaisesRegex(ValueError, "different content"):
                service.process(InputMessage("m1", "Объявление: Второе", "staff"), SimulatedSink())

    def test_unrecognized_message_is_recorded_without_delivery(self):
        with NotificationService(self.state) as service:
            sink = SimulatedSink()
            result = service.process(InputMessage("m1", "Обычный текст", "staff"), sink)
            count = service.connection.execute("SELECT COUNT(*) FROM message_registry").fetchone()[0]
        self.assertEqual(result.kind, Kind.UNRECOGNIZED)
        self.assertEqual(sink.sent, [])
        self.assertEqual(count, 1)

    def test_state_does_not_store_message_body(self):
        marker = "SYNTHETIC_UNIQUE_MESSAGE_BODY"
        with NotificationService(self.state) as service:
            service.process(InputMessage("m1", f"Объявление: {marker}", "staff"), SimulatedSink())
            dump = "\n".join(service.connection.iterdump())
        self.assertNotIn(marker, dump)

    def test_input_validation(self):
        for args in [("bad id", "Объявление: Тест", "staff"), ("m1", " ", "staff"), ("m1", "Тест", "external"), (None, "Тест", "staff")]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                InputMessage(*args)

    def test_unexpected_adapter_error_rolls_back(self):
        class BuggySink:
            def emit(self, notification):
                raise RuntimeError("programming error")

        with NotificationService(self.state) as service:
            with self.assertRaisesRegex(RuntimeError, "programming error"):
                service.process(InputMessage("m1", "Объявление: Тест", "staff"), BuggySink())
            count = service.connection.execute("SELECT COUNT(*) FROM message_registry").fetchone()[0]
        self.assertEqual(count, 0)

    def test_corrupt_state_returns_clean_cli_error(self):
        self.state.write_text("not a SQLite database", encoding="utf-8")
        command = [sys.executable, "-m", "automation_portfolio", "--input", "examples/data/messages.json", "--state", str(self.state)]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("DatabaseError", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_cli_runs_offline_and_second_run_skips(self):
        path = Path(self.temp.name) / "messages.json"
        path.write_text(json.dumps([{"message_id": "m1", "text": "Объявление: Тест", "audience": "staff"}]), encoding="utf-8")
        command = [sys.executable, "-m", "automation_portfolio", "--input", str(path), "--state", str(self.state)]
        first = subprocess.run(command, capture_output=True, text=True, check=False)
        second = subprocess.run(command, capture_output=True, text=True, check=False)
        self.assertEqual((first.returncode, second.returncode), (0, 0))
        self.assertEqual(json.loads(first.stdout)["results"][0]["emitted"], ["demo-staff"])
        self.assertEqual(json.loads(second.stdout)["results"][0]["skipped"], ["demo-staff"])


if __name__ == "__main__":
    unittest.main()
