"""Deterministic, offline notification pipeline using synthetic recipients."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Protocol


class Kind(str, Enum):
    URGENT = "urgent"
    REMINDER = "reminder"
    ANNOUNCEMENT = "announcement"
    UNRECOGNIZED = "unrecognized"


class DeliveryError(Exception):
    """Expected failure reported by a delivery adapter."""


AUDIENCES = {"students": "demo-students", "staff": "demo-staff"}
PREFIXES = (
    ("срочно:", Kind.URGENT),
    ("напоминание:", Kind.REMINDER),
    ("объявление:", Kind.ANNOUNCEMENT),
)


@dataclass(frozen=True)
class InputMessage:
    message_id: str
    text: str
    audience: str

    def __post_init__(self) -> None:
        if not isinstance(self.message_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.message_id):
            raise ValueError("message_id must contain 1–64 ASCII letters, digits, _ or -")
        if not isinstance(self.text, str) or not 1 <= len(self.text.strip()) <= 500:
            raise ValueError("text must contain 1–500 characters")
        if not isinstance(self.audience, str) or self.audience not in AUDIENCES:
            raise ValueError("audience must be students or staff")


@dataclass(frozen=True)
class Classification:
    kind: Kind
    body: str


@dataclass(frozen=True)
class Notification:
    message_id: str
    recipient: str
    text: str


@dataclass
class ProcessResult:
    message_id: str
    kind: Kind
    emitted: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)


class Sink(Protocol):
    def emit(self, notification: Notification) -> None: ...


class SimulatedSink:
    """Records output in memory; never connects to an external service."""

    def __init__(self, fail_recipient: str | None = None) -> None:
        self.fail_recipient = fail_recipient
        self.sent: list[Notification] = []

    def emit(self, notification: Notification) -> None:
        if notification.recipient == self.fail_recipient:
            raise DeliveryError("simulated delivery failure")
        self.sent.append(notification)


def classify(text: str) -> Classification:
    clean = text.strip()
    lowered = clean.casefold()
    for prefix, kind in PREFIXES:
        if lowered.startswith(prefix):
            body = clean[len(prefix) :].strip()
            return Classification(kind, body) if body else Classification(Kind.UNRECOGNIZED, "")
    return Classification(Kind.UNRECOGNIZED, "")


def plan(message: InputMessage, classification: Classification) -> list[Notification]:
    if classification.kind == Kind.UNRECOGNIZED:
        return []
    recipients = [AUDIENCES[message.audience]]
    if classification.kind == Kind.URGENT:
        recipients.append("demo-operators")
    labels = {
        Kind.URGENT: "Срочное сообщение",
        Kind.REMINDER: "Напоминание",
        Kind.ANNOUNCEMENT: "Объявление",
    }
    prepared = f"[{labels[classification.kind]}] {classification.body}"
    return [Notification(message.message_id, recipient, prepared) for recipient in recipients]


def _fingerprint(message: InputMessage) -> str:
    payload = json.dumps(
        {"text": message.text, "audience": message.audience},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class NotificationService:
    def __init__(self, state_path: str | Path) -> None:
        self.state_path = Path(state_path)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.state_path, timeout=10)
        try:
            self.connection.execute("PRAGMA foreign_keys = ON")
            with self.connection:
                self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS message_registry (
                    message_id TEXT PRIMARY KEY,
                    fingerprint TEXT NOT NULL,
                    kind TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS delivery_attempts (
                    message_id TEXT NOT NULL REFERENCES message_registry(message_id),
                    recipient TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('emitted', 'failed')),
                    attempts INTEGER NOT NULL CHECK (attempts >= 1),
                    error_code TEXT,
                    PRIMARY KEY (message_id, recipient)
                );
                """
                )
        except sqlite3.Error:
            self.connection.close()
            raise

    def __enter__(self) -> NotificationService:
        return self

    def __exit__(self, *_: object) -> None:
        self.connection.close()

    def process(self, message: InputMessage, sink: Sink) -> ProcessResult:
        classification = classify(message.text)
        actions = plan(message, classification)
        result = ProcessResult(message.message_id, classification.kind)
        fingerprint = _fingerprint(message)

        # The sink in this demonstration has no external side effects. For a real
        # API, a database transaction alone cannot guarantee exactly-once delivery.
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            previous = self.connection.execute(
                "SELECT fingerprint FROM message_registry WHERE message_id = ?",
                (message.message_id,),
            ).fetchone()
            if previous and previous[0] != fingerprint:
                raise ValueError("message_id was already used with different content")
            if not previous:
                self.connection.execute(
                    "INSERT INTO message_registry VALUES (?, ?, ?)",
                    (message.message_id, fingerprint, classification.kind.value),
                )

            for action in actions:
                row = self.connection.execute(
                    "SELECT status, attempts FROM delivery_attempts "
                    "WHERE message_id = ? AND recipient = ?",
                    (message.message_id, action.recipient),
                ).fetchone()
                if row and row[0] == "emitted":
                    result.skipped.append(action.recipient)
                    continue
                attempts = (row[1] if row else 0) + 1
                try:
                    sink.emit(action)
                except DeliveryError:
                    status, error_code = "failed", "delivery_error"
                    result.failed.append(action.recipient)
                else:
                    status, error_code = "emitted", None
                    result.emitted.append(action.recipient)
                self.connection.execute(
                    """INSERT INTO delivery_attempts
                       (message_id, recipient, status, attempts, error_code)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(message_id, recipient) DO UPDATE SET
                       status = excluded.status,
                       attempts = excluded.attempts,
                       error_code = excluded.error_code""",
                    (message.message_id, action.recipient, status, attempts, error_code),
                )
        return result
