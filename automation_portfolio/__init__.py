"""Offline notification pipeline with synthetic recipients."""

from .pipeline import InputMessage, NotificationService, SimulatedSink

__all__ = ["InputMessage", "NotificationService", "SimulatedSink"]
