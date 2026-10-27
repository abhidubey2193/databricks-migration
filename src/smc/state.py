"""Migration state machine (approval gates + audit trail). Status = latest event per workload."""
from __future__ import annotations

from typing import Dict, Set

STATES = ["DISCOVERED", "APPROVED", "CLONED", "MIGRATE_FAILED", "VALIDATED", "VALIDATION_FAILED",
          "PROMOTE_APPROVED", "PROMOTED", "ROLLBACK_REQUESTED", "ROLLED_BACK", "REJECTED"]

TRANSITIONS: Dict[str, Set[str]] = {
    "DISCOVERED": {"APPROVED", "REJECTED"},
    "APPROVED": {"CLONED", "MIGRATE_FAILED", "REJECTED"},
    "MIGRATE_FAILED": {"APPROVED", "REJECTED"},
    "CLONED": {"VALIDATED", "VALIDATION_FAILED", "APPROVED"},
    "VALIDATION_FAILED": {"APPROVED", "REJECTED", "VALIDATED"},
    "VALIDATED": {"PROMOTE_APPROVED", "REJECTED", "APPROVED"},
    "PROMOTE_APPROVED": {"PROMOTED", "VALIDATION_FAILED"},
    "PROMOTED": {"ROLLBACK_REQUESTED"},
    "ROLLBACK_REQUESTED": {"ROLLED_BACK"},
    "ROLLED_BACK": {"APPROVED", "REJECTED"},
    "REJECTED": {"APPROVED"},
}
HUMAN_EVENTS = {"APPROVED", "REJECTED", "PROMOTE_APPROVED", "ROLLBACK_REQUESTED"}


def can_transition(current: str, nxt: str) -> bool:
    return nxt in TRANSITIONS.get(current or "DISCOVERED", set())


def require(current: str, nxt: str) -> None:
    if not can_transition(current, nxt):
        raise ValueError(f"illegal transition {current} -> {nxt}")
