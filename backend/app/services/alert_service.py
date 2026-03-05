"""
alert_service.py
================
Called after every POST /health-data.

Responsibilities:
  1. Load the applicable alert_thresholds for this animal's type
  2. Compare each metric against warn/critical bands
  3. Insert alert rows (avoiding duplicates of open same-type alerts)
  4. Push the health reading via WS to anyone watching the animal
  5. Push new alert payloads via WS to the animal's farmer

Import pattern:
    from app.services.alert_service import check_and_broadcast
"""

import logging
from datetime import datetime, timezone
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.alert import Alert
from app.models.alert_threshold import AlertThreshold
from app.models.animal import Animal
from app.models.device import Device
from app.models.health_reading import HealthReading
from app.schemas.health import HealthReadingCreate
from app.services.ws_manager import manager

logger = logging.getLogger(__name__)

# ---- Metric names that the threshold table covers -------------------------
METRICS = ["temperature", "heart_rate", "activity_level", "rumination_score"]

# Map DB metric name → alert_type string stored in alerts table
METRIC_TO_ALERT_TYPE = {
    "temperature":      "fever",
    "heart_rate":       "heart_rate",
    "activity_level":   "inactivity",
    "rumination_score": "low_rumination",
}


# ---------------------------------------------------------------------------
# Main entry-point called from the health data router
# ---------------------------------------------------------------------------

async def check_and_broadcast(
    db: AsyncSession,
    reading: HealthReading,
    animal: Animal,
) -> None:
    """
    Steps:
      A. Load thresholds for this animal_type
      B. Evaluate each metric → determine severity
      C. Create alert rows where thresholds are exceeded
      D. Push health reading via WS
      E. Push each new alert via WS
    """

    # A. Load thresholds
    result = await db.execute(
        select(AlertThreshold).where(AlertThreshold.animal_type == animal.animal_type)
    )
    thresholds: Sequence[AlertThreshold] = result.scalars().all()

    # Index by metric name for O(1) lookup
    thresh_map: dict[str, AlertThreshold] = {t.metric: t for t in thresholds}

    # B + C. Evaluate and create alerts
    new_alerts: list[dict] = []

    for metric in METRICS:
        value = getattr(reading, metric, None)
        if value is None:
            continue

        thresh = thresh_map.get(metric)
        if not thresh:
            continue

        severity = _evaluate_severity(value, thresh)
        if severity == "normal":
            continue

        alert_type = METRIC_TO_ALERT_TYPE[metric]

        # Avoid duplicate open alerts of the same type for this animal
        already_open = await _has_open_alert(db, animal.id, alert_type)
        if already_open:
            logger.debug(
                "Skipping duplicate open alert: animal=%d type=%s", animal.id, alert_type
            )
            continue

        message = _build_message(metric, value, severity, thresh)
        alert = Alert(
            animal_id=animal.id,
            alert_type=alert_type,
            message=message,
            severity=severity,
            is_resolved=False,
            created_at=datetime.now(timezone.utc),
        )
        db.add(alert)
        await db.flush()  # get the auto-generated alert.id

        new_alerts.append(_alert_ws_payload(alert, animal))
        logger.info("Alert created: %s", message)

    await db.commit()

    # D. Push raw health reading to animal's health room
    await manager.push_to_animal(
        animal_id=animal.id,
        payload=_reading_ws_payload(reading, animal),
    )

    # E. Push each new alert to the farmer's alert room
    for alert_payload in new_alerts:
        await manager.push_to_user(
            user_id=animal.farmer_id,
            payload=alert_payload,
        )


# ---------------------------------------------------------------------------
# Background task: detect offline devices (run every 5 min from main.py)
# ---------------------------------------------------------------------------

async def check_device_offline(db: AsyncSession) -> None:
    """
    Any device with last_seen older than 15 minutes gets a device_offline
    alert if one isn't already open.
    """
    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=15)

    result = await db.execute(
        select(Device).where(Device.last_seen < cutoff, Device.status != "unassigned")
    )
    stale_devices: Sequence[Device] = result.scalars().all()

    for device in stale_devices:
        if device.animal_id is None:
            continue

        already_open = await _has_open_alert(db, device.animal_id, "device_offline")
        if already_open:
            continue

        # Fetch the animal to get farmer_id
        animal_result = await db.execute(
            select(Animal).where(Animal.id == device.animal_id)
        )
        animal = animal_result.scalar_one_or_none()
        if not animal:
            continue

        alert = Alert(
            animal_id=animal.id,
            alert_type="device_offline",
            message=f"Device {device.device_serial} has not reported in >15 minutes.",
            severity="warning",
            is_resolved=False,
            created_at=datetime.now(timezone.utc),
        )
        db.add(alert)
        await db.flush()

        # Update device status
        device.status = "offline"

        await manager.push_to_user(
            user_id=animal.farmer_id,
            payload=_alert_ws_payload(alert, animal),
        )
        logger.warning("device_offline alert for device %s", device.device_serial)

    await db.commit()


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _evaluate_severity(value: float, thresh: AlertThreshold) -> str:
    """
    Returns 'critical', 'warning', or 'normal'.

    Thresholds contain four optional fields:
        warn_min, warn_max   → outside this band = warning
        critical_min, critical_max → outside this band = critical

    Rule:   check critical first (tighter band wins higher severity)
    """
    # Critical check
    if thresh.critical_min is not None and value < thresh.critical_min:
        return "critical"
    if thresh.critical_max is not None and value > thresh.critical_max:
        return "critical"

    # Warning check
    if thresh.warn_min is not None and value < thresh.warn_min:
        return "warning"
    if thresh.warn_max is not None and value > thresh.warn_max:
        return "warning"

    return "normal"


def _build_message(metric: str, value: float, severity: str, thresh: AlertThreshold) -> str:
    labels = {
        "temperature":      ("Temperature", "°C"),
        "heart_rate":       ("Heart Rate", "bpm"),
        "activity_level":   ("Activity Level", "%"),
        "rumination_score": ("Rumination Score", "%"),
    }
    label, unit = labels.get(metric, (metric, ""))
    return (
        f"{label} reading of {value}{unit} is {severity.upper()} — "
        f"threshold exceeded for this animal type."
    )


async def _has_open_alert(db: AsyncSession, animal_id: int, alert_type: str) -> bool:
    result = await db.execute(
        select(Alert).where(
            Alert.animal_id == animal_id,
            Alert.alert_type == alert_type,
            Alert.is_resolved == False,  # noqa: E712
        )
    )
    return result.scalar_one_or_none() is not None


def _reading_ws_payload(reading: HealthReading, animal: Animal) -> dict:
    """Payload sent to /ws/health/{animal_id} subscribers."""
    return {
        "type":             "health_reading",
        "animal_id":        animal.id,
        "temperature":      reading.temperature,
        "heart_rate":       reading.heart_rate,
        "activity_level":   reading.activity_level,
        "rumination_score": reading.rumination_score,
        "timestamp":        reading.timestamp.isoformat(),
    }


def _alert_ws_payload(alert: Alert, animal: Animal) -> dict:
    """Payload sent to /ws/alerts/{user_id} subscribers."""
    return {
        "type":        "new_alert",
        "alert_id":    alert.id,
        "animal_id":   animal.id,
        "animal_name": animal.name,
        "alert_type":  alert.alert_type,
        "severity":    alert.severity,
        "message":     alert.message,
        "created_at":  alert.created_at.isoformat(),
    }
