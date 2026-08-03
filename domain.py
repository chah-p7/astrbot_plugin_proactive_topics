from __future__ import annotations

import hashlib
import json
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, tzinfo
from typing import Any, Callable, Mapping


STATE_VERSION = 3

PRESETS: dict[str, dict[str, int]] = {
    "low": {
        "min_interval_minutes": 240,
        "max_interval_minutes": 480,
        "daily_limit": 2,
        "cooldown_minutes": 180,
        "silence_minutes": 45,
    },
    "medium": {
        "min_interval_minutes": 120,
        "max_interval_minutes": 300,
        "daily_limit": 3,
        "cooldown_minutes": 90,
        "silence_minutes": 30,
    },
    "high": {
        "min_interval_minutes": 60,
        "max_interval_minutes": 120,
        "daily_limit": 5,
        "cooldown_minutes": 45,
        "silence_minutes": 15,
    },
}

PRESET_ALIASES = {
    "低": "low",
    "低频": "low",
    "low": "low",
    "中": "medium",
    "中频": "medium",
    "medium": "medium",
    "高": "high",
    "高频": "high",
    "high": "high",
}

PRESET_LABELS = {
    "low": "低频",
    "medium": "中频",
    "high": "高频",
    "custom": "自定义",
}


def clamp_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, parsed))


def normalize_hhmm(value: Any) -> str | None:
    match = re.fullmatch(r"\s*(\d{1,2}):(\d{1,2})\s*", str(value or ""))
    if not match:
        return None
    hour, minute = int(match.group(1)), int(match.group(2))
    if not 0 <= hour <= 23 or not 0 <= minute <= 59:
        return None
    return f"{hour:02d}:{minute:02d}"


def time_to_minutes(value: Any) -> int | None:
    normalized = normalize_hhmm(value)
    if normalized is None:
        return None
    hour, minute = normalized.split(":", 1)
    return int(hour) * 60 + int(minute)


def parse_fixed_times(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        candidates = [str(item) for item in value]
    else:
        candidates = re.split(r"[,，;；\s]+", str(value or ""))
    result: list[str] = []
    for candidate in candidates:
        normalized = normalize_hhmm(candidate)
        if normalized and normalized not in result:
            result.append(normalized)
    return sorted(result)


def in_active_window(now_minutes: int, start: str, end: str) -> bool:
    start_minutes = time_to_minutes(start)
    end_minutes = time_to_minutes(end)
    if start_minutes is None or end_minutes is None or start_minutes == end_minutes:
        return True
    if start_minutes < end_minutes:
        return start_minutes <= now_minutes < end_minutes
    return now_minutes >= start_minutes or now_minutes < end_minutes


def _next_active_start(now: datetime, start: str, end: str) -> float:
    start_minutes = time_to_minutes(start)
    end_minutes = time_to_minutes(end)
    if start_minutes is None or end_minutes is None or start_minutes == end_minutes:
        return now.timestamp()
    now_minutes = now.hour * 60 + now.minute
    today_start = now.replace(
        hour=start_minutes // 60,
        minute=start_minutes % 60,
        second=0,
        microsecond=0,
    )
    if start_minutes < end_minutes:
        target = today_start if now_minutes < start_minutes else today_start + timedelta(days=1)
    else:
        target = today_start
    return target.timestamp()


def _tomorrow_active_start(now: datetime, start: str) -> float:
    start_minutes = time_to_minutes(start)
    if start_minutes is None:
        start_minutes = 0
    target = (now + timedelta(days=1)).replace(
        hour=start_minutes // 60,
        minute=start_minutes % 60,
        second=0,
        microsecond=0,
    )
    return target.timestamp()


@dataclass(slots=True)
class BotIdentity:
    platform_id: str = ""
    platform_name: str = ""
    self_id: str = ""
    group_id: str = ""

    def __post_init__(self) -> None:
        self.platform_id = str(self.platform_id or "").strip()
        self.platform_name = str(self.platform_name or "").strip()
        self.self_id = str(self.self_id or "").strip()
        self.group_id = str(self.group_id or "").strip()

    @property
    def routable(self) -> bool:
        return bool(self.group_id and (self.platform_id or self.self_id))

    def matches_actor(self, other: BotIdentity) -> bool:
        if self.group_id and other.group_id and self.group_id != other.group_id:
            return False
        pairs = [
            (self.self_id, other.self_id),
            (self.platform_id, other.platform_id),
        ]
        known = [(left, right) for left, right in pairs if left and right]
        return any(left == right for left, right in known)

    def conflicts_with(self, other: BotIdentity) -> bool:
        if self.group_id and other.group_id and self.group_id != other.group_id:
            return True
        pairs = [
            (self.self_id, other.self_id),
            (self.platform_id, other.platform_id),
        ]
        known = [(left, right) for left, right in pairs if left and right]
        return bool(known) and not any(left == right for left, right in known)

    def as_dict(self) -> dict[str, str]:
        return {
            "platform_id": self.platform_id,
            "platform_name": self.platform_name,
            "self_id": self.self_id,
            "group_id": self.group_id,
        }

    @classmethod
    def from_mapping(cls, value: Any) -> BotIdentity:
        raw = value if isinstance(value, Mapping) else {}
        return cls(
            platform_id=raw.get("platform_id", ""),
            platform_name=raw.get("platform_name", ""),
            self_id=raw.get("self_id", raw.get("account_id", "")),
            group_id=raw.get("group_id", raw.get("raw_group_id", "")),
        )


@dataclass(slots=True)
class TopicSettings:
    enabled: bool = False
    frequency: str = "medium"
    min_interval_minutes: int = 120
    max_interval_minutes: int = 300
    daily_limit: int = 3
    cooldown_minutes: int = 90
    silence_minutes: int = 30
    probability_percent: int = 70
    active_start: str = "09:00"
    active_end: str = "22:30"
    fixed_times: list[str] = field(default_factory=lambda: ["09:00", "21:30"])

    def normalize(self) -> None:
        if self.frequency not in {*PRESETS, "custom"}:
            self.frequency = "custom"
        self.min_interval_minutes = clamp_int(
            self.min_interval_minutes, 120, 5, 10080
        )
        self.max_interval_minutes = clamp_int(
            self.max_interval_minutes, 300, 5, 10080
        )
        if self.min_interval_minutes > self.max_interval_minutes:
            self.min_interval_minutes, self.max_interval_minutes = (
                self.max_interval_minutes,
                self.min_interval_minutes,
            )
        self.daily_limit = clamp_int(self.daily_limit, 3, 1, 50)
        self.cooldown_minutes = clamp_int(self.cooldown_minutes, 90, 0, 10080)
        self.silence_minutes = clamp_int(self.silence_minutes, 30, 0, 1440)
        self.probability_percent = clamp_int(
            self.probability_percent, 70, 0, 100
        )
        self.active_start = normalize_hhmm(self.active_start) or "09:00"
        self.active_end = normalize_hhmm(self.active_end) or "22:30"
        self.fixed_times = parse_fixed_times(self.fixed_times)

    def apply_preset(self, name: str) -> None:
        preset = PRESETS[name]
        self.frequency = name
        for key, value in preset.items():
            setattr(self, key, value)
        self.normalize()

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.enabled,
            "frequency": self.frequency,
            "min_interval_minutes": self.min_interval_minutes,
            "max_interval_minutes": self.max_interval_minutes,
            "daily_limit": self.daily_limit,
            "cooldown_minutes": self.cooldown_minutes,
            "silence_minutes": self.silence_minutes,
            "probability_percent": self.probability_percent,
            "active_start": self.active_start,
            "active_end": self.active_end,
            "fixed_times": list(self.fixed_times),
        }

    @classmethod
    def defaults(cls, config: Mapping[str, Any]) -> TopicSettings:
        requested = str(config.get("default_frequency", "medium") or "").lower()
        preset_name = PRESET_ALIASES.get(requested, "medium")
        preset = PRESETS[preset_name]
        settings = cls(
            enabled=False,
            frequency=preset_name,
            min_interval_minutes=preset["min_interval_minutes"],
            max_interval_minutes=preset["max_interval_minutes"],
            daily_limit=preset["daily_limit"],
            cooldown_minutes=preset["cooldown_minutes"],
            silence_minutes=preset["silence_minutes"],
            probability_percent=clamp_int(
                config.get("default_probability_percent"), 70, 0, 100
            ),
            active_start=normalize_hhmm(config.get("active_start")) or "09:00",
            active_end=normalize_hhmm(config.get("active_end")) or "22:30",
            fixed_times=parse_fixed_times(
                config.get("default_fixed_times", "09:00,21:30")
            ),
        )
        settings.normalize()
        return settings

    @classmethod
    def from_mapping(
        cls,
        value: Any,
        defaults: TopicSettings,
    ) -> TopicSettings:
        raw = value if isinstance(value, Mapping) else {}
        settings = cls(
            enabled=bool(raw.get("enabled", defaults.enabled)),
            frequency=str(raw.get("frequency", defaults.frequency) or "custom"),
            min_interval_minutes=raw.get(
                "min_interval_minutes", defaults.min_interval_minutes
            ),
            max_interval_minutes=raw.get(
                "max_interval_minutes", defaults.max_interval_minutes
            ),
            daily_limit=raw.get("daily_limit", defaults.daily_limit),
            cooldown_minutes=raw.get(
                "cooldown_minutes", defaults.cooldown_minutes
            ),
            silence_minutes=raw.get("silence_minutes", defaults.silence_minutes),
            probability_percent=raw.get(
                "probability_percent", defaults.probability_percent
            ),
            active_start=str(raw.get("active_start", defaults.active_start)),
            active_end=str(raw.get("active_end", defaults.active_end)),
            fixed_times=parse_fixed_times(raw.get("fixed_times", defaults.fixed_times)),
        )
        settings.normalize()
        return settings


@dataclass(slots=True)
class TopicRuntime:
    last_activity_at: float = 0.0
    last_sent_at: float = 0.0
    next_random_at: float = 0.0
    retry_not_before: float = 0.0
    retry_reason: str = ""
    retry_fixed_token: str = ""
    failure_count: int = 0
    last_error: str = ""
    retry_blocked: bool = False
    daily_date: str = ""
    daily_count: int = 0
    fixed_sent: set[str] = field(default_factory=set)
    recent_messages: list[dict[str, Any]] = field(default_factory=list)
    recent_topics: list[str] = field(default_factory=list)

    def roll_day(self, today: str) -> None:
        yesterday = (
            datetime.fromisoformat(today) - timedelta(days=1)
        ).strftime("%Y-%m-%d")
        self.fixed_sent = {
            token
            for token in self.fixed_sent
            if token.startswith(today + "@") or token.startswith(yesterday + "@")
        }
        if self.daily_date == today:
            return
        self.daily_date = today
        self.daily_count = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "last_activity_at": self.last_activity_at,
            "last_sent_at": self.last_sent_at,
            "next_random_at": self.next_random_at,
            "retry_not_before": self.retry_not_before,
            "retry_reason": self.retry_reason,
            "retry_fixed_token": self.retry_fixed_token,
            "failure_count": self.failure_count,
            "last_error": self.last_error,
            "retry_blocked": self.retry_blocked,
            "daily_date": self.daily_date,
            "daily_count": self.daily_count,
            "fixed_sent": sorted(self.fixed_sent),
            "recent_messages": list(self.recent_messages),
            "recent_topics": list(self.recent_topics),
        }

    @classmethod
    def from_mapping(cls, value: Any) -> TopicRuntime:
        raw = value if isinstance(value, Mapping) else {}
        fixed_sent = {
            str(item)
            for item in raw.get("fixed_sent", [])
            if str(item).strip()
        }
        legacy_fixed = raw.get("fixed_seen", {})
        if isinstance(legacy_fixed, Mapping):
            for fixed_time, day in legacy_fixed.items():
                normalized = normalize_hhmm(fixed_time)
                if normalized and str(day).strip():
                    fixed_sent.add(f"{str(day).strip()}@{normalized}")
        recent_messages = raw.get("recent_messages", [])
        recent_topics = raw.get("recent_topics", [])
        return cls(
            last_activity_at=float(raw.get("last_activity_at", 0) or 0),
            last_sent_at=float(raw.get("last_sent_at", 0) or 0),
            next_random_at=float(raw.get("next_random_at", 0) or 0),
            retry_not_before=float(raw.get("retry_not_before", 0) or 0),
            retry_reason=str(raw.get("retry_reason", "") or ""),
            retry_fixed_token=str(raw.get("retry_fixed_token", "") or ""),
            failure_count=clamp_int(raw.get("failure_count"), 0, 0, 1000000),
            last_error=str(raw.get("last_error", "") or "")[:500],
            retry_blocked=bool(raw.get("retry_blocked", False)),
            daily_date=str(raw.get("daily_date", "") or ""),
            daily_count=clamp_int(raw.get("daily_count"), 0, 0, 1000000),
            fixed_sent=fixed_sent,
            recent_messages=(
                [dict(item) for item in recent_messages if isinstance(item, Mapping)]
                if isinstance(recent_messages, list)
                else []
            ),
            recent_topics=(
                [str(item) for item in recent_topics if str(item).strip()]
                if isinstance(recent_topics, list)
                else []
            ),
        )


@dataclass(slots=True)
class TopicScope:
    scope_id: str
    identity: BotIdentity
    umo: str
    group_name: str
    settings: TopicSettings
    runtime: TopicRuntime
    legacy_unclaimed: bool = False

    @property
    def ready(self) -> bool:
        return self.identity.routable and not self.legacy_unclaimed

    def as_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity.as_dict(),
            "route": {
                "umo": self.umo,
                "group_name": self.group_name,
            },
            "settings": self.settings.as_dict(),
            "runtime": self.runtime.as_dict(),
            "legacy_unclaimed": self.legacy_unclaimed,
        }

    @classmethod
    def from_mapping(
        cls,
        scope_id: str,
        value: Any,
        defaults: TopicSettings,
    ) -> TopicScope:
        raw = value if isinstance(value, Mapping) else {}
        if isinstance(raw.get("identity"), Mapping):
            identity = BotIdentity.from_mapping(raw["identity"])
            route = raw.get("route", {})
            if not isinstance(route, Mapping):
                route = {}
            settings_raw = raw.get("settings", {})
            runtime_raw = raw.get("runtime", {})
            return cls(
                scope_id=scope_id,
                identity=identity,
                umo=str(route.get("umo", "") or ""),
                group_name=str(route.get("group_name", "") or ""),
                settings=TopicSettings.from_mapping(settings_raw, defaults),
                runtime=TopicRuntime.from_mapping(runtime_raw),
                legacy_unclaimed=bool(
                    raw.get("legacy_unclaimed", not identity.routable)
                ),
            )

        identity = BotIdentity.from_mapping(raw)
        return cls(
            scope_id=scope_id,
            identity=identity,
            umo=str(raw.get("umo", "") or ""),
            group_name=str(raw.get("group_name", "") or ""),
            settings=TopicSettings.from_mapping(raw, defaults),
            runtime=TopicRuntime.from_mapping(raw),
            legacy_unclaimed=not identity.routable,
        )


def legacy_scope_id(umo: str) -> str:
    digest = hashlib.sha256(str(umo or "").encode("utf-8")).hexdigest()[:24]
    return f"legacy-{digest}"


def new_scope_id(identity: BotIdentity, nonce: str) -> str:
    payload = json.dumps(
        {"identity": identity.as_dict(), "nonce": str(nonce)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


@dataclass(frozen=True, slots=True)
class Dispatch:
    reason: str
    fixed_token: str = ""


class SchedulePolicy:
    def __init__(
        self,
        *,
        timezone: tzinfo,
        fixed_grace_minutes: int,
        failure_retry_minutes: int,
        randint: Callable[[int, int], int] | None = None,
    ) -> None:
        self.timezone = timezone
        self.fixed_grace_minutes = clamp_int(
            fixed_grace_minutes, 10, 1, 120
        )
        self.failure_retry_minutes = clamp_int(
            failure_retry_minutes, 15, 1, 1440
        )
        self.randint = randint or random.randint

    def schedule_next(self, scope: TopicScope, base_time: float) -> None:
        settings = scope.settings
        interval = self.randint(
            settings.min_interval_minutes,
            settings.max_interval_minutes,
        )
        scope.runtime.next_random_at = base_time + interval * 60

    def evaluate(self, scope: TopicScope, now_ts: float) -> Dispatch | None:
        if not scope.ready or not scope.settings.enabled:
            return None
        settings = scope.settings
        runtime = scope.runtime
        now = datetime.fromtimestamp(now_ts, self.timezone)
        today = now.strftime("%Y-%m-%d")
        runtime.roll_day(today)

        if runtime.next_random_at <= 0:
            self.schedule_next(scope, now_ts)
        if runtime.retry_blocked:
            return None
        if runtime.retry_not_before > now_ts:
            return None

        retry_due = bool(runtime.retry_reason)
        fixed_token = (
            runtime.retry_fixed_token
            if retry_due
            else self._due_fixed_token(scope, now)
        )
        random_due = retry_due or now_ts >= runtime.next_random_at
        if not retry_due and not fixed_token and not random_due:
            return None

        now_minutes = now.hour * 60 + now.minute
        if not in_active_window(
            now_minutes,
            settings.active_start,
            settings.active_end,
        ):
            if random_due:
                runtime.next_random_at = _next_active_start(
                    now,
                    settings.active_start,
                    settings.active_end,
                )
            return None
        if runtime.daily_count >= settings.daily_limit:
            if random_due:
                runtime.next_random_at = _tomorrow_active_start(
                    now,
                    settings.active_start,
                )
            return None
        cooldown_until = runtime.last_sent_at + settings.cooldown_minutes * 60
        if runtime.last_sent_at and now_ts < cooldown_until:
            if random_due:
                runtime.next_random_at = cooldown_until
            return None
        silence_until = runtime.last_activity_at + settings.silence_minutes * 60
        if runtime.last_activity_at and now_ts < silence_until:
            if random_due:
                runtime.next_random_at = silence_until
            return None

        if retry_due:
            return Dispatch(reason="retry", fixed_token=fixed_token)
        if fixed_token:
            return Dispatch(reason="fixed", fixed_token=fixed_token)
        if self.randint(1, 100) > settings.probability_percent:
            self.schedule_next(scope, now_ts)
            return None
        return Dispatch(reason="random")

    def mark_success(
        self,
        scope: TopicScope,
        dispatch: Dispatch,
        *,
        sent_at: float,
        message: str,
        recent_topic_limit: int,
    ) -> None:
        runtime = scope.runtime
        today = datetime.fromtimestamp(sent_at, self.timezone).strftime("%Y-%m-%d")
        runtime.roll_day(today)
        runtime.last_sent_at = sent_at
        runtime.daily_count += 1
        runtime.retry_not_before = 0.0
        runtime.retry_reason = ""
        runtime.retry_fixed_token = ""
        runtime.failure_count = 0
        runtime.last_error = ""
        runtime.retry_blocked = False
        if dispatch.fixed_token:
            runtime.fixed_sent.add(dispatch.fixed_token)
        if str(message).strip():
            runtime.recent_topics.append(str(message).strip()[:500])
            limit = clamp_int(recent_topic_limit, 12, 1, 50)
            runtime.recent_topics = runtime.recent_topics[-limit:]
        self.schedule_next(scope, sent_at)

    def mark_failure(
        self,
        scope: TopicScope,
        dispatch: Dispatch,
        failed_at: float,
        *,
        detail: str = "",
        permanent: bool = False,
    ) -> None:
        if dispatch.reason == "manual":
            return
        runtime = scope.runtime
        runtime.failure_count += 1
        runtime.last_error = str(detail or "")[:500]
        if permanent:
            runtime.retry_not_before = 0.0
            runtime.retry_reason = ""
            runtime.retry_fixed_token = ""
            runtime.retry_blocked = True
            return
        multiplier = 2 ** min(max(0, runtime.failure_count - 1), 5)
        retry_minutes = min(1440, self.failure_retry_minutes * multiplier)
        retry_at = failed_at + retry_minutes * 60
        runtime.retry_not_before = retry_at
        runtime.retry_reason = dispatch.reason
        runtime.retry_fixed_token = dispatch.fixed_token
        runtime.retry_blocked = False
        if runtime.next_random_at <= failed_at:
            runtime.next_random_at = retry_at

    def _due_fixed_token(self, scope: TopicScope, now: datetime) -> str:
        grace_seconds = self.fixed_grace_minutes * 60
        for day_offset in (0, -1):
            day = now + timedelta(days=day_offset)
            for fixed_time in scope.settings.fixed_times:
                scheduled_minutes = time_to_minutes(fixed_time)
                if scheduled_minutes is None:
                    continue
                scheduled_at = day.replace(
                    hour=scheduled_minutes // 60,
                    minute=scheduled_minutes % 60,
                    second=0,
                    microsecond=0,
                )
                elapsed = (now - scheduled_at).total_seconds()
                token = f"{scheduled_at.strftime('%Y-%m-%d')}@{fixed_time}"
                if token in scope.runtime.fixed_sent:
                    continue
                if 0 <= elapsed <= grace_seconds:
                    return token
        return ""
