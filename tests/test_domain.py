from __future__ import annotations

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from astrbot_plugin_proactive_topics.domain import (
    BotIdentity,
    Dispatch,
    SchedulePolicy,
    TopicRuntime,
    TopicScope,
    TopicSettings,
    in_active_window,
    normalize_hhmm,
    parse_fixed_times,
)


TZ = ZoneInfo("Asia/Shanghai")


def timestamp(day: str, hhmm: str) -> float:
    return datetime.fromisoformat(f"{day}T{hhmm}:00").replace(tzinfo=TZ).timestamp()


def make_scope(**setting_overrides) -> TopicScope:
    settings = TopicSettings(
        enabled=True,
        min_interval_minutes=60,
        max_interval_minutes=60,
        daily_limit=3,
        cooldown_minutes=0,
        silence_minutes=0,
        probability_percent=100,
        active_start="00:00",
        active_end="00:00",
        fixed_times=[],
    )
    for key, value in setting_overrides.items():
        setattr(settings, key, value)
    settings.normalize()
    return TopicScope(
        scope_id="scope-a",
        identity=BotIdentity(
            platform_id="onebot_main",
            platform_name="aiocqhttp",
            self_id="10001",
            group_id="42",
        ),
        umo="onebot_main:GroupMessage:42",
        group_name="测试群",
        settings=settings,
        runtime=TopicRuntime(),
    )


class ParsingTests(unittest.TestCase):
    def test_time_and_fixed_time_normalization(self):
        self.assertEqual(normalize_hhmm("9:05"), "09:05")
        self.assertIsNone(normalize_hhmm("25:00"))
        self.assertEqual(
            parse_fixed_times("21:30，09:00 09:00;bad"),
            ["09:00", "21:30"],
        )

    def test_active_window_supports_daytime_and_overnight(self):
        self.assertTrue(in_active_window(10 * 60, "09:00", "22:00"))
        self.assertFalse(in_active_window(23 * 60, "09:00", "22:00"))
        self.assertTrue(in_active_window(23 * 60, "22:00", "06:00"))
        self.assertTrue(in_active_window(5 * 60, "22:00", "06:00"))
        self.assertFalse(in_active_window(12 * 60, "22:00", "06:00"))
        self.assertTrue(in_active_window(12 * 60, "00:00", "00:00"))

    def test_default_settings_keep_documented_fixed_times(self):
        settings = TopicSettings.defaults({})
        self.assertEqual(settings.fixed_times, ["09:00", "21:30"])


class IdentityTests(unittest.TestCase):
    def test_same_account_survives_platform_rename(self):
        old = BotIdentity("onebot_old", "aiocqhttp", "10001", "42")
        renamed = BotIdentity("onebot_main", "aiocqhttp", "10001", "42")
        self.assertTrue(old.matches_actor(renamed))
        self.assertFalse(old.conflicts_with(renamed))

    def test_different_bots_in_same_group_conflict(self):
        first = BotIdentity("onebot_main", "aiocqhttp", "10001", "42")
        second = BotIdentity("onebot_second", "aiocqhttp", "10002", "42")
        self.assertFalse(first.matches_actor(second))
        self.assertTrue(first.conflicts_with(second))


class SchedulePolicyTests(unittest.TestCase):
    def policy(self, values=None) -> SchedulePolicy:
        sequence = iter(values or [60, 1, 60, 1])
        return SchedulePolicy(
            timezone=TZ,
            fixed_grace_minutes=10,
            failure_retry_minutes=15,
            randint=lambda _a, _b: next(sequence),
        )

    def test_initialization_schedules_random_without_dispatch(self):
        scope = make_scope()
        now = timestamp("2026-07-27", "10:00")
        decision = self.policy([60]).evaluate(scope, now)
        self.assertIsNone(decision)
        self.assertEqual(scope.runtime.next_random_at, now + 3600)

    def test_due_fixed_time_is_not_lost_when_random_is_initialized(self):
        scope = make_scope(fixed_times=["10:00"])
        now = timestamp("2026-07-27", "10:05")
        decision = self.policy([60]).evaluate(scope, now)
        self.assertEqual(decision, Dispatch("fixed", "2026-07-27@10:00"))
        self.assertNotIn(decision.fixed_token, scope.runtime.fixed_sent)

    def test_blocked_fixed_time_remains_available_inside_grace(self):
        scope = make_scope(fixed_times=["10:00"], cooldown_minutes=10)
        scope.runtime.next_random_at = timestamp("2026-07-27", "12:00")
        scope.runtime.last_sent_at = timestamp("2026-07-27", "09:58")
        policy = self.policy()

        blocked = policy.evaluate(scope, timestamp("2026-07-27", "10:02"))
        allowed = policy.evaluate(scope, timestamp("2026-07-27", "10:08"))

        self.assertIsNone(blocked)
        self.assertEqual(allowed.fixed_token, "2026-07-27@10:00")

    def test_probability_miss_reschedules_random_candidate(self):
        scope = make_scope(probability_percent=20)
        now = timestamp("2026-07-27", "10:00")
        scope.runtime.next_random_at = now
        decision = self.policy([99, 60]).evaluate(scope, now)
        self.assertIsNone(decision)
        self.assertEqual(scope.runtime.next_random_at, now + 3600)

    def test_silence_defers_random_to_exact_threshold(self):
        scope = make_scope(silence_minutes=30)
        now = timestamp("2026-07-27", "10:00")
        scope.runtime.next_random_at = now
        scope.runtime.last_activity_at = timestamp("2026-07-27", "09:50")
        decision = self.policy().evaluate(scope, now)
        self.assertIsNone(decision)
        self.assertEqual(
            scope.runtime.next_random_at,
            timestamp("2026-07-27", "10:20"),
        )

    def test_success_is_the_only_operation_that_consumes_fixed_slot(self):
        scope = make_scope(fixed_times=["10:00"])
        now = timestamp("2026-07-27", "10:01")
        scope.runtime.next_random_at = now + 3600
        policy = self.policy([60])
        dispatch = policy.evaluate(scope, now)
        self.assertNotIn(dispatch.fixed_token, scope.runtime.fixed_sent)

        policy.mark_success(
            scope,
            dispatch,
            sent_at=now,
            message="测试话题",
            recent_topic_limit=12,
        )

        self.assertIn("2026-07-27@10:00", scope.runtime.fixed_sent)
        self.assertEqual(scope.runtime.daily_count, 1)
        self.assertEqual(scope.runtime.recent_topics, ["测试话题"])

    def test_failure_adds_retry_gate_without_counting_a_message(self):
        scope = make_scope()
        now = timestamp("2026-07-27", "10:00")
        scope.runtime.next_random_at = now
        policy = self.policy()
        policy.mark_failure(scope, Dispatch("random"), now)
        self.assertEqual(scope.runtime.retry_not_before, now + 15 * 60)
        self.assertEqual(scope.runtime.daily_count, 0)
        self.assertIsNone(policy.evaluate(scope, now + 60))

    def test_failed_fixed_dispatch_retries_after_original_grace(self):
        scope = make_scope(fixed_times=["10:00"])
        now = timestamp("2026-07-27", "10:05")
        scope.runtime.next_random_at = now + 3600
        policy = self.policy()
        dispatch = policy.evaluate(scope, now)
        policy.mark_failure(scope, dispatch, now)

        retry = policy.evaluate(scope, timestamp("2026-07-27", "10:20"))

        self.assertEqual(retry.reason, "retry")
        self.assertEqual(retry.fixed_token, "2026-07-27@10:00")

    def test_fixed_grace_crosses_midnight_without_duplicate(self):
        scope = make_scope(fixed_times=["23:58"])
        scope.runtime.next_random_at = timestamp("2026-07-28", "02:00")
        policy = self.policy()
        dispatch = policy.evaluate(scope, timestamp("2026-07-28", "00:03"))
        self.assertEqual(dispatch.fixed_token, "2026-07-27@23:58")

        scope.runtime.fixed_sent.add("2026-07-27@23:58")
        self.assertIsNone(
            policy.evaluate(scope, timestamp("2026-07-28", "00:04"))
        )


if __name__ == "__main__":
    unittest.main()
