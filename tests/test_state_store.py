from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from astrbot_plugin_proactive_topics.domain import STATE_VERSION, TopicSettings
from astrbot_plugin_proactive_topics.state_store import StateStore


class StateStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "state.json"
        self.store = StateStore(self.path)
        self.defaults = TopicSettings.defaults({})

    def write(self, payload):
        self.path.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8",
        )

    def test_v2_flat_group_is_migrated_to_identity_scoped_v3(self):
        self.write(
            {
                "version": 2,
                "groups": {
                    "aiocqhttp:GroupMessage:42": {
                        "umo": "aiocqhttp:GroupMessage:42",
                        "platform_id": "onebot_main",
                        "platform_name": "aiocqhttp",
                        "self_id": "10001",
                        "group_id": "42",
                        "enabled": True,
                        "daily_limit": 5,
                        "fixed_seen": {"09:00": "2026-07-27"},
                    }
                },
            }
        )

        scopes, migrated = self.store.load(self.defaults)

        self.assertTrue(migrated)
        self.assertEqual(len(scopes), 1)
        scope = next(iter(scopes.values()))
        self.assertTrue(scope.ready)
        self.assertEqual(scope.identity.platform_id, "onebot_main")
        self.assertEqual(scope.identity.self_id, "10001")
        self.assertTrue(scope.settings.enabled)
        self.assertEqual(scope.settings.daily_limit, 5)
        self.assertIn("2026-07-27@09:00", scope.runtime.fixed_sent)

        self.store.save(scopes)
        written = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(written["version"], STATE_VERSION)
        saved = next(iter(written["scopes"].values()))
        self.assertIn("identity", saved)
        self.assertIn("settings", saved)
        self.assertIn("runtime", saved)
        self.assertNotIn("groups", written)

    def test_legacy_state_without_bot_identity_is_never_scheduler_ready(self):
        self.write(
            {
                "version": 1,
                "groups": {
                    "aiocqhttp:GroupMessage:42": {
                        "umo": "aiocqhttp:GroupMessage:42",
                        "group_id": "42",
                        "enabled": True,
                    }
                },
            }
        )

        scopes, migrated = self.store.load(self.defaults)
        scope = next(iter(scopes.values()))

        self.assertTrue(migrated)
        self.assertTrue(scope.legacy_unclaimed)
        self.assertFalse(scope.ready)
        self.assertTrue(scope.settings.enabled)

    def test_v3_round_trip_preserves_multiple_bots_in_one_raw_group(self):
        self.write(
            {
                "version": 3,
                "scopes": {
                    "scope-a": {
                        "identity": {
                            "platform_id": "onebot_main",
                            "self_id": "10001",
                            "group_id": "42",
                        },
                        "route": {
                            "umo": "aiocqhttp:GroupMessage:42",
                            "group_name": "测试群",
                        },
                        "settings": {"enabled": True},
                        "runtime": {},
                    },
                    "scope-b": {
                        "identity": {
                            "platform_id": "onebot_second",
                            "self_id": "10002",
                            "group_id": "42",
                        },
                        "route": {
                            "umo": "aiocqhttp:GroupMessage:42",
                            "group_name": "测试群",
                        },
                        "settings": {"enabled": False},
                        "runtime": {},
                    },
                },
            }
        )

        scopes, migrated = self.store.load(self.defaults)

        self.assertFalse(migrated)
        self.assertEqual(set(scopes), {"scope-a", "scope-b"})
        self.assertEqual(
            {scope.identity.self_id for scope in scopes.values()},
            {"10001", "10002"},
        )


if __name__ == "__main__":
    unittest.main()
