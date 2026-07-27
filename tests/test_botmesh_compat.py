from __future__ import annotations

import sys
import tempfile
import types
import unittest


class _Logger:
    def __getattr__(self, _name):
        return lambda *_args, **_kwargs: None


def _decorator(*_args, **_kwargs):
    return lambda function: function


class _MessageChain:
    def __init__(self):
        self.text = ""

    def message(self, text):
        self.text = str(text)
        return self


class _Star:
    def __init__(self, context, config=None):
        self.context = context
        self.config = config


def _install_astrbot_stubs() -> None:
    astrbot = types.ModuleType("astrbot")
    astrbot.__path__ = []
    api = types.ModuleType("astrbot.api")
    api.__path__ = []
    api.AstrBotConfig = dict
    api.logger = _Logger()
    event = types.ModuleType("astrbot.api.event")
    event.AstrMessageEvent = object
    event.MessageChain = _MessageChain
    event.filter = types.SimpleNamespace(
        event_message_type=_decorator,
        command=_decorator,
        EventMessageType=types.SimpleNamespace(GROUP_MESSAGE="group"),
    )
    star = types.ModuleType("astrbot.api.star")
    star.Context = object
    star.Star = _Star
    astrbot.api = api
    sys.modules.update(
        {
            "astrbot": astrbot,
            "astrbot.api": api,
            "astrbot.api.event": event,
            "astrbot.api.star": star,
        }
    )


_install_astrbot_stubs()

from astrbot_plugin_proactive_topics.main import ProactiveTopics


class _Context:
    def __init__(self):
        self.last_llm_call = None
        self.sent = []

    async def get_current_chat_provider_id(self, _umo):
        return "provider_a"

    async def llm_generate(self, **kwargs):
        self.last_llm_call = kwargs
        return types.SimpleNamespace(completion_text="要不要一起聊聊今天最意外的小发现？")

    async def send_message(self, umo, message):
        self.sent.append((umo, message))
        return True


class _Event:
    def __init__(self):
        self.sent = []

    async def send(self, message):
        self.sent.append(message)


class _IdentityEvent:
    def __init__(self, *, platform_id, self_id, group_id="42"):
        self.unified_msg_origin = "aiocqhttp:GroupMessage:42"
        self._platform_id = platform_id
        self._self_id = self_id
        self._group_id = group_id
        self.message_obj = types.SimpleNamespace(group=None)

    def get_platform_id(self):
        return self._platform_id

    def get_platform_name(self):
        return "aiocqhttp"

    def get_self_id(self):
        return self._self_id

    def get_group_id(self):
        return self._group_id


class BotMeshCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_generation_uses_botmesh_persona_policy_history_and_frame(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        context = _Context()
        plugin = ProactiveTopics(
            context,
            {
                "data_dir": directory.name,
                "botmesh_compat_enabled": True,
                "generation_max_tokens": 180,
            },
        )

        async def get_proactive_topics_context(**_kwargs):
            return {
                "available": True,
                "enabled": True,
                "platform_id": "onebot_main",
                "account_id": "10001",
                "raw_group_id": "42",
                "persona_prompt": "BotMesh 群专属人格",
                "policy_prompt": "BotMesh 当前关系与身份策略",
                "history_context": "BotMesh + chat_history_context 持久化历史",
            }

        def wrap_proactive_topics_message(*, content, **_kwargs):
            return f"{content}<hidden-botmesh-display>"

        plugin._botmesh_integration = types.SimpleNamespace(
            get_proactive_topics_context=get_proactive_topics_context,
            wrap_proactive_topics_message=wrap_proactive_topics_message,
        )
        event = _Event()
        group = plugin._new_group(
            "onebot_main:GroupMessage:42",
            group_id="42",
            group_name="测试群",
            platform_id="onebot_main",
            self_id="10001",
        )
        group["recent_messages"] = [
            {"sender": "旧上下文", "text": "不应覆盖 BotMesh 历史"}
        ]

        success, detail = await plugin._generate_and_send(
            group["umo"],
            group,
            "manual",
            event=event,
        )

        self.assertTrue(success)
        self.assertEqual(detail, "要不要一起聊聊今天最意外的小发现？")
        self.assertIn("BotMesh 群专属人格", context.last_llm_call["system_prompt"])
        self.assertIn("BotMesh 当前关系与身份策略", context.last_llm_call["system_prompt"])
        self.assertIn(
            "BotMesh + chat_history_context 持久化历史",
            context.last_llm_call["prompt"],
        )
        self.assertNotIn("不应覆盖 BotMesh 历史", context.last_llm_call["prompt"])
        self.assertEqual(len(event.sent), 1)
        self.assertIn("<hidden-botmesh-display>", event.sent[0].text)

    async def test_background_generation_uses_persisted_bot_identity(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        context = _Context()
        plugin = ProactiveTopics(
            context,
            {"data_dir": directory.name, "botmesh_compat_enabled": True},
        )
        received = {}

        async def get_proactive_topics_context(**kwargs):
            received.update(kwargs)
            return {
                "available": True,
                "enabled": True,
                "bot_id": "bot_a",
                "platform_id": "onebot_main",
                "account_id": "10001",
                "raw_group_id": "42",
                "persona_prompt": "只属于小A的人格",
            }

        plugin._botmesh_integration = types.SimpleNamespace(
            get_proactive_topics_context=get_proactive_topics_context,
            wrap_proactive_topics_message=lambda content, **_kwargs: content,
        )
        group = plugin._new_group(
            "onebot_main:GroupMessage:42",
            group_id="42",
            platform_id="onebot_main",
            platform_name="aiocqhttp",
            self_id="10001",
        )

        success, _detail = await plugin._generate_and_send(
            group["umo"],
            group,
            "random",
        )

        self.assertTrue(success)
        self.assertEqual(
            received["identity"],
            {
                "platform_id": "onebot_main",
                "platform_name": "aiocqhttp",
                "self_id": "10001",
                "group_id": "42",
            },
        )
        self.assertIn("只属于小A的人格", context.last_llm_call["system_prompt"])
        self.assertEqual(len(context.sent), 1)

    async def test_botmesh_identity_mismatch_is_rejected_before_generation(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        context = _Context()
        plugin = ProactiveTopics(
            context,
            {"data_dir": directory.name, "botmesh_compat_enabled": True},
        )

        async def get_proactive_topics_context(**_kwargs):
            return {
                "available": True,
                "enabled": True,
                "bot_id": "bot_b",
                "platform_id": "onebot_second",
                "account_id": "10002",
                "raw_group_id": "42",
                "persona_prompt": "错误人格",
            }

        plugin._botmesh_integration = types.SimpleNamespace(
            get_proactive_topics_context=get_proactive_topics_context,
            wrap_proactive_topics_message=lambda content, **_kwargs: content,
        )
        group = plugin._new_group(
            "onebot_main:GroupMessage:42",
            group_id="42",
            platform_id="onebot_main",
            self_id="10001",
        )

        success, detail = await plugin._generate_and_send(
            group["umo"],
            group,
            "random",
        )

        self.assertFalse(success)
        self.assertIn("身份校验失败", detail)
        self.assertIsNone(context.last_llm_call)
        self.assertEqual(context.sent, [])

    async def test_active_botmesh_failure_never_falls_back_to_native_persona(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        context = _Context()
        plugin = ProactiveTopics(
            context,
            {"data_dir": directory.name, "botmesh_compat_enabled": True},
        )

        async def get_proactive_topics_context(**_kwargs):
            return {
                "available": True,
                "enabled": False,
                "error": "identity_unresolved",
            }

        plugin._botmesh_integration = types.SimpleNamespace(
            get_proactive_topics_context=get_proactive_topics_context,
            wrap_proactive_topics_message=lambda content, **_kwargs: content,
        )
        group = plugin._new_group(
            "onebot_main:GroupMessage:42",
            group_id="42",
            platform_id="onebot_main",
            self_id="10001",
        )

        success, detail = await plugin._generate_and_send(
            group["umo"],
            group,
            "random",
        )

        self.assertFalse(success)
        self.assertIn("已拒绝发送", detail)
        self.assertIsNone(context.last_llm_call)
        self.assertEqual(context.sent, [])

    async def test_adapter_umo_waits_for_fresh_event_after_restart(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        context = _Context()
        plugin = ProactiveTopics(
            context,
            {"data_dir": directory.name, "botmesh_compat_enabled": True},
        )
        group = plugin._new_group(
            "aiocqhttp:GroupMessage:42",
            group_id="42",
            platform_id="onebot_main",
            self_id="10001",
        )

        success, detail = await plugin._generate_and_send(
            group["umo"],
            group,
            "random",
        )

        self.assertFalse(success)
        self.assertIn("无法确认主动发送路由", detail)
        self.assertIsNone(context.last_llm_call)

    async def test_unloaded_installed_botmesh_is_fail_closed(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        context = _Context()
        plugin = ProactiveTopics(
            context,
            {"data_dir": directory.name, "botmesh_compat_enabled": True},
        )

        async def get_proactive_topics_context(**_kwargs):
            return {
                "available": False,
                "enabled": False,
                "error": "provider_unavailable",
            }

        plugin._botmesh_integration = types.SimpleNamespace(
            get_proactive_topics_context=get_proactive_topics_context,
            wrap_proactive_topics_message=lambda content, **_kwargs: content,
        )
        group = plugin._new_group(
            "onebot_main:GroupMessage:42",
            group_id="42",
            platform_id="onebot_main",
            self_id="10001",
        )

        success, detail = await plugin._generate_and_send(
            group["umo"],
            group,
            "random",
        )

        self.assertFalse(success)
        self.assertIn("provider_unavailable", detail)
        self.assertIsNone(context.last_llm_call)

    async def test_another_bot_cannot_overwrite_saved_group_identity(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        plugin = ProactiveTopics(
            _Context(),
            {"data_dir": directory.name, "botmesh_compat_enabled": True},
        )
        bot_a_event = _IdentityEvent(platform_id="onebot_main", self_id="10001")
        bot_b_event = _IdentityEvent(platform_id="onebot_second", self_id="10002")

        group = plugin._ensure_group_locked(bot_a_event)
        rejected = plugin._ensure_group_locked(bot_b_event)

        self.assertIsNotNone(group)
        self.assertIsNone(rejected)
        self.assertEqual(group["platform_id"], "onebot_main")
        self.assertEqual(group["self_id"], "10001")

    async def test_same_account_can_refresh_a_renamed_platform_id(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        plugin = ProactiveTopics(
            _Context(),
            {"data_dir": directory.name, "botmesh_compat_enabled": True},
        )
        old_event = _IdentityEvent(platform_id="onebot_old", self_id="10001")
        renamed_event = _IdentityEvent(platform_id="onebot_main", self_id="10001")

        group = plugin._ensure_group_locked(old_event)
        refreshed = plugin._ensure_group_locked(renamed_event)

        self.assertIs(group, refreshed)
        self.assertEqual(group["platform_id"], "onebot_main")
        self.assertEqual(group["self_id"], "10001")


if __name__ == "__main__":
    unittest.main()
