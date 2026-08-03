from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path


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

from astrbot_plugin_proactive_topics.domain import Dispatch, TopicRuntime
from astrbot_plugin_proactive_topics.main import ProactiveTopics


class _ConversationManager:
    async def get_curr_conversation_id(self, _umo):
        return None

    async def get_conversation(self, _umo, _conversation_id):
        return None


class _PersonaManager:
    async def resolve_selected_persona(self, **_kwargs):
        return None, {"prompt": "当前 Bot 的原生人格"}, None, None

    async def get_default_persona_v3(self, _umo):
        return {"prompt": "默认人格"}


class _Context:
    def __init__(self):
        self.conversation_manager = _ConversationManager()
        self.persona_manager = _PersonaManager()
        self.last_llm_call = None
        self.llm_calls = []
        self.sent = []
        self.completion_text = "要不要分享一下今天最意外的小发现？"
        self.completion_texts = []

    async def get_current_chat_provider_id(self, _umo):
        return "provider_a"

    async def llm_generate(self, **kwargs):
        self.last_llm_call = kwargs
        self.llm_calls.append(kwargs)
        completion = (
            self.completion_texts.pop(0)
            if self.completion_texts
            else self.completion_text
        )
        return types.SimpleNamespace(completion_text=completion)

    async def send_message(self, umo, message):
        self.sent.append((umo, message))
        return True

    def get_config(self, _umo):
        return {"provider_settings": {}}


class _Event:
    def __init__(
        self,
        *,
        platform_id="onebot_main",
        self_id="10001",
        group_id="42",
        sender_id="90001",
        text="普通群消息",
        message_id="m-1",
        umo="aiocqhttp:GroupMessage:42",
        platform_name="aiocqhttp",
    ):
        self._platform_id = platform_id
        self._self_id = self_id
        self._group_id = group_id
        self._sender_id = sender_id
        self._text = text
        self._platform_name = platform_name
        self.unified_msg_origin = umo
        self.sent = []
        group = types.SimpleNamespace(
            group_name="测试群",
            group_owner="90001",
            group_admins=[],
        )
        self.message_obj = types.SimpleNamespace(
            group=group,
            message_id=message_id,
            raw_message={"message_id": message_id, "sender": {"role": "owner"}},
        )

    def get_platform_id(self):
        return self._platform_id

    def get_platform_name(self):
        return self._platform_name

    def get_self_id(self):
        return self._self_id

    def get_group_id(self):
        return self._group_id

    def get_sender_id(self):
        return self._sender_id

    def get_sender_name(self):
        return f"用户{self._sender_id}"

    def get_message_str(self):
        return self._text

    def is_admin(self):
        return True

    def plain_result(self, text):
        return str(text)

    async def send(self, message):
        self.sent.append(message)


class PluginRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def make_plugin(self, **overrides):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        config = {
            "data_dir": directory.name,
            "botmesh_compat_enabled": True,
            "generation_max_tokens": 180,
        }
        config.update(overrides)
        context = _Context()
        return ProactiveTopics(context, config), context

    def create_scope(self, plugin, event):
        scope = plugin._resolve_scope_locked(
            event,
            create=True,
            claim_legacy=True,
        )
        self.assertIsNotNone(scope)
        return scope

    async def test_same_umo_is_isolated_by_bot_identity(self):
        plugin, _context = self.make_plugin()
        bot_a = _Event(platform_id="onebot_main", self_id="10001")
        bot_b = _Event(platform_id="onebot_second", self_id="10002")

        scope_a = self.create_scope(plugin, bot_a)
        scope_b = self.create_scope(plugin, bot_b)

        self.assertNotEqual(scope_a.scope_id, scope_b.scope_id)
        self.assertEqual(len(plugin.scopes), 2)
        self.assertEqual(
            {scope.identity.self_id for scope in plugin.scopes.values()},
            {"10001", "10002"},
        )

    async def test_dynamic_astrbot_module_name_finds_registered_botmesh(self):
        plugin, _context = self.make_plugin()
        module_name = "astrbot.dynamic.astrbot_plugin_botmesh.integration"
        dynamic_module = types.ModuleType(module_name)
        dynamic_module.__file__ = (
            r"C:\AstrBot\data\plugins\astrbot_plugin_botmesh\integration.py"
        )
        dynamic_module._provider = object()
        dynamic_module.get_proactive_topics_context = lambda **_kwargs: None
        dynamic_module.dispatch_proactive_topic = lambda **_kwargs: None
        sys.modules[module_name] = dynamic_module
        self.addCleanup(sys.modules.pop, module_name, None)

        self.assertIs(plugin.botmesh.installed_module(), dynamic_module)

    async def test_enable_command_applies_only_to_current_bot_and_group(self):
        plugin, _context = self.make_plugin()
        event = _Event(text="/主动话题 开启")

        replies = [item async for item in plugin.proactive_topic_command(event)]

        self.assertEqual(len(replies), 1)
        self.assertIn("已开启", replies[0])
        self.assertEqual(len(plugin.scopes), 1)
        scope = next(iter(plugin.scopes.values()))
        self.assertTrue(scope.settings.enabled)
        self.assertGreater(scope.runtime.next_random_at, 0)
        self.assertTrue(plugin.state_store.path.is_file())

    async def test_activity_is_deduplicated_and_other_bot_cannot_touch_scope(self):
        plugin, _context = self.make_plugin()
        event_a = _Event(message_id="same-message")
        scope_a = self.create_scope(plugin, event_a)
        scope_a.settings.enabled = True

        await plugin.remember_group_activity(event_a)
        await plugin.remember_group_activity(event_a)
        last_activity = scope_a.runtime.last_activity_at
        event_b = _Event(
            platform_id="onebot_second",
            self_id="10002",
            message_id="other-bot-copy",
        )
        await plugin.remember_group_activity(event_b)

        self.assertEqual(len(scope_a.runtime.recent_messages), 1)
        self.assertEqual(scope_a.runtime.last_activity_at, last_activity)
        self.assertNotIn(event_b, plugin._latest_events.values())

    async def test_non_text_activity_resets_silence_without_polluting_prompt(self):
        plugin, _context = self.make_plugin()
        event = _Event(text="", message_id="image-only")
        scope = self.create_scope(plugin, event)
        scope.settings.enabled = True
        scope.runtime.last_activity_at = 1.0

        await plugin.remember_group_activity(event)

        self.assertGreater(scope.runtime.last_activity_at, 1.0)
        self.assertEqual(scope.runtime.recent_messages, [])

    async def test_verified_botmesh_frame_is_removed_before_local_capture(self):
        plugin, _context = self.make_plugin()
        plugin.botmesh.set_module_for_testing(
            types.SimpleNamespace(
                normalize_chat_history_message=lambda **_kwargs: "可见正文",
            )
        )
        event = _Event(text="可见正文<hidden-frame>")
        scope = self.create_scope(plugin, event)
        scope.settings.enabled = True

        await plugin.remember_group_activity(event)

        self.assertEqual(scope.runtime.recent_messages[0]["text"], "可见正文")

    async def test_verified_botmesh_sender_identity_replaces_platform_echo(self):
        plugin, _context = self.make_plugin()
        plugin.botmesh.set_module_for_testing(
            types.SimpleNamespace(
                normalize_chat_history_record=lambda **_kwargs: {
                    "content": "小B的可见正文",
                    "sender_id": "10002",
                    "sender_name": "小B",
                    "source_bot_id": "bot_b",
                },
            )
        )
        event = _Event(
            text="小B的可见正文<hidden-frame>",
            sender_id="platform-echo-does-not-expose-bot-account",
        )
        scope = self.create_scope(plugin, event)
        scope.settings.enabled = True

        await plugin.remember_group_activity(event)

        self.assertEqual(scope.runtime.recent_messages[0]["sender_id"], "10002")
        self.assertEqual(scope.runtime.recent_messages[0]["sender"], "小B")
        self.assertEqual(scope.runtime.recent_messages[0]["source_bot_id"], "bot_b")

    async def test_no_installed_botmesh_uses_native_persona(self):
        plugin, context = self.make_plugin()
        plugin.botmesh.installed_module = lambda: None
        event = _Event(umo="onebot_main:GroupMessage:42")
        scope = self.create_scope(plugin, event)

        success, detail = await plugin._generate_and_send(
            scope,
            Dispatch("manual"),
            event=event,
        )

        self.assertTrue(success)
        self.assertIn("当前 Bot 的原生人格", context.last_llm_call["system_prompt"])
        self.assertIn(
            "本次主动发言没有默认的当前对话者",
            context.last_llm_call["system_prompt"],
        )
        self.assertEqual(detail, "要不要分享一下今天最意外的小发现？")
        self.assertEqual(len(event.sent), 1)

    async def test_botmesh_dispatch_is_the_only_generation_and_send_owner(self):
        plugin, context = self.make_plugin()
        received = []

        async def dispatch_proactive_topic(**kwargs):
            received.append(kwargs)
            return {
                "success": True,
                "proactive_dispatch_version": 1,
                "content": "A称呼B，要不要一起看看这个问题？",
                "bot_id": "bot_a",
                "platform_id": "onebot_main",
                "account_id": "10001",
                "raw_group_id": "42",
                "logical_group_id": "main_group",
                "persona_scope": "group:main_group",
                "persona_fingerprint": "1234567890abcdef",
                "target_id": "bot_b",
                "audience": "target",
            }

        plugin.botmesh.set_module_for_testing(
            types.SimpleNamespace(dispatch_proactive_topic=dispatch_proactive_topic)
        )
        event = _Event()
        scope = self.create_scope(plugin, event)
        scope.runtime.recent_messages = [
            {
                "sender": "小B",
                "sender_id": "10002",
                "source_bot_id": "bot_b",
                "text": "刚才还聊了本地记录",
            }
        ]

        success, detail = await plugin._generate_and_send(
            scope,
            Dispatch("manual"),
            event=event,
        )

        self.assertTrue(success, detail)
        self.assertEqual(detail, "A称呼B，要不要一起看看这个问题？")
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["identity"]["self_id"], "10001")
        self.assertEqual(received[0]["local_history"][0]["source_bot_id"], "bot_b")
        self.assertEqual(received[0]["trigger"]["reason"], "manual")
        self.assertIn("task_prompt", received[0]["generation_options"])
        self.assertIsNone(context.last_llm_call)
        self.assertEqual(context.sent, [])
        self.assertEqual(event.sent, [])

    async def test_botmesh_diagnosis_still_uses_read_only_context(self):
        plugin, _context = self.make_plugin()

        async def get_proactive_topics_context(**_kwargs):
            return {
                "enabled": True,
                "proactive_contract_version": 2,
                "bot_id": "bot_a",
                "platform_id": "onebot_main",
                "account_id": "10001",
                "raw_group_id": "42",
                "logical_group_id": "main_group",
                "persona_prompt": "BotMesh 小A群人格",
                "persona_scope": "group:main_group",
                "persona_fingerprint": "1234567890abcdef",
                "address_book": [{"target_id": "bot_b"}],
            }

        async def dispatch_proactive_topic(**_kwargs):
            raise AssertionError("诊断不应派发")

        plugin.botmesh.set_module_for_testing(
            types.SimpleNamespace(
                get_proactive_topics_context=get_proactive_topics_context,
                dispatch_proactive_topic=dispatch_proactive_topic,
            )
        )
        event = _Event()
        scope = self.create_scope(plugin, event)

        diagnostic = await plugin._diagnose_scope(scope, event)

        self.assertIn("BotMesh bot_id：bot_a", diagnostic)
        self.assertIn("逻辑群：main_group", diagnostic)
        self.assertIn("Persona 作用域：group:main_group", diagnostic)
        self.assertIn("精确称呼条目：1", diagnostic)
        self.assertIn("全部委托 BotMesh", diagnostic)

    async def test_background_botmesh_dispatch_uses_persisted_identity(self):
        plugin, context = self.make_plugin()
        received = {}

        async def dispatch_proactive_topic(**kwargs):
            received.update(kwargs)
            return {
                "success": True,
                "proactive_dispatch_version": 1,
                "content": "大家，继续聊聊？",
                "bot_id": "bot_a",
                "platform_id": "onebot_main",
                "account_id": "10001",
                "raw_group_id": "42",
                "logical_group_id": "main_group",
                "persona_scope": "group:main_group",
                "persona_fingerprint": "1234567890abcdef",
                "target_id": "",
                "audience": "group",
            }

        plugin.botmesh.set_module_for_testing(
            types.SimpleNamespace(dispatch_proactive_topic=dispatch_proactive_topic)
        )
        event = _Event(umo="onebot_main:GroupMessage:42")
        scope = self.create_scope(plugin, event)

        success, detail = await plugin._generate_and_send(
            scope,
            Dispatch("random"),
            event=None,
        )

        self.assertTrue(success, detail)
        self.assertIsNone(received["event"])
        self.assertEqual(received["identity"]["platform_id"], "onebot_main")
        self.assertEqual(received["identity"]["self_id"], "10001")
        self.assertIsNone(context.last_llm_call)
        self.assertEqual(context.sent, [])

    async def test_qqofficial_background_botmesh_dispatch_uses_persisted_route(self):
        plugin, context = self.make_plugin()
        received = {}

        async def dispatch_proactive_topic(**kwargs):
            received.update(kwargs)
            return {
                "success": True,
                "proactive_dispatch_version": 1,
                "content": "大家，继续聊聊？",
                "bot_id": "bot_a",
                "platform_id": "default_1905252075",
                "account_id": "OPENID",
                "raw_group_id": "GROUP_OPENID",
                "logical_group_id": "main_group",
                "persona_scope": "group:main_group",
                "persona_fingerprint": "1234567890abcdef",
                "target_id": "",
                "audience": "group",
            }

        plugin.botmesh.set_module_for_testing(
            types.SimpleNamespace(dispatch_proactive_topic=dispatch_proactive_topic)
        )
        event = _Event(
            platform_id="default_1905252075",
            platform_name="qqofficial",
            self_id="OPENID",
            group_id="GROUP_OPENID",
            umo="default_1905252075:GroupMessage:GROUP_OPENID",
        )
        scope = self.create_scope(plugin, event)

        success, detail = await plugin._generate_and_send(
            scope,
            Dispatch("retry"),
            event=None,
        )

        self.assertTrue(success, detail)
        self.assertIsNone(received["event"])
        self.assertEqual(received["identity"]["platform_id"], "default_1905252075")
        self.assertEqual(received["identity"]["group_id"], "GROUP_OPENID")
        self.assertIsNone(context.last_llm_call)
        self.assertEqual(context.sent, [])

    async def test_botmesh_dispatch_rejection_is_fail_closed(self):
        plugin, context = self.make_plugin()

        async def dispatch_proactive_topic(**_kwargs):
            return {
                "success": False,
                "proactive_dispatch_version": 1,
                "error": "provider_unavailable",
                "platform_id": "onebot_main",
                "account_id": "10001",
                "raw_group_id": "42",
            }

        plugin.botmesh.set_module_for_testing(
            types.SimpleNamespace(dispatch_proactive_topic=dispatch_proactive_topic)
        )
        event = _Event()
        scope = self.create_scope(plugin, event)

        success, detail = await plugin._generate_and_send(
            scope,
            Dispatch("manual"),
            event=event,
        )

        self.assertFalse(success)
        self.assertIn("provider_unavailable", detail)
        self.assertIsNone(context.last_llm_call)
        self.assertEqual(event.sent, [])

    async def test_old_botmesh_without_dispatch_api_is_fail_closed(self):
        plugin, context = self.make_plugin()
        plugin.botmesh.set_module_for_testing(
            types.SimpleNamespace(get_proactive_topics_context=lambda **_kwargs: {})
        )
        event = _Event()
        scope = self.create_scope(plugin, event)

        success, detail = await plugin._generate_and_send(
            scope,
            Dispatch("manual"),
            event=event,
        )

        self.assertFalse(success)
        self.assertIn("dispatch_api_unavailable", detail)
        self.assertIsNone(context.last_llm_call)
        self.assertEqual(event.sent, [])

    async def test_ambiguous_adapter_umo_requires_fresh_event_after_restart(self):
        plugin, context = self.make_plugin(botmesh_compat_enabled=False)
        event = _Event(umo="aiocqhttp:GroupMessage:42")
        scope = self.create_scope(plugin, event)

        success, detail = await plugin._generate_and_send(
            scope,
            Dispatch("random"),
            event=None,
        )

        self.assertFalse(success)
        self.assertIn("无法确认发送路由", detail)
        self.assertIsNone(context.last_llm_call)

    async def test_real_qqofficial_name_always_requires_fresh_event(self):
        plugin, _context = self.make_plugin(botmesh_compat_enabled=False)
        event = _Event(
            platform_id="default1905252075",
            platform_name="qqofficial",
            self_id="OPENID",
            group_id="GROUP_OPENID",
            umo="default_1905252075:GroupMessage:GROUP_OPENID",
        )
        scope = self.create_scope(plugin, event)

        self.assertTrue(plugin._route_requires_event(scope))
        self.assertTrue(plugin._is_qq_official("qq_official"))
        self.assertTrue(plugin._is_qq_official("qqofficial"))

    async def test_cached_event_from_another_bot_is_rejected_before_llm(self):
        plugin, context = self.make_plugin(botmesh_compat_enabled=False)
        event_a = _Event(platform_id="onebot_main", self_id="10001")
        event_b = _Event(platform_id="onebot_second", self_id="10002")
        scope = self.create_scope(plugin, event_a)

        success, detail = await plugin._generate_and_send(
            scope,
            Dispatch("random"),
            event=event_b,
        )

        self.assertFalse(success)
        self.assertIn("另一 Bot", detail)
        self.assertIsNone(context.last_llm_call)

    async def test_finish_attempt_counts_only_successful_send(self):
        plugin, _context = self.make_plugin(botmesh_compat_enabled=False)
        event = _Event(umo="onebot_main:GroupMessage:42")
        scope = self.create_scope(plugin, event)
        scope.runtime = TopicRuntime()
        plugin._inflight.add(scope.scope_id)

        await plugin._finish_attempt(
            scope.scope_id,
            Dispatch("random"),
            False,
            "发送失败",
        )
        self.assertEqual(scope.runtime.daily_count, 0)
        self.assertGreater(scope.runtime.retry_not_before, 0)

        plugin._inflight.add(scope.scope_id)
        await plugin._finish_attempt(
            scope.scope_id,
            Dispatch("manual"),
            True,
            "成功话题",
        )
        self.assertEqual(scope.runtime.daily_count, 1)
        self.assertEqual(scope.runtime.recent_topics, ["成功话题"])

    async def test_status_command_claims_legacy_state_without_losing_settings(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        state_path = Path(directory.name) / "state.json"
        state_path.write_text(
            json.dumps(
                {
                    "version": 2,
                    "groups": {
                        "aiocqhttp:GroupMessage:42": {
                            "umo": "aiocqhttp:GroupMessage:42",
                            "group_id": "42",
                            "enabled": True,
                            "daily_limit": 7,
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        plugin = ProactiveTopics(
            _Context(),
            {"data_dir": directory.name, "botmesh_compat_enabled": False},
        )
        event = _Event(text="/主动话题 状态")

        replies = [item async for item in plugin.proactive_topic_command(event)]
        scope = next(iter(plugin.scopes.values()))

        self.assertEqual(len(replies), 1)
        self.assertTrue(scope.ready)
        self.assertTrue(scope.settings.enabled)
        self.assertEqual(scope.settings.daily_limit, 7)
        self.assertEqual(scope.identity.self_id, "10001")


if __name__ == "__main__":
    unittest.main()
