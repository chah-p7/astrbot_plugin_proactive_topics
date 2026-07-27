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

    async def get_current_chat_provider_id(self, _umo):
        return "provider_a"

    async def llm_generate(self, **kwargs):
        self.last_llm_call = kwargs
        return types.SimpleNamespace(completion_text="要不要一起聊聊今天最意外的小发现？")


class _Event:
    def __init__(self):
        self.sent = []

    async def send(self, message):
        self.sent.append(message)


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
                "enabled": True,
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


if __name__ == "__main__":
    unittest.main()
