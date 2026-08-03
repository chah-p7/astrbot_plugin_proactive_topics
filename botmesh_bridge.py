from __future__ import annotations

import importlib
import sys
from dataclasses import dataclass
from typing import Any

from astrbot.api import logger

from .domain import BotIdentity


class BotMeshError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BotMeshContext:
    active: bool
    contract_version: int = 0
    persona_prompt: str = ""
    policy_prompt: str = ""
    history_context: str = ""
    bot_id: str = ""
    logical_group_id: str = ""
    persona_scope: str = ""
    persona_fingerprint: str = ""
    address_book_count: int = 0


@dataclass(frozen=True, slots=True)
class BotMeshDispatchResult:
    content: str
    bot_id: str
    logical_group_id: str
    persona_scope: str
    persona_fingerprint: str
    target_id: str = ""
    audience: str = "group"


class BotMeshBridge:
    def __init__(self, enabled: bool) -> None:
        self.enabled = bool(enabled)
        self._module: Any | None = None

    def set_module_for_testing(self, module: Any | None) -> None:
        self._module = module

    def installed_module(self) -> Any | None:
        if not self.enabled:
            return None
        if self._module is not None:
            return self._module
        loaded = self._loaded_integration_modules()
        active = next(
            (
                module
                for module in loaded
                if getattr(module, "_provider", None) is not None
            ),
            None,
        )
        if active is not None:
            return active
        try:
            imported = importlib.import_module("astrbot_plugin_botmesh.integration")
        except (ImportError, ModuleNotFoundError):
            return loaded[0] if loaded else None
        if getattr(imported, "_provider", None) is not None:
            return imported
        refreshed = self._loaded_integration_modules()
        return next(
            (
                module
                for module in refreshed
                if getattr(module, "_provider", None) is not None
            ),
            imported,
        )

    @staticmethod
    def _loaded_integration_modules() -> list[Any]:
        result: list[Any] = []
        for name, module in list(sys.modules.items()):
            if module is None:
                continue
            module_path = str(getattr(module, "__file__", "") or "")
            fingerprint = f"{name} {module_path}".replace("-", "_").casefold()
            if "astrbot_plugin_botmesh" not in fingerprint:
                continue
            if not any(
                callable(getattr(module, method_name, None))
                for method_name in (
                    "dispatch_proactive_topic",
                    "get_proactive_topics_context",
                    "wrap_proactive_topics_message",
                )
            ):
                continue
            result.append(module)
        return result

    async def resolve(
        self,
        *,
        umo: str,
        identity: BotIdentity,
        event: Any | None,
    ) -> BotMeshContext:
        module = self.installed_module()
        if module is None:
            return BotMeshContext(active=False)
        method = getattr(module, "get_proactive_topics_context", None)
        if not callable(method):
            raise BotMeshError("api_unavailable")

        legacy_verified = False
        try:
            result = await method(
                umo=umo,
                event=event,
                identity=identity.as_dict(),
            )
        except TypeError as exc:
            if "identity" not in str(exc):
                raise BotMeshError("integration_failure") from exc
            if event is None:
                raise BotMeshError("identity_api_unsupported") from exc
            result = await method(umo=umo, event=event)
            legacy_verified = True
        except Exception as exc:
            raise BotMeshError("integration_failure") from exc

        context = dict(result) if isinstance(result, dict) else {}
        if not context:
            raise BotMeshError("empty_context")
        if not context.get("enabled"):
            raise BotMeshError(str(context.get("error", "identity_unresolved")))
        try:
            contract_version = int(context.get("proactive_contract_version", 0))
        except (TypeError, ValueError):
            contract_version = 0
        if contract_version < 2:
            raise BotMeshError("proactive_contract_v2_required")
        if not legacy_verified:
            mismatch = self._identity_mismatch(identity, context)
            if mismatch:
                raise BotMeshError(f"identity_mismatch:{mismatch}")
        raw_address_book = context.get("address_book", [])
        address_book_count = (
            len([item for item in raw_address_book if isinstance(item, dict)])
            if isinstance(raw_address_book, list)
            else 0
        )
        return BotMeshContext(
            active=True,
            contract_version=contract_version,
            persona_prompt=str(context.get("persona_prompt", "") or "").strip(),
            policy_prompt=str(context.get("policy_prompt", "") or "").strip(),
            history_context=str(context.get("history_context", "") or "").strip(),
            bot_id=str(context.get("bot_id", "") or "").strip(),
            logical_group_id=str(
                context.get("logical_group_id", "") or ""
            ).strip(),
            persona_scope=str(context.get("persona_scope", "") or "").strip(),
            persona_fingerprint=str(
                context.get("persona_fingerprint", "") or ""
            ).strip(),
            address_book_count=address_book_count,
        )

    async def dispatch(
        self,
        *,
        umo: str,
        identity: BotIdentity,
        event: Any | None,
        trigger: dict[str, Any],
        local_history: list[dict[str, Any]],
        recent_topics: list[str],
        generation_options: dict[str, Any],
    ) -> BotMeshDispatchResult:
        trace_id = str(trigger.get("trace_id", "") or "no-trace")
        module = self.installed_module()
        if module is None:
            logger.warning(
                "[主动话题][%s] 调用链 2/4：BotMeshBridge 未找到集成模块",
                trace_id,
            )
            raise BotMeshError("provider_unavailable")
        method = getattr(module, "dispatch_proactive_topic", None)
        if not callable(method):
            logger.warning(
                "[主动话题][%s] 调用链 2/4：BotMeshBridge 找到模块但缺少 "
                "dispatch_proactive_topic；module=%s file=%s",
                trace_id,
                getattr(module, "__name__", type(module).__name__),
                getattr(module, "__file__", "<unknown>"),
            )
            raise BotMeshError("dispatch_api_unavailable")
        logger.info(
            "[主动话题][%s] 调用链 2/4：BotMeshBridge -> integration；"
            "module=%s file=%s",
            trace_id,
            getattr(module, "__name__", type(module).__name__),
            getattr(module, "__file__", "<unknown>"),
        )
        try:
            result = await method(
                umo=umo,
                event=event,
                identity=identity.as_dict(),
                trigger=trigger,
                local_history=local_history,
                recent_topics=recent_topics,
                generation_options=generation_options,
            )
        except Exception as exc:
            logger.exception(
                "[主动话题][%s] BotMesh 集成接口抛出异常：%s",
                trace_id,
                exc,
            )
            raise BotMeshError("dispatch_failure") from exc
        payload = dict(result) if isinstance(result, dict) else {}
        logger.info(
            "[主动话题][%s] BotMeshBridge 收到接口结果：success=%s "
            "version=%s bot_id=%s logical_group=%s audience=%s target_id=%s "
            "error=%s",
            trace_id,
            payload.get("success"),
            payload.get("proactive_dispatch_version"),
            payload.get("bot_id", ""),
            payload.get("logical_group_id", ""),
            payload.get("audience", ""),
            payload.get("target_id", "") or "<group>",
            payload.get("error", ""),
        )
        try:
            version = int(payload.get("proactive_dispatch_version", 0))
        except (TypeError, ValueError):
            version = 0
        if version < 1:
            raise BotMeshError("dispatch_contract_v1_required")
        if not payload.get("success"):
            raise BotMeshError(
                str(
                    payload.get("error", "dispatch_rejected")
                    or "dispatch_rejected"
                )
            )
        mismatch = self._identity_mismatch(identity, payload)
        if mismatch:
            raise BotMeshError(f"identity_mismatch:{mismatch}")
        content = str(payload.get("content", "") or "").strip()
        if not content:
            raise BotMeshError("empty_dispatch_content")
        return BotMeshDispatchResult(
            content=content,
            bot_id=str(payload.get("bot_id", "") or "").strip(),
            logical_group_id=str(
                payload.get("logical_group_id", "") or ""
            ).strip(),
            persona_scope=str(payload.get("persona_scope", "") or "").strip(),
            persona_fingerprint=str(
                payload.get("persona_fingerprint", "") or ""
            ).strip(),
            target_id=str(payload.get("target_id", "") or "").strip(),
            audience=str(payload.get("audience", "group") or "group").strip(),
        )

    def normalize_message(self, *, umo: str, content: str, event: Any | None) -> str:
        return self.normalize_record(umo=umo, content=content, event=event)["content"]

    def normalize_record(
        self,
        *,
        umo: str,
        content: str,
        event: Any | None,
    ) -> dict[str, str]:
        raw = str(content or "")
        fallback = {"content": raw}
        module = self.installed_module()
        if module is None:
            return fallback
        record_method = getattr(module, "normalize_chat_history_record", None)
        if callable(record_method):
            try:
                result = record_method(umo=umo, content=raw, event=event)
                if isinstance(result, dict):
                    normalized = {
                        key: str(value or "").strip()
                        for key, value in result.items()
                        if key in {"content", "sender_id", "sender_name", "source_bot_id"}
                    }
                    normalized.setdefault("content", raw)
                    return normalized
            except Exception:
                return fallback
        method = getattr(module, "normalize_chat_history_message", None)
        if not callable(method):
            return fallback
        try:
            fallback["content"] = str(
                method(umo=umo, content=raw, event=event) or raw
            )
            return fallback
        except Exception:
            return fallback

    @staticmethod
    def _identity_mismatch(
        expected: BotIdentity,
        context: dict[str, Any],
    ) -> str:
        comparisons = [
            ("platform_id", expected.platform_id, context.get("platform_id")),
            ("group_id", expected.group_id, context.get("raw_group_id")),
        ]
        # QQ Official can expose an adapter sentinel such as ``qq_official`` as
        # self_id.  A stable platform instance is the canonical route in that
        # case, so compare raw account IDs only when platform_id is unavailable.
        if not expected.platform_id:
            comparisons.append(
                ("self_id", expected.self_id, context.get("account_id"))
            )
        for label, expected_value, actual_value in comparisons:
            if not expected_value:
                continue
            actual = str(actual_value or "").strip()
            if actual != expected_value:
                return f"{label}={expected_value!r}/{actual!r}"
        return ""
