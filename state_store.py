from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from .domain import (
    STATE_VERSION,
    BotIdentity,
    TopicScope,
    TopicSettings,
    legacy_scope_id,
    new_scope_id,
)


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self, defaults: TopicSettings) -> tuple[dict[str, TopicScope], bool]:
        if not self.path.is_file():
            return {}, False
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("状态文件根节点必须是对象")

        raw_scopes = payload.get("scopes")
        if isinstance(raw_scopes, Mapping):
            scopes = {
                str(scope_id): TopicScope.from_mapping(
                    str(scope_id),
                    raw,
                    defaults,
                )
                for scope_id, raw in raw_scopes.items()
                if isinstance(raw, Mapping)
            }
            return scopes, int(payload.get("version", 0) or 0) != STATE_VERSION

        raw_groups = payload.get("groups", {})
        if not isinstance(raw_groups, Mapping):
            raise ValueError("旧状态文件中的 groups 必须是对象")
        scopes: dict[str, TopicScope] = {}
        for old_key, raw in raw_groups.items():
            if not isinstance(raw, Mapping):
                continue
            umo = str(raw.get("umo", old_key) or old_key)
            identity = BotIdentity.from_mapping(raw)
            scope_id = (
                new_scope_id(identity, umo)
                if identity.routable
                else legacy_scope_id(umo)
            )
            scope_id = self._unused_scope_id(scopes, scope_id)
            merged: dict[str, Any] = dict(raw)
            merged["umo"] = umo
            scope = TopicScope.from_mapping(scope_id, merged, defaults)
            scope.legacy_unclaimed = not scope.identity.routable
            scopes[scope_id] = scope
        return scopes, bool(raw_groups)

    def save(self, scopes: Mapping[str, TopicScope]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": STATE_VERSION,
            "scopes": {
                scope_id: scope.as_dict()
                for scope_id, scope in sorted(scopes.items())
            },
        }
        temp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, self.path)

    @staticmethod
    def _unused_scope_id(scopes: Mapping[str, TopicScope], requested: str) -> str:
        if requested not in scopes:
            return requested
        suffix = 2
        while f"{requested}-{suffix}" in scopes:
            suffix += 1
        return f"{requested}-{suffix}"
