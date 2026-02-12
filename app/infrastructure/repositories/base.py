from __future__ import annotations

import re
from typing import Any, Iterable


class TenantScopeRequiredError(ValueError):
    """Raised when a repository is instantiated without tenant/workspace scope."""


class BaseRepository:
    def __init__(self, *, tenant_id: str | None = None, workspace_id: str | None = None) -> None:
        scope = str(tenant_id or workspace_id or "").strip()
        if not scope:
            raise TenantScopeRequiredError("tenant_id or workspace_id is required for repository access")
        self.tenant_id = scope
        self.workspace_id = str(workspace_id or tenant_id or "").strip() or scope

    def build_tenant_clause(
        self,
        *,
        table_alias: str | None = None,
        column_name: str = "tenant_id",
    ) -> str:
        prefix = f"{table_alias.strip()}." if table_alias and str(table_alias).strip() else ""
        return f"{prefix}{column_name} = ?"

    def enforce_tenant_scope(
        self,
        query: str,
        *,
        table_alias: str | None = None,
        column_name: str = "tenant_id",
    ) -> str:
        raw_query = str(query or "").strip()
        if not raw_query:
            return raw_query

        lowered = raw_query.lower()
        if "tenant_id" in lowered or "workspace_id" in lowered:
            return raw_query

        clause = self.build_tenant_clause(table_alias=table_alias, column_name=column_name)
        marker = re.search(r"\b(group\s+by|order\s+by|limit|offset|returning)\b", raw_query, flags=re.IGNORECASE)
        if marker:
            head = raw_query[: marker.start()].rstrip()
            tail = raw_query[marker.start() :]
        else:
            head = raw_query
            tail = ""

        if re.search(r"\bwhere\b", head, flags=re.IGNORECASE):
            scoped_head = f"{head} AND {clause}"
        else:
            scoped_head = f"{head} WHERE {clause}"
        return f"{scoped_head} {tail}".strip()

    def scoped_params(self, params: Iterable[Any] | None = None) -> tuple[Any, ...]:
        values = tuple(params or ())
        return (*values, self.tenant_id)

    @staticmethod
    def rows_to_dicts(rows: Iterable[Any]) -> list[dict]:
        return [dict(row) for row in rows]

