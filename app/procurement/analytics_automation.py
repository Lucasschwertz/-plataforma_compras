from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List
from urllib.parse import urlencode

from app.procurement.analytics_actions import kpi_primary_actions
from app.ui_strings import get_ui_text


SUPPLIER_RESPONSE_THRESHOLD = 70.0
ERP_AWAITING_HOURS_THRESHOLD = 24.0


SECTION_SLUGS: Dict[str, str] = {
    "overview": "visao-geral",
    "efficiency": "eficiencia-processo",
    "costs": "custos-economia",
    "suppliers": "fornecedores",
    "quality_erp": "qualidade-erp",
    "compliance": "compliance",
    "executive": "executivo",
}


def build_analytics_alerts(
    records: Iterable[Dict[str, Any]] | None,
    section_kpis: Iterable[Dict[str, Any]] | None,
    raw_filters: Dict[str, Any] | None,
) -> List[Dict[str, Any]]:
    current_records = list(records or [])
    kpi_index = _kpi_index(section_kpis)
    normalized_filters = _normalized_filters(raw_filters)

    late_count = _kpi_or_default(
        kpi_index,
        "late_processes",
        float(sum(1 for row in current_records if bool(row.get("is_delayed")))),
    )
    supplier_response_rate = _kpi_or_default(
        kpi_index,
        "supplier_response_rate",
        _supplier_response_rate(current_records),
    )
    awaiting_erp_count = _kpi_or_default(
        kpi_index,
        "awaiting_erp",
        float(sum(1 for row in current_records if str(row.get("erp_ui_status") or "") == "enviado")),
    )
    no_competition = _kpi_or_default(
        kpi_index,
        "no_competition",
        float(sum(1 for row in current_records if _no_competition_flag(row))),
    )
    emergency_without_competition = _kpi_or_default(
        kpi_index,
        "emergency_without_competition",
        float(
            sum(
                1
                for row in current_records
                if str(row.get("purchase_type") or "") == "emergencial" and _no_competition_flag(row)
            )
        ),
    )

    alerts: List[Dict[str, Any]] = []

    if late_count > 0:
        alerts.append(
            _build_alert(
                alert_type="late_processes",
                severity="high" if late_count >= 5 else "medium",
                title=get_ui_text("analytics.alert.late_processes.title", "Processos com atraso"),
                description=get_ui_text(
                    "analytics.alert.late_processes.description",
                    f"{int(late_count)} processos estao com prazo vencido e exigem priorizacao.",
                ),
                entity={
                    "kind": "purchase_request",
                    "label": get_ui_text("term.purchase_request", "Solicitacao de compra"),
                    "count": int(late_count),
                },
                suggested_action=_suggested_action("late_processes", "efficiency", normalized_filters),
            )
        )

    invites_total = sum(float(row.get("rfq_invite_count") or 0.0) for row in current_records)
    if invites_total > 0 and supplier_response_rate < SUPPLIER_RESPONSE_THRESHOLD:
        alerts.append(
            _build_alert(
                alert_type="supplier_response_low",
                severity="high" if supplier_response_rate < 50.0 else "medium",
                title=get_ui_text("analytics.alert.supplier_response_low.title", "Baixa resposta de fornecedores"),
                description=get_ui_text(
                    "analytics.alert.supplier_response_low.description",
                    (
                        f"Taxa de resposta em {supplier_response_rate:.1f}% (meta minima {SUPPLIER_RESPONSE_THRESHOLD:.0f}%). "
                        "Reforce convites e ajuste a base fornecedora."
                    ),
                ),
                entity={
                    "kind": "supplier",
                    "label": get_ui_text("term.supplier", "Fornecedor"),
                    "count": int(round(invites_total)),
                },
                suggested_action=_suggested_action("supplier_response_rate", "suppliers", normalized_filters),
            )
        )

    max_awaiting_hours, tracked_awaiting = _awaiting_erp_window_stats(current_records)
    if awaiting_erp_count > 0 and (max_awaiting_hours >= ERP_AWAITING_HOURS_THRESHOLD or tracked_awaiting == 0):
        if tracked_awaiting > 0:
            erp_description = (
                f"{int(awaiting_erp_count)} ordens estao aguardando retorno do ERP "
                f"ha ate {max_awaiting_hours:.1f} horas."
            )
        else:
            erp_description = (
                f"{int(awaiting_erp_count)} ordens estao aguardando retorno do ERP "
                "e exigem acompanhamento operacional imediato."
            )
        alerts.append(
            _build_alert(
                alert_type="erp_waiting",
                severity="high" if max_awaiting_hours >= 72.0 else "medium",
                title=get_ui_text("analytics.alert.erp_waiting.title", "Ordens aguardando retorno do ERP"),
                description=get_ui_text(
                    "analytics.alert.erp_waiting.description",
                    erp_description,
                ),
                entity={
                    "kind": "purchase_order",
                    "label": get_ui_text("term.purchase_order", "Ordem de compra"),
                    "count": int(awaiting_erp_count),
                },
                suggested_action=_suggested_action("awaiting_erp", "quality_erp", normalized_filters),
            )
        )

    compliance_count = float(no_competition + emergency_without_competition)
    if compliance_count > 0:
        alerts.append(
            _build_alert(
                alert_type="compliance_outlier",
                severity="high" if emergency_without_competition > 0 else "medium",
                title=get_ui_text("analytics.alert.compliance_outlier.title", "Compras fora do padrao"),
                description=get_ui_text(
                    "analytics.alert.compliance_outlier.description",
                    (
                        f"{int(compliance_count)} compras com baixa concorrencia, sendo "
                        f"{int(emergency_without_competition)} emergenciais."
                    ),
                ),
                entity={
                    "kind": "award",
                    "label": get_ui_text("term.decision", "Decisao"),
                    "count": int(compliance_count),
                },
                suggested_action=_suggested_action("no_competition", "compliance", normalized_filters),
            )
        )

    return alerts


def _build_alert(
    *,
    alert_type: str,
    severity: str,
    title: str,
    description: str,
    entity: Dict[str, Any],
    suggested_action: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "type": alert_type,
        "severity": severity,
        "title": title,
        "description": description,
        "entity": dict(entity),
        "suggested_action": dict(suggested_action),
    }


def _kpi_index(kpis: Iterable[Dict[str, Any]] | None) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for item in list(kpis or []):
        key = str((item or {}).get("key") or "").strip()
        if key:
            index[key] = dict(item or {})
    return index


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _kpi_or_default(kpi_index: Dict[str, Dict[str, Any]], key: str, fallback: float) -> float:
    item = kpi_index.get(key)
    if not item:
        return float(fallback)
    return _to_float(item.get("value"))


def _normalized_filters(raw_filters: Dict[str, Any] | None) -> Dict[str, Any]:
    payload = dict(raw_filters or {})
    normalized: Dict[str, Any] = {}
    for key, value in payload.items():
        text = str(value or "").strip()
        if not text:
            continue
        normalized[key] = text
    return normalized


def _section_url(section_key: str, raw_filters: Dict[str, Any]) -> str:
    slug = SECTION_SLUGS.get(section_key, SECTION_SLUGS["overview"])
    base_url = f"/procurement/analises/{slug}"
    if not raw_filters:
        return base_url
    return f"{base_url}?{urlencode(raw_filters)}"


def _suggested_action(kpi_key: str, section_key: str, raw_filters: Dict[str, Any]) -> Dict[str, Any]:
    actions = kpi_primary_actions(kpi_key)
    if actions:
        primary = actions[0]
    else:
        primary = {"key": "view_history", "label": get_ui_text("analytics.action.view_history", "Ver historico")}
    return {
        "action_key": str(primary.get("key") or ""),
        "label": str(primary.get("label") or get_ui_text("analytics.action.view_actions", "Ver acoes")),
        "url": _section_url(section_key, raw_filters),
        "action_type": "open_list",
    }


def _parse_datetime(raw_value: Any) -> datetime | None:
    if raw_value in (None, ""):
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    try:
        if value.endswith("Z"):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def _hours_since(raw_datetime: Any, now_utc: datetime) -> float:
    value = _parse_datetime(raw_datetime)
    if not value:
        return 0.0
    delta = now_utc - value
    return max(0.0, round(delta.total_seconds() / 3600.0, 2))


def _awaiting_erp_window_stats(records: Iterable[Dict[str, Any]]) -> tuple[float, int]:
    now_utc = datetime.now(timezone.utc)
    max_hours = 0.0
    tracked = 0
    for row in list(records):
        if str(row.get("erp_ui_status") or "") != "enviado":
            continue
        raw_ref = row.get("po_updated_at") or row.get("po_created_at")
        if _parse_datetime(raw_ref):
            tracked += 1
        elapsed = _hours_since(raw_ref, now_utc)
        if elapsed > max_hours:
            max_hours = elapsed
    return max_hours, tracked


def _supplier_response_rate(records: Iterable[Dict[str, Any]]) -> float:
    invites = sum(float(row.get("rfq_invite_count") or 0.0) for row in list(records))
    responses = sum(float(row.get("rfq_response_count") or 0.0) for row in list(records))
    if invites <= 0:
        return 0.0
    return round((responses / invites) * 100.0, 2)


def _no_competition_flag(record: Dict[str, Any]) -> bool:
    invite_count = int(record.get("rfq_invite_count") or 0)
    response_count = int(record.get("rfq_response_count") or 0)
    quote_count = int(record.get("rfq_quote_count") or 0)
    if not record.get("rfq_id"):
        return False
    if invite_count <= 1:
        return True
    if response_count <= 1:
        return True
    if quote_count <= 1:
        return True
    return False
