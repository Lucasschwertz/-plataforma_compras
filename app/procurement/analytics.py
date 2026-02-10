from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from statistics import mean
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from app.procurement.analytics_actions import (
    build_record_primary_action,
    build_supplier_primary_action,
    enrich_kpi_actions,
)
from app.tenant import DEFAULT_TENANT_ID
from app.ui_strings import STATUS_LABELS, get_ui_text


ANALYTICS_SECTIONS: List[Dict[str, str]] = [
    {
        "key": "overview",
        "slug": "visao-geral",
        "label": "Visao Geral",
        "description": "Resumo operacional do ciclo de compras.",
    },
    {
        "key": "efficiency",
        "slug": "eficiencia-processo",
        "label": "Eficiencia do Processo",
        "description": "Velocidade de execucao e gargalos de etapa.",
    },
    {
        "key": "costs",
        "slug": "custos-economia",
        "label": "Custos e Economia",
        "description": "Efeito financeiro da negociacao de compras.",
    },
    {
        "key": "suppliers",
        "slug": "fornecedores",
        "label": "Fornecedores",
        "description": "Engajamento e desempenho da base fornecedora.",
    },
    {
        "key": "quality_erp",
        "slug": "qualidade-erp",
        "label": "Qualidade e ERP",
        "description": "Saude da integracao e qualidade do retorno ERP.",
    },
    {
        "key": "compliance",
        "slug": "compliance",
        "label": "Compliance",
        "description": "Controles de concorrencia, excecao e trilha critica.",
    },
]


_SECTION_BY_KEY = {item["key"]: item for item in ANALYTICS_SECTIONS}
_SECTION_BY_SLUG = {item["slug"]: item for item in ANALYTICS_SECTIONS}
_SECTION_ALIASES = {
    "visao_geral": "overview",
    "overview": "overview",
    "eficiencia": "efficiency",
    "efficiency": "efficiency",
    "custos": "costs",
    "costs": "costs",
    "fornecedor": "suppliers",
    "suppliers": "suppliers",
    "qualidade_erp": "quality_erp",
    "quality_erp": "quality_erp",
}


PURCHASE_TYPE_OPTIONS: List[Dict[str, str]] = [
    {"key": "regular", "label": "Compra regular"},
    {"key": "emergencial", "label": "Compra emergencial"},
]


ERP_REJECTION_HINTS = ("rejeit", "recus", "reject", "invalid", "inval", "422")
EXCEPTION_REASON_HINTS = ("exce", "sem concorr", "emergenc", "single source", "justific")
CRITICAL_EVENT_REASONS = {
    "purchase_request_cancelled",
    "rfq_cancelled",
    "purchase_order_cancelled",
    "rfq_awarded",
    "award_converted_to_po",
    "po_push_started",
    "po_push_retry_started",
    "po_push_failed",
    "po_push_rejected",
}
OPEN_REQUEST_STATUSES = {"pending_rfq", "in_rfq", "awarded", "ordered", "partially_received"}


def analytics_sections() -> List[Dict[str, str]]:
    return [dict(item) for item in ANALYTICS_SECTIONS]


def normalize_section_key(raw_value: str | None) -> str:
    normalized = str(raw_value or "").strip().lower()
    if not normalized:
        return "overview"
    if normalized in _SECTION_BY_KEY:
        return normalized
    if normalized in _SECTION_ALIASES:
        return _SECTION_ALIASES[normalized]
    if normalized in _SECTION_BY_SLUG:
        return _SECTION_BY_SLUG[normalized]["key"]
    return "overview"


def section_meta(section_key: str) -> Dict[str, str]:
    section = _SECTION_BY_KEY.get(section_key) or _SECTION_BY_KEY["overview"]
    return dict(section)


def parse_analytics_filters(args: Any, workspace_id: str | None) -> Dict[str, Any]:
    start_date = _parse_date(args.get("start_date"))
    end_date = _parse_date(args.get("end_date"))
    if start_date and end_date and start_date > end_date:
        start_date, end_date = end_date, start_date

    statuses = [value for value in _parse_csv_values(args.get("status")) if value in STATUS_LABELS]
    purchase_types = [
        value
        for value in _parse_csv_values(args.get("purchase_type"))
        if value in {"regular", "emergencial"}
    ]
    period_basis = str(args.get("period_basis") or "pr_created_at").strip().lower()
    if period_basis not in {"pr_created_at", "po_updated_at"}:
        period_basis = "pr_created_at"

    supplier = str(args.get("supplier") or "").strip()
    buyer = str(args.get("buyer") or "").strip()
    workspace_filter = str(args.get("workspace_id") or workspace_id or DEFAULT_TENANT_ID).strip() or DEFAULT_TENANT_ID
    status_values = sorted(dict.fromkeys(statuses))
    purchase_type_values = sorted(dict.fromkeys(purchase_types))

    return {
        "start_date": start_date,
        "end_date": end_date,
        "supplier": supplier,
        "buyer": buyer,
        "status": status_values,
        "purchase_type": purchase_type_values,
        "period_basis": period_basis,
        "workspace_id": workspace_filter,
        "raw": {
            "start_date": start_date.isoformat() if start_date else "",
            "end_date": end_date.isoformat() if end_date else "",
            "supplier": supplier,
            "buyer": buyer,
            "status": ",".join(status_values),
            "purchase_type": ",".join(purchase_type_values),
            "period_basis": period_basis,
            "workspace_id": workspace_filter,
        },
    }


def resolve_visibility(
    role: str | None,
    user_email: str | None,
    display_name: str | None,
    team_members: Any = None,
) -> Dict[str, Any]:
    normalized_role = str(role or "buyer").strip().lower()
    if normalized_role not in {"buyer", "manager", "admin", "approver"}:
        normalized_role = "buyer"

    aliases = _normalize_actors([user_email, display_name])
    if normalized_role == "admin":
        return {
            "role": normalized_role,
            "scope": "workspace",
            "restrict_by_actor": False,
            "actors": [],
        }

    if normalized_role == "manager":
        team_values = _normalize_actors(_coerce_to_list(team_members))
        if team_values:
            for alias in aliases:
                team_values.add(alias)
            return {
                "role": normalized_role,
                "scope": "team",
                "restrict_by_actor": True,
                "actors": sorted(team_values),
            }
        return {
            "role": normalized_role,
            "scope": "workspace_team",
            "restrict_by_actor": False,
            "actors": [],
        }

    # buyer and approver default to own portfolio.
    return {
        "role": normalized_role,
        "scope": "self",
        "restrict_by_actor": True,
        "actors": sorted(aliases),
    }


def build_filter_options(
    db,
    tenant_id: str | None,
    visibility: Dict[str, Any],
    selected_filters: Dict[str, Any],
) -> Dict[str, Any]:
    dataset = _load_dataset(db, tenant_id, visibility)
    records = dataset["records"]

    suppliers = sorted({str(item.get("supplier_name") or "").strip() for item in records if str(item.get("supplier_name") or "").strip()})
    buyers = sorted({str(item.get("requested_by") or "").strip() for item in records if str(item.get("requested_by") or "").strip()})

    statuses = sorted(
        {
            str(value).strip()
            for row in records
            for value in (row.get("pr_status"), row.get("rfq_status"), row.get("po_status"))
            if str(value or "").strip()
        }
    )
    status_options = [{"key": key, "label": STATUS_LABELS.get(key, key)} for key in statuses]

    return {
        "suppliers": [{"key": value, "label": value} for value in suppliers],
        "buyers": [{"key": value, "label": value} for value in buyers],
        "statuses": status_options,
        "purchase_types": [dict(item) for item in PURCHASE_TYPE_OPTIONS],
        "workspace": {
            "key": tenant_id or DEFAULT_TENANT_ID,
            "label": tenant_id or DEFAULT_TENANT_ID,
        },
        "selected": selected_filters.get("raw", {}),
        "sections": analytics_sections(),
    }


def build_analytics_payload(
    db,
    tenant_id: str | None,
    section_key: str,
    filters: Dict[str, Any],
    visibility: Dict[str, Any],
) -> Dict[str, Any]:
    resolved_section = normalize_section_key(section_key)
    section_info = section_meta(resolved_section)
    dataset = _load_dataset(db, tenant_id, visibility)
    period_date_key = "pr_created_at"
    if resolved_section == "quality_erp" and str(filters.get("period_basis") or "") == "po_updated_at":
        period_date_key = "po_updated_at"

    filtered_no_period = _apply_filters(dataset["records"], filters, include_period=False)
    current_records = _apply_period(
        filtered_no_period,
        filters.get("start_date"),
        filters.get("end_date"),
        date_field=period_date_key,
    )
    previous_start, previous_end = _previous_period_window(filters.get("start_date"), filters.get("end_date"))
    comparison_records = _apply_period(
        filtered_no_period,
        previous_start,
        previous_end,
        date_field=period_date_key,
    )

    section_builder = _SECTION_BUILDERS.get(resolved_section, _build_overview_section)
    section_payload = section_builder(current_records, comparison_records, dataset, filters)
    section_payload["kpis"] = enrich_kpi_actions(
        section_payload.get("kpis", []),
        resolved_section,
        filters.get("raw", {}),
    )

    return {
        "section": section_info,
        "filters": filters.get("raw", {}),
        "visibility": {
            "role": visibility.get("role"),
            "scope": visibility.get("scope"),
        },
        "meta": {
            "records_count": len(current_records),
            "comparison_records_count": len(comparison_records),
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        **section_payload,
    }


def _action_column_label() -> str:
    return get_ui_text("analytics.drilldown.next_action", "Proxima acao")


def _no_action_label() -> str:
    return get_ui_text("analytics.drilldown.no_action", "Sem acao disponivel")


def _enrich_record_drilldown_row(base_row: Dict[str, Any], record: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(base_row)
    fallback_link = str(row.get("link") or _record_link(record) or "").strip() or None
    action = build_record_primary_action(record, fallback_url=fallback_link)
    row["acao"] = (action or {}).get("label") or _no_action_label()
    row["_action"] = action
    return row


def _enrich_supplier_drilldown_row(base_row: Dict[str, Any], supplier_name: str | None) -> Dict[str, Any]:
    row = dict(base_row)
    action = build_supplier_primary_action(supplier_name)
    if action and not row.get("link"):
        row["link"] = action.get("url")
    row["acao"] = (action or {}).get("label") or _no_action_label()
    row["_action"] = action
    return row


def _build_overview_section(
    records: List[dict],
    comparison: List[dict],
    _dataset: Dict[str, Any],
    _filters: Dict[str, Any],
) -> Dict[str, Any]:
    backlog_open = sum(1 for row in records if str(row.get("pr_status") or "") in OPEN_REQUEST_STATUSES)
    active_quotes = sum(1 for row in records if str(row.get("rfq_status") or "") in {"open", "collecting_quotes"})
    orders_in_progress = sum(
        1
        for row in records
        if str(row.get("po_status") or "") in {"draft", "approved", "sent_to_erp", "erp_error"}
    )
    erp_attention = sum(
        1
        for row in records
        if str(row.get("erp_ui_status") or "") in {"enviado", "rejeitado", "reenvio_necessario"}
    )

    backlog_prev = sum(1 for row in comparison if str(row.get("pr_status") or "") in OPEN_REQUEST_STATUSES)
    active_quotes_prev = sum(
        1 for row in comparison if str(row.get("rfq_status") or "") in {"open", "collecting_quotes"}
    )
    orders_prev = sum(
        1
        for row in comparison
        if str(row.get("po_status") or "") in {"draft", "approved", "sent_to_erp", "erp_error"}
    )
    erp_attention_prev = sum(
        1
        for row in comparison
        if str(row.get("erp_ui_status") or "") in {"enviado", "rejeitado", "reenvio_necessario"}
    )

    kpis = [
        _kpi(
            "backlog_open",
            "Backlog aberto",
            backlog_open,
            _fmt_int(backlog_open),
            "Solicitacoes que ainda nao encerraram o fluxo.",
            _trend(backlog_open, backlog_prev, lower_is_better=True),
        ),
        _kpi(
            "active_quotes",
            "Cotacoes ativas",
            active_quotes,
            _fmt_int(active_quotes),
            "Cotacoes em andamento aguardando propostas ou decisao.",
            _trend(active_quotes, active_quotes_prev, lower_is_better=True),
        ),
        _kpi(
            "orders_in_progress",
            "Ordens em andamento",
            orders_in_progress,
            _fmt_int(orders_in_progress),
            "Ordens ainda nao encerradas com recebimento final.",
            _trend(orders_in_progress, orders_prev, lower_is_better=True),
        ),
        _kpi(
            "erp_attention",
            "Pendencias ERP",
            erp_attention,
            _fmt_int(erp_attention),
            "Ordens com envio pendente, rejeicao ou reenvio necessario.",
            _trend(erp_attention, erp_attention_prev, lower_is_better=True),
        ),
    ]

    charts = [
        {
            "key": "volume_line",
            "type": "line",
            "title": "Volume de solicitacoes por dia",
            "items": _series_count_by_date(records, "pr_created_at"),
        },
        {
            "key": "funnel_bar",
            "type": "bar",
            "title": "Funil operacional do periodo",
            "items": _chart_items_from_mapping(
                {
                    "Backlog aberto": backlog_open,
                    "Cotacoes ativas": active_quotes,
                    "Ordens em andamento": orders_in_progress,
                    "Pendencias ERP": erp_attention,
                }
            ),
        },
    ]

    drilldown_rows = []
    for row in sorted(records, key=lambda item: item.get("pr_created_at") or "", reverse=True)[:20]:
        drilldown_rows.append(
            _enrich_record_drilldown_row(
                {
                    "solicitacao": row.get("pr_number") or f"SR-{row.get('pr_id')}",
                    "comprador": row.get("requested_by") or "Nao informado",
                    "fornecedor": row.get("supplier_name") or "Nao informado",
                    "status": STATUS_LABELS.get(str(row.get("pr_status") or ""), row.get("pr_status") or "Nao informado"),
                    "ordem": row.get("po_number") or "-",
                    "link": _record_link(row),
                },
                row,
            )
        )

    return {
        "kpis": kpis,
        "charts": charts,
        "drilldown": {
            "title": "Itens recentes do fluxo",
            "columns": ["Solicitacao", "Comprador", "Fornecedor", "Status", "Ordem", _action_column_label()],
            "column_keys": ["solicitacao", "comprador", "fornecedor", "status", "ordem", "acao"],
            "rows": drilldown_rows,
        },
    }


def _build_efficiency_section(
    records: List[dict],
    comparison: List[dict],
    _dataset: Dict[str, Any],
    _filters: Dict[str, Any],
) -> Dict[str, Any]:
    sr_to_oc_values = [float(row["sr_to_oc_hours"]) for row in records if row.get("sr_to_oc_hours") is not None]
    sr_to_oc_prev = [float(row["sr_to_oc_hours"]) for row in comparison if row.get("sr_to_oc_hours") is not None]

    stage_a = [float(row["stage_sr_to_rfq_hours"]) for row in records if row.get("stage_sr_to_rfq_hours") is not None]
    stage_b = [float(row["stage_rfq_to_award_hours"]) for row in records if row.get("stage_rfq_to_award_hours") is not None]
    stage_c = [float(row["stage_award_to_po_hours"]) for row in records if row.get("stage_award_to_po_hours") is not None]
    stage_d = [float(row["stage_po_to_erp_hours"]) for row in records if row.get("stage_po_to_erp_hours") is not None]
    stage_e = [float(row["stage_erp_resolution_hours"]) for row in records if row.get("stage_erp_resolution_hours") is not None]

    stage_values = [*stage_a, *stage_b, *stage_c, *stage_d]
    avg_stage_hours = _avg(stage_values)
    avg_stage_prev = _avg(
        [
            float(row[key])
            for row in comparison
            for key in (
                "stage_sr_to_rfq_hours",
                "stage_rfq_to_award_hours",
                "stage_award_to_po_hours",
                "stage_po_to_erp_hours",
            )
            if row.get(key) is not None
        ]
    )

    late_count = sum(1 for row in records if row.get("is_delayed"))
    late_prev = sum(1 for row in comparison if row.get("is_delayed"))
    backlog_open = sum(1 for row in records if str(row.get("pr_status") or "") in OPEN_REQUEST_STATUSES)
    backlog_prev = sum(1 for row in comparison if str(row.get("pr_status") or "") in OPEN_REQUEST_STATUSES)

    kpis = [
        _kpi(
            "avg_sr_to_oc",
            "Tempo medio SR para OC",
            _avg(sr_to_oc_values),
            _fmt_duration(_avg(sr_to_oc_values)),
            "Tempo medio entre criacao da solicitacao e criacao da ordem.",
            _trend(_avg(sr_to_oc_values), _avg(sr_to_oc_prev), lower_is_better=True),
        ),
        _kpi(
            "avg_stage_time",
            "Tempo medio por etapa",
            avg_stage_hours,
            _fmt_duration(avg_stage_hours),
            (
                "Media das duracoes entre SR->Cotacao, Cotacao->Decisao, Decisao->OC e OC->ERP. "
                "Quanto menor o valor, mais fluido o processo."
            ),
            _trend(avg_stage_hours, avg_stage_prev, lower_is_better=True),
        ),
        _kpi(
            "late_processes",
            "Processos em atraso",
            late_count,
            _fmt_int(late_count),
            "Processos com data de necessidade vencida e sem encerramento.",
            _trend(late_count, late_prev, lower_is_better=True),
        ),
        _kpi(
            "backlog_open",
            "Backlog aberto",
            backlog_open,
            _fmt_int(backlog_open),
            "Itens ainda em aberto no pipeline de compras.",
            _trend(backlog_open, backlog_prev, lower_is_better=True),
        ),
    ]

    charts = [
        {
            "key": "stage_breakdown_bar",
            "type": "bar",
            "title": "Breakdown por etapa do processo",
            "items": _chart_items_from_mapping(
                {
                    "SR": _avg(stage_a),
                    "Cotacao": _avg(stage_b),
                    "Decisao": _avg(stage_c),
                    "OC": _avg(stage_d),
                    "ERP": _avg(stage_e),
                },
                value_formatter=_fmt_duration,
            ),
        },
        {
            "key": "stage_time_bar",
            "type": "bar",
            "title": "Tempo medio por etapa",
            "items": _chart_items_from_mapping(
                {
                    "SR para Cotacao": _avg(stage_a),
                    "Cotacao para Decisao": _avg(stage_b),
                    "Decisao para OC": _avg(stage_c),
                    "OC para ERP": _avg(stage_d),
                },
                value_formatter=_fmt_duration,
            ),
        },
        {
            "key": "late_trend_line",
            "type": "line",
            "title": "Ocorrencia de atrasos por dia",
            "items": _series_count_by_date(
                [row for row in records if row.get("is_delayed")],
                "pr_created_at",
            ),
        },
    ]

    drilldown = []
    for row in sorted(
        [item for item in records if item.get("is_delayed")],
        key=lambda item: item.get("needed_at") or "",
    )[:20]:
        drilldown.append(
            _enrich_record_drilldown_row(
                {
                    "solicitacao": row.get("pr_number") or f"SR-{row.get('pr_id')}",
                    "comprador": row.get("requested_by") or "Nao informado",
                    "necessidade": row.get("needed_at") or "Nao informado",
                    "tempo_sr_oc": _fmt_duration(row.get("sr_to_oc_hours")),
                    "link": _record_link(row),
                },
                row,
            )
        )

    return {
        "kpis": kpis,
        "charts": charts,
        "drilldown": {
            "title": "Processos em atraso",
            "columns": ["Solicitacao", "Comprador", "Necessidade", "Tempo SR para OC", _action_column_label()],
            "column_keys": ["solicitacao", "comprador", "necessidade", "tempo_sr_oc", "acao"],
            "rows": drilldown,
        },
    }


def _build_costs_section(
    records: List[dict],
    comparison: List[dict],
    _dataset: Dict[str, Any],
    _filters: Dict[str, Any],
) -> Dict[str, Any]:
    economy_abs = sum(float(row.get("rfq_savings_abs") or 0.0) for row in records)
    economy_prev = sum(float(row.get("rfq_savings_abs") or 0.0) for row in comparison)

    baseline_current = sum(float(row.get("rfq_quote_avg_total") or 0.0) for row in records)
    baseline_previous = sum(float(row.get("rfq_quote_avg_total") or 0.0) for row in comparison)

    economy_pct = (economy_abs / baseline_current * 100.0) if baseline_current > 0 else 0.0
    economy_pct_prev = (economy_prev / baseline_previous * 100.0) if baseline_previous > 0 else 0.0

    winner_ratios = [float(row["winner_vs_avg_ratio"]) for row in records if row.get("winner_vs_avg_ratio") is not None]
    winner_ratios_prev = [
        float(row["winner_vs_avg_ratio"]) for row in comparison if row.get("winner_vs_avg_ratio") is not None
    ]
    winner_vs_avg = (_avg(winner_ratios) * 100.0) if winner_ratios else 0.0
    winner_vs_avg_prev = (_avg(winner_ratios_prev) * 100.0) if winner_ratios_prev else 0.0

    emergency_count = sum(1 for row in records if row.get("purchase_type") == "emergencial")
    emergency_prev = sum(1 for row in comparison if row.get("purchase_type") == "emergencial")

    kpis = [
        _kpi(
            "economy_abs",
            "Economia absoluta",
            economy_abs,
            _fmt_currency(economy_abs),
            "Diferenca acumulada entre media de propostas e valor vencedor.",
            _trend(economy_abs, economy_prev),
        ),
        _kpi(
            "economy_pct",
            "Economia percentual",
            economy_pct,
            _fmt_percent(economy_pct),
            "Economia em relacao ao baseline medio das propostas.",
            _trend(economy_pct, economy_pct_prev),
        ),
        _kpi(
            "winner_vs_avg",
            "Preco vencedor vs media",
            winner_vs_avg,
            _fmt_percent(winner_vs_avg),
            (
                "Razao media (preco vencedor / media das propostas) x 100. "
                "Abaixo de 100% indica fechamento abaixo da media; quanto menor, melhor."
            ),
            _trend(winner_vs_avg, winner_vs_avg_prev, lower_is_better=True),
        ),
        _kpi(
            "emergency_count",
            "Compras emergenciais",
            emergency_count,
            _fmt_int(emergency_count),
            "Quantidade de solicitacoes classificadas como emergenciais.",
            _trend(emergency_count, emergency_prev, lower_is_better=True),
        ),
    ]

    savings_by_day: Dict[str, float] = {}
    for row in records:
        day = _day_label(row.get("pr_created_at"))
        if not day:
            continue
        savings_by_day[day] = savings_by_day.get(day, 0.0) + float(row.get("rfq_savings_abs") or 0.0)

    supplier_ranking: Dict[str, float] = {}
    for row in records:
        supplier_name = str(row.get("winner_supplier_name") or row.get("supplier_name") or "").strip()
        if not supplier_name:
            continue
        supplier_ranking[supplier_name] = supplier_ranking.get(supplier_name, 0.0) + float(row.get("rfq_savings_abs") or 0.0)

    charts = [
        {
            "key": "economy_line",
            "type": "line",
            "title": "Economia por dia",
            "items": _chart_items_from_mapping(dict(sorted(savings_by_day.items())), value_formatter=_fmt_currency),
        },
        {
            "key": "supplier_economy_ranking",
            "type": "ranking",
            "title": "Ranking por economia",
            "items": _chart_items_from_mapping(
                dict(sorted(supplier_ranking.items(), key=lambda item: item[1], reverse=True)[:8]),
                value_formatter=_fmt_currency,
            ),
        },
    ]

    drilldown = []
    for row in sorted(records, key=lambda item: float(item.get("rfq_savings_abs") or 0.0), reverse=True)[:20]:
        drilldown.append(
            _enrich_record_drilldown_row(
                {
                    "solicitacao": row.get("pr_number") or f"SR-{row.get('pr_id')}",
                    "fornecedor": row.get("winner_supplier_name") or row.get("supplier_name") or "Nao informado",
                    "economia": _fmt_currency(float(row.get("rfq_savings_abs") or 0.0)),
                    "tipo": "Emergencial" if row.get("purchase_type") == "emergencial" else "Regular",
                    "link": _record_link(row),
                },
                row,
            )
        )

    return {
        "kpis": kpis,
        "charts": charts,
        "drilldown": {
            "title": "Itens com impacto financeiro",
            "columns": ["Solicitacao", "Fornecedor", "Economia", "Tipo de compra", _action_column_label()],
            "column_keys": ["solicitacao", "fornecedor", "economia", "tipo", "acao"],
            "rows": drilldown,
        },
    }


def _build_suppliers_section(
    records: List[dict],
    comparison: List[dict],
    _dataset: Dict[str, Any],
    _filters: Dict[str, Any],
) -> Dict[str, Any]:
    invite_total = sum(int(row.get("rfq_invite_count") or 0) for row in records)
    invite_total_prev = sum(int(row.get("rfq_invite_count") or 0) for row in comparison)
    response_total = sum(int(row.get("rfq_response_count") or 0) for row in records)
    response_total_prev = sum(int(row.get("rfq_response_count") or 0) for row in comparison)

    response_rate = (response_total / invite_total * 100.0) if invite_total > 0 else 0.0
    response_rate_prev = (response_total_prev / invite_total_prev * 100.0) if invite_total_prev > 0 else 0.0

    response_times = [float(row["rfq_avg_response_hours"]) for row in records if row.get("rfq_avg_response_hours") is not None]
    response_times_prev = [
        float(row["rfq_avg_response_hours"]) for row in comparison if row.get("rfq_avg_response_hours") is not None
    ]

    supplier_economy: Dict[str, float] = {}
    supplier_delay: Dict[str, float] = {}
    supplier_response: Dict[str, Dict[str, float]] = {}
    for row in records:
        supplier = str(row.get("winner_supplier_name") or row.get("supplier_name") or "").strip()
        if not supplier:
            continue
        supplier_economy[supplier] = supplier_economy.get(supplier, 0.0) + float(row.get("rfq_savings_abs") or 0.0)
        supplier_delay[supplier] = supplier_delay.get(supplier, 0.0) + float(row.get("supplier_delay_score") or 0.0)
        bucket = supplier_response.setdefault(supplier, {"invites": 0.0, "responses": 0.0})
        bucket["invites"] += float(row.get("rfq_invite_count") or 0.0)
        bucket["responses"] += float(row.get("rfq_response_count") or 0.0)

    top_economy_label = next(iter(sorted(supplier_economy, key=supplier_economy.get, reverse=True)), "Nenhum")
    top_delay_label = next(iter(sorted(supplier_delay, key=supplier_delay.get, reverse=True)), "Nenhum")

    kpis = [
        _kpi(
            "supplier_response_rate",
            "Taxa de resposta",
            response_rate,
            _fmt_percent(response_rate),
            "Percentual de convites com proposta enviada.",
            _trend(response_rate, response_rate_prev),
        ),
        _kpi(
            "supplier_avg_response_time",
            "Tempo medio de resposta",
            _avg(response_times),
            _fmt_duration(_avg(response_times)),
            "Tempo medio entre abertura do convite e submissao de proposta.",
            _trend(_avg(response_times), _avg(response_times_prev), lower_is_better=True),
        ),
        _kpi(
            "supplier_top_economy",
            "Ranking por economia",
            supplier_economy.get(top_economy_label, 0.0),
            top_economy_label,
            "Fornecedor com maior economia acumulada no periodo.",
            None,
        ),
        _kpi(
            "supplier_top_delay",
            "Ranking por atraso",
            supplier_delay.get(top_delay_label, 0.0),
            top_delay_label,
            "Fornecedor com maior ocorrencia de atraso e risco operacional.",
            None,
        ),
    ]

    charts = [
        {
            "key": "supplier_economy",
            "type": "ranking",
            "title": "Fornecedores por economia",
            "items": _chart_items_from_mapping(
                dict(sorted(supplier_economy.items(), key=lambda item: item[1], reverse=True)[:8]),
                value_formatter=_fmt_currency,
            ),
        },
        {
            "key": "supplier_delay",
            "type": "ranking",
            "title": "Fornecedores por atraso",
            "items": _chart_items_from_mapping(
                dict(sorted(supplier_delay.items(), key=lambda item: item[1], reverse=True)[:8]),
                value_formatter=_fmt_number,
            ),
        },
    ]

    drilldown_rows = []
    for supplier_name, counters in sorted(
        supplier_response.items(),
        key=lambda item: (item[1].get("responses", 0.0) / item[1].get("invites", 1.0)),
        reverse=True,
    )[:20]:
        invites = counters.get("invites", 0.0)
        responses = counters.get("responses", 0.0)
        rate = (responses / invites * 100.0) if invites > 0 else 0.0
        drilldown_rows.append(
            _enrich_supplier_drilldown_row(
                {
                    "fornecedor": supplier_name,
                    "resposta": _fmt_percent(rate),
                    "economia": _fmt_currency(supplier_economy.get(supplier_name, 0.0)),
                    "atraso": _fmt_number(supplier_delay.get(supplier_name, 0.0)),
                },
                supplier_name,
            )
        )

    return {
        "kpis": kpis,
        "charts": charts,
        "drilldown": {
            "title": "Desempenho da base fornecedora",
            "columns": ["Fornecedor", "Taxa de resposta", "Economia", "Indice de atraso", _action_column_label()],
            "column_keys": ["fornecedor", "resposta", "economia", "atraso", "acao"],
            "rows": drilldown_rows,
        },
    }


def _build_quality_erp_section(
    records: List[dict],
    comparison: List[dict],
    _dataset: Dict[str, Any],
    _filters: Dict[str, Any],
) -> Dict[str, Any]:
    rejections = sum(1 for row in records if row.get("erp_rejected"))
    rejections_prev = sum(1 for row in comparison if row.get("erp_rejected"))
    retries = sum(int(row.get("po_retry_count") or 0) for row in records)
    retries_prev = sum(int(row.get("po_retry_count") or 0) for row in comparison)
    integration_errors = sum(int(row.get("po_integration_error_count") or 0) for row in records)
    integration_errors_prev = sum(int(row.get("po_integration_error_count") or 0) for row in comparison)
    awaiting_erp = sum(1 for row in records if str(row.get("erp_ui_status") or "") == "enviado")
    awaiting_erp_prev = sum(1 for row in comparison if str(row.get("erp_ui_status") or "") == "enviado")

    kpis = [
        _kpi(
            "erp_rejections",
            "Rejeicoes ERP",
            rejections,
            _fmt_int(rejections),
            "Ordens rejeitadas pelo ERP no periodo analisado.",
            _trend(rejections, rejections_prev, lower_is_better=True),
        ),
        _kpi(
            "erp_retries",
            "Reenvios",
            retries,
            _fmt_int(retries),
            "Tentativas adicionais de envio para o ERP.",
            _trend(retries, retries_prev, lower_is_better=True),
        ),
        _kpi(
            "integration_errors",
            "Erros de integracao",
            integration_errors,
            _fmt_int(integration_errors),
            "Falhas de envio ou retorno de integracao ERP.",
            _trend(integration_errors, integration_errors_prev, lower_is_better=True),
        ),
        _kpi(
            "awaiting_erp",
            "Aguardando ERP",
            awaiting_erp,
            _fmt_int(awaiting_erp),
            "Ordens enviadas e ainda sem resposta final do ERP.",
            _trend(awaiting_erp, awaiting_erp_prev, lower_is_better=True),
        ),
    ]

    erp_status_distribution: Dict[str, float] = {}
    for row in records:
        key = str(row.get("erp_ui_status") or "nao_enviado")
        label = {
            "nao_enviado": "Nao enviado",
            "enviado": "Enviado",
            "aceito": "Aceito",
            "rejeitado": "Rejeitado",
            "reenvio_necessario": "Reenvio necessario",
        }.get(key, key)
        erp_status_distribution[label] = erp_status_distribution.get(label, 0.0) + 1.0

    issue_line: Dict[str, float] = {}
    for row in records:
        if not row.get("po_integration_error_count") and not row.get("po_retry_count"):
            continue
        label = _day_label(row.get("po_updated_at") or row.get("pr_updated_at"))
        if not label:
            continue
        issue_line[label] = issue_line.get(label, 0.0) + float(row.get("po_integration_error_count") or 0.0) + float(
            row.get("po_retry_count") or 0.0
        )

    charts = [
        {
            "key": "erp_status_bar",
            "type": "bar",
            "title": "Distribuicao de status ERP",
            "items": _chart_items_from_mapping(erp_status_distribution, value_formatter=_fmt_number),
        },
        {
            "key": "integration_issue_line",
            "type": "line",
            "title": "Ocorrencias de erro e reenvio",
            "items": _chart_items_from_mapping(dict(sorted(issue_line.items())), value_formatter=_fmt_number),
        },
    ]

    drilldown_rows = []
    for row in sorted(
        [item for item in records if item.get("erp_rejected") or item.get("po_retry_count") or item.get("po_integration_error_count")],
        key=lambda item: item.get("po_updated_at") or item.get("pr_updated_at") or "",
        reverse=True,
    )[:20]:
        drilldown_rows.append(
            _enrich_record_drilldown_row(
                {
                    "ordem": row.get("po_number") or "-",
                    "fornecedor": row.get("supplier_name") or "Nao informado",
                    "status_erp": {
                        "nao_enviado": "Nao enviado",
                        "enviado": "Enviado",
                        "aceito": "Aceito",
                        "rejeitado": "Rejeitado",
                        "reenvio_necessario": "Reenvio necessario",
                    }.get(str(row.get("erp_ui_status") or ""), "Nao informado"),
                    "ultima_atualizacao": row.get("po_updated_at") or row.get("pr_updated_at") or "Nao informado",
                    "link": _record_link(row),
                },
                row,
            )
        )

    return {
        "kpis": kpis,
        "charts": charts,
        "drilldown": {
            "title": "Ocorrencias ERP para acompanhamento",
            "columns": ["Ordem", "Fornecedor", "Status ERP", "Ultima atualizacao", _action_column_label()],
            "column_keys": ["ordem", "fornecedor", "status_erp", "ultima_atualizacao", "acao"],
            "rows": drilldown_rows,
        },
    }


def _build_compliance_section(
    records: List[dict],
    comparison: List[dict],
    _dataset: Dict[str, Any],
    _filters: Dict[str, Any],
) -> Dict[str, Any]:
    no_competition = sum(1 for row in records if _no_competition_flag(row))
    no_competition_prev = sum(1 for row in comparison if _no_competition_flag(row))

    approved_exceptions = sum(1 for row in records if _approved_exception_flag(str(row.get("award_reason") or "")))
    approved_exceptions_prev = sum(
        1 for row in comparison if _approved_exception_flag(str(row.get("award_reason") or ""))
    )

    critical_actions = sum(int(row.get("critical_events_count") or 0) for row in records)
    critical_actions_prev = sum(int(row.get("critical_events_count") or 0) for row in comparison)

    emergency_without_competition = sum(
        1 for row in records if row.get("purchase_type") == "emergencial" and _no_competition_flag(row)
    )
    emergency_without_competition_prev = sum(
        1 for row in comparison if row.get("purchase_type") == "emergencial" and _no_competition_flag(row)
    )

    kpis = [
        _kpi(
            "no_competition",
            "Compras sem concorrencia",
            no_competition,
            _fmt_int(no_competition),
            "Contagem de processos com ate 1 convite, resposta ou proposta valida. Serve como alerta de baixa concorrencia.",
            _trend(no_competition, no_competition_prev, lower_is_better=True),
        ),
        _kpi(
            "approved_exceptions",
            "Excecoes aprovadas",
            approved_exceptions,
            _fmt_int(approved_exceptions),
            "Decisoes registradas com justificativa de excecao.",
            _trend(approved_exceptions, approved_exceptions_prev, lower_is_better=True),
        ),
        _kpi(
            "critical_actions",
            "Acoes criticas",
            critical_actions,
            _fmt_int(critical_actions),
            "Volume de eventos de cancelamento, adjudicacao e envio ERP.",
            _trend(critical_actions, critical_actions_prev, lower_is_better=True),
        ),
        _kpi(
            "emergency_without_competition",
            "Emergenciais sem concorrencia",
            emergency_without_competition,
            _fmt_int(emergency_without_competition),
            "Compras emergenciais com baixa concorrencia.",
            _trend(emergency_without_competition, emergency_without_competition_prev, lower_is_better=True),
        ),
    ]

    by_buyer: Dict[str, float] = {}
    for row in records:
        buyer = str(row.get("requested_by") or "Nao informado").strip() or "Nao informado"
        by_buyer[buyer] = by_buyer.get(buyer, 0.0) + float(row.get("critical_events_count") or 0.0)

    charts = [
        {
            "key": "compliance_findings",
            "type": "bar",
            "title": "Ocorrencias de compliance",
            "items": _chart_items_from_mapping(
                {
                    "Sem concorrencia": no_competition,
                    "Excecoes aprovadas": approved_exceptions,
                    "Acoes criticas": critical_actions,
                    "Emergenciais sem concorrencia": emergency_without_competition,
                },
                value_formatter=_fmt_number,
            ),
        },
        {
            "key": "critical_by_buyer",
            "type": "ranking",
            "title": "Acoes criticas por comprador",
            "items": _chart_items_from_mapping(
                dict(sorted(by_buyer.items(), key=lambda item: item[1], reverse=True)[:8]),
                value_formatter=_fmt_number,
            ),
        },
    ]

    drilldown_rows = []
    for row in sorted(
        [item for item in records if _no_competition_flag(item) or _approved_exception_flag(str(item.get("award_reason") or ""))],
        key=lambda item: item.get("pr_created_at") or "",
        reverse=True,
    )[:20]:
        drilldown_rows.append(
            _enrich_record_drilldown_row(
                {
                    "solicitacao": row.get("pr_number") or f"SR-{row.get('pr_id')}",
                    "comprador": row.get("requested_by") or "Nao informado",
                    "concorrencia": "Baixa" if _no_competition_flag(row) else "Adequada",
                    "excecao": "Sim" if _approved_exception_flag(str(row.get("award_reason") or "")) else "Nao",
                    "link": _record_link(row),
                },
                row,
            )
        )

    return {
        "kpis": kpis,
        "charts": charts,
        "drilldown": {
            "title": "Itens com ponto de atencao",
            "columns": ["Solicitacao", "Comprador", "Concorrencia", "Excecao", _action_column_label()],
            "column_keys": ["solicitacao", "comprador", "concorrencia", "excecao", "acao"],
            "rows": drilldown_rows,
        },
    }


_SECTION_BUILDERS = {
    "overview": _build_overview_section,
    "efficiency": _build_efficiency_section,
    "costs": _build_costs_section,
    "suppliers": _build_suppliers_section,
    "quality_erp": _build_quality_erp_section,
    "compliance": _build_compliance_section,
}


def _load_dataset(db, tenant_id: str | None, visibility: Dict[str, Any]) -> Dict[str, Any]:
    effective_tenant_id = tenant_id or DEFAULT_TENANT_ID
    pr_rows = [
        dict(row)
        for row in db.execute(
            """
            SELECT id, number, status, priority, requested_by, department, needed_at, created_at, updated_at
            FROM purchase_requests
            WHERE tenant_id = ?
            ORDER BY created_at DESC, id DESC
            """,
            (effective_tenant_id,),
        ).fetchall()
    ]

    if visibility.get("restrict_by_actor"):
        allowed_actors = {_normalize_text(item) for item in visibility.get("actors", []) if _normalize_text(item)}
        pr_rows = [row for row in pr_rows if _normalize_text(row.get("requested_by")) in allowed_actors]

    pr_ids = [int(row["id"]) for row in pr_rows]
    if not pr_ids:
        return {"records": []}

    pr_items = _query_by_ids(
        db,
        """
        SELECT id, purchase_request_id
        FROM purchase_request_items
        WHERE tenant_id = ? AND purchase_request_id IN ({placeholders})
        """,
        pr_ids,
        prefix_params=(effective_tenant_id,),
    )
    item_to_pr = {int(row["id"]): int(row["purchase_request_id"]) for row in pr_items}
    item_ids = list(item_to_pr.keys())

    rfq_items = _query_by_ids(
        db,
        """
        SELECT id, rfq_id, purchase_request_item_id
        FROM rfq_items
        WHERE tenant_id = ? AND purchase_request_item_id IN ({placeholders})
        """,
        item_ids,
        prefix_params=(effective_tenant_id,),
    )
    pr_to_rfq_ids: Dict[int, set[int]] = {}
    for row in rfq_items:
        pr_id = item_to_pr.get(int(row["purchase_request_item_id"]))
        if not pr_id:
            continue
        pr_to_rfq_ids.setdefault(pr_id, set()).add(int(row["rfq_id"]))

    rfq_ids = sorted({rfq_id for values in pr_to_rfq_ids.values() for rfq_id in values})
    rfqs = _query_by_ids(
        db,
        """
        SELECT id, status, title, created_at, updated_at
        FROM rfqs
        WHERE tenant_id = ? AND id IN ({placeholders})
        """,
        rfq_ids,
        prefix_params=(effective_tenant_id,),
    )
    rfq_by_id = {int(row["id"]): row for row in rfqs}

    awards_rows = _query_by_ids(
        db,
        """
        SELECT id, rfq_id, supplier_name, status, reason, purchase_order_id, created_at, updated_at
        FROM awards
        WHERE tenant_id = ? AND rfq_id IN ({placeholders})
        ORDER BY id DESC
        """,
        rfq_ids,
        prefix_params=(effective_tenant_id,),
    )
    latest_award_by_rfq: Dict[int, dict] = {}
    for row in awards_rows:
        rfq_id = int(row["rfq_id"])
        if rfq_id not in latest_award_by_rfq:
            latest_award_by_rfq[rfq_id] = row

    award_ids = [int(row["id"]) for row in latest_award_by_rfq.values()]
    explicit_po_ids = [
        int(row["purchase_order_id"])
        for row in latest_award_by_rfq.values()
        if row.get("purchase_order_id") is not None
    ]

    po_by_award_id = {
        int(row["award_id"]): row
        for row in _query_by_ids(
            db,
            """
            SELECT id, award_id, number, supplier_name, status, currency, total_amount, erp_last_error,
                   external_id, created_at, updated_at
            FROM purchase_orders
            WHERE tenant_id = ? AND award_id IN ({placeholders})
            ORDER BY id DESC
            """,
            award_ids,
            prefix_params=(effective_tenant_id,),
        )
    }
    po_by_id = {
        int(row["id"]): row
        for row in _query_by_ids(
            db,
            """
            SELECT id, award_id, number, supplier_name, status, currency, total_amount, erp_last_error,
                   external_id, created_at, updated_at
            FROM purchase_orders
            WHERE tenant_id = ? AND id IN ({placeholders})
            """,
            explicit_po_ids,
            prefix_params=(effective_tenant_id,),
        )
    }

    invite_rows = _query_by_ids(
        db,
        """
        SELECT id, rfq_id, supplier_id, status, opened_at, submitted_at, created_at
        FROM rfq_supplier_invites
        WHERE tenant_id = ? AND rfq_id IN ({placeholders})
        """,
        rfq_ids,
        prefix_params=(effective_tenant_id,),
    )

    quote_rows = _query_by_ids(
        db,
        """
        SELECT q.rfq_id, q.supplier_id, qi.rfq_item_id, qi.unit_price, ri.quantity
        FROM quotes q
        JOIN quote_items qi ON qi.quote_id = q.id AND qi.tenant_id = q.tenant_id
        LEFT JOIN rfq_items ri ON ri.id = qi.rfq_item_id AND ri.tenant_id = q.tenant_id
        WHERE q.tenant_id = ? AND q.rfq_id IN ({placeholders})
        """,
        rfq_ids,
        prefix_params=(effective_tenant_id,),
    )

    supplier_ids = sorted(
        {
            int(value)
            for value in [row.get("supplier_id") for row in invite_rows] + [row.get("supplier_id") for row in quote_rows]
            if value is not None
        }
    )
    suppliers = _query_by_ids(
        db,
        """
        SELECT id, name, risk_flags
        FROM suppliers
        WHERE tenant_id = ? AND id IN ({placeholders})
        """,
        supplier_ids,
        prefix_params=(effective_tenant_id,),
    )
    supplier_name_by_id = {int(row["id"]): str(row.get("name") or "").strip() for row in suppliers}
    supplier_late_flag_by_name = {
        _normalize_text(str(row.get("name") or "")): bool(_parse_risk_flags(row.get("risk_flags")).get("late_delivery"))
        for row in suppliers
    }

    invite_stats_by_rfq, late_count_by_supplier = _build_invite_stats(invite_rows)
    quote_summary_by_rfq = _build_quote_stats(quote_rows, supplier_name_by_id)

    po_ids = sorted({int(po_id) for po_id in [*po_by_id.keys(), *[int(row["id"]) for row in po_by_award_id.values()]]})

    pr_events = _load_status_events_for_entity(db, effective_tenant_id, "purchase_request", pr_ids)
    rfq_events = _load_status_events_for_entity(db, effective_tenant_id, "rfq", rfq_ids)
    award_events = _load_status_events_for_entity(db, effective_tenant_id, "award", award_ids)
    po_events = _load_status_events_for_entity(db, effective_tenant_id, "purchase_order", po_ids)
    po_event_stats = _build_po_event_stats(po_events)

    critical_counts = {
        "purchase_request": _critical_event_count_by_entity(pr_events),
        "rfq": _critical_event_count_by_entity(rfq_events),
        "award": _critical_event_count_by_entity(award_events),
        "purchase_order": _critical_event_count_by_entity(po_events),
    }

    records: List[dict] = []
    for pr in pr_rows:
        pr_id = int(pr["id"])
        rfq_id = _pick_rfq_for_request(pr_to_rfq_ids.get(pr_id, set()), rfq_by_id)
        rfq = rfq_by_id.get(rfq_id) if rfq_id is not None else None
        award = latest_award_by_rfq.get(rfq_id) if rfq_id is not None else None

        po = None
        if award:
            explicit_po_id = award.get("purchase_order_id")
            if explicit_po_id is not None and int(explicit_po_id) in po_by_id:
                po = po_by_id[int(explicit_po_id)]
            elif int(award["id"]) in po_by_award_id:
                po = po_by_award_id[int(award["id"])]

        invite_stats = invite_stats_by_rfq.get(int(rfq_id or 0), {})
        quote_summary = quote_summary_by_rfq.get(int(rfq_id or 0), {})
        supplier_name = (
            str((po or {}).get("supplier_name") or "")
            or str((award or {}).get("supplier_name") or "")
        ).strip()
        supplier_norm = _normalize_text(supplier_name)

        po_event = po_event_stats.get(int((po or {}).get("id") or 0), {})
        erp_rejected = _erp_rejected_flag(po, po_event)
        erp_status = _erp_ui_status(po, erp_rejected)

        critical_total = (
            critical_counts["purchase_request"].get(pr_id, 0)
            + critical_counts["rfq"].get(int((rfq or {}).get("id") or 0), 0)
            + critical_counts["award"].get(int((award or {}).get("id") or 0), 0)
            + critical_counts["purchase_order"].get(int((po or {}).get("id") or 0), 0)
        )

        record = {
            "workspace_id": effective_tenant_id,
            "pr_id": pr_id,
            "pr_number": pr.get("number"),
            "pr_status": pr.get("status"),
            "priority": pr.get("priority"),
            "purchase_type": _purchase_type(pr.get("priority")),
            "requested_by": pr.get("requested_by"),
            "department": pr.get("department"),
            "needed_at": pr.get("needed_at"),
            "pr_created_at": pr.get("created_at"),
            "pr_updated_at": pr.get("updated_at"),
            "rfq_id": (rfq or {}).get("id"),
            "rfq_status": (rfq or {}).get("status"),
            "rfq_title": (rfq or {}).get("title"),
            "rfq_created_at": (rfq or {}).get("created_at"),
            "rfq_updated_at": (rfq or {}).get("updated_at"),
            "award_id": (award or {}).get("id"),
            "award_status": (award or {}).get("status"),
            "award_reason": (award or {}).get("reason"),
            "award_created_at": (award or {}).get("created_at"),
            "po_id": (po or {}).get("id"),
            "po_number": (po or {}).get("number"),
            "po_status": (po or {}).get("status"),
            "po_total_amount": float((po or {}).get("total_amount") or 0.0),
            "po_currency": (po or {}).get("currency") or "BRL",
            "po_created_at": (po or {}).get("created_at"),
            "po_updated_at": (po or {}).get("updated_at"),
            "po_erp_last_error": (po or {}).get("erp_last_error"),
            "supplier_name": supplier_name,
            "winner_supplier_name": quote_summary.get("winner_supplier_name") or supplier_name,
            "rfq_invite_count": int(invite_stats.get("invite_count") or 0),
            "rfq_response_count": int(invite_stats.get("response_count") or 0),
            "rfq_avg_response_hours": invite_stats.get("avg_response_hours"),
            "rfq_quote_count": int(quote_summary.get("quote_count") or 0),
            "rfq_quote_avg_total": float(quote_summary.get("avg_total") or 0.0),
            "rfq_quote_winner_total": float(quote_summary.get("winner_total") or 0.0),
            "rfq_savings_abs": float(quote_summary.get("savings_abs") or 0.0),
            "rfq_savings_pct": float(quote_summary.get("savings_pct") or 0.0),
            "winner_vs_avg_ratio": quote_summary.get("winner_vs_avg_ratio"),
            "supplier_delay_score": float(late_count_by_supplier.get(supplier_norm, 0.0))
            + (1.0 if supplier_late_flag_by_name.get(supplier_norm) else 0.0),
            "critical_events_count": int(critical_total),
            "po_retry_count": int(po_event.get("retry_count") or 0),
            "po_integration_error_count": int(po_event.get("integration_error_count") or 0),
            "erp_rejected": bool(erp_rejected),
            "erp_ui_status": erp_status,
        }

        record["sr_to_oc_hours"] = _hours_between(record.get("pr_created_at"), record.get("po_created_at"))
        record["stage_sr_to_rfq_hours"] = _hours_between(record.get("pr_created_at"), record.get("rfq_created_at"))
        record["stage_rfq_to_award_hours"] = _hours_between(record.get("rfq_created_at"), record.get("award_created_at"))
        record["stage_award_to_po_hours"] = _hours_between(record.get("award_created_at"), record.get("po_created_at"))
        record["stage_po_to_erp_hours"] = _hours_between(record.get("po_created_at"), po_event.get("first_push_at"))
        record["stage_erp_resolution_hours"] = _hours_between(po_event.get("first_push_at"), record.get("po_updated_at"))
        record["is_delayed"] = _is_delayed(record)

        records.append(record)

    return {"records": records}


def _apply_filters(records: List[dict], filters: Dict[str, Any], include_period: bool) -> List[dict]:
    supplier_filter = _normalize_text(filters.get("supplier"))
    buyer_filter = _normalize_text(filters.get("buyer"))
    statuses = {str(value).strip() for value in filters.get("status", []) if str(value).strip()}
    purchase_types = {str(value).strip() for value in filters.get("purchase_type", []) if str(value).strip()}
    workspace_filter = str(filters.get("workspace_id") or "").strip()

    result: List[dict] = []
    for row in records:
        if workspace_filter and workspace_filter != str(row.get("workspace_id") or ""):
            continue
        if supplier_filter and supplier_filter not in _normalize_text(row.get("supplier_name")):
            continue
        if buyer_filter and buyer_filter not in _normalize_text(row.get("requested_by")):
            continue
        if statuses and not any(
            str(row.get(field) or "").strip() in statuses for field in ("pr_status", "rfq_status", "po_status")
        ):
            continue
        if purchase_types and str(row.get("purchase_type") or "").strip() not in purchase_types:
            continue
        if include_period and not _in_period(row.get("pr_created_at"), filters.get("start_date"), filters.get("end_date")):
            continue
        result.append(row)
    return result


def _apply_period(
    records: List[dict],
    start: date | None,
    end: date | None,
    date_field: str = "pr_created_at",
) -> List[dict]:
    if not start and not end:
        return list(records)
    return [row for row in records if _in_period(row.get(date_field), start, end)]


def _previous_period_window(start: date | None, end: date | None) -> Tuple[date | None, date | None]:
    if not start or not end:
        return (None, None)
    delta_days = max(1, (end - start).days + 1)
    previous_end = start - timedelta(days=1)
    previous_start = previous_end - timedelta(days=delta_days - 1)
    return (previous_start, previous_end)


def _in_period(raw_datetime: Any, start: date | None, end: date | None) -> bool:
    value = _parse_datetime(raw_datetime)
    if not value:
        return False
    current_date = value.date()
    if start and current_date < start:
        return False
    if end and current_date > end:
        return False
    return True


def _query_by_ids(db, sql_template: str, ids: Sequence[int], prefix_params: Sequence[Any]) -> List[dict]:
    normalized_ids = [int(value) for value in ids if value is not None]
    if not normalized_ids:
        return []
    rows: List[dict] = []
    for chunk in _chunked(list(dict.fromkeys(normalized_ids)), size=400):
        placeholders = ",".join("?" for _ in chunk)
        sql = sql_template.format(placeholders=placeholders)
        query_params = [*prefix_params, *chunk]
        rows.extend(dict(row) for row in db.execute(sql, tuple(query_params)).fetchall())
    return rows


def _load_status_events_for_entity(db, tenant_id: str, entity: str, entity_ids: Sequence[int]) -> List[dict]:
    return _query_by_ids(
        db,
        """
        SELECT id, entity, entity_id, from_status, to_status, reason, occurred_at
        FROM status_events
        WHERE tenant_id = ? AND entity = ? AND entity_id IN ({placeholders})
        ORDER BY occurred_at ASC, id ASC
        """,
        [int(value) for value in entity_ids if value is not None],
        prefix_params=(tenant_id, entity),
    )


def _critical_event_count_by_entity(events: Iterable[dict]) -> Dict[int, int]:
    counters: Dict[int, int] = {}
    for event in events:
        reason = str(event.get("reason") or "").strip().lower()
        if reason not in CRITICAL_EVENT_REASONS:
            continue
        entity_id = int(event.get("entity_id") or 0)
        if entity_id <= 0:
            continue
        counters[entity_id] = counters.get(entity_id, 0) + 1
    return counters


def _build_invite_stats(invite_rows: Sequence[dict]) -> Tuple[Dict[int, dict], Dict[str, int]]:
    by_rfq: Dict[int, dict] = {}
    expired_by_supplier_norm: Dict[str, int] = {}
    for row in invite_rows:
        rfq_id = int(row.get("rfq_id") or 0)
        if rfq_id <= 0:
            continue
        bucket = by_rfq.setdefault(
            rfq_id,
            {
                "invite_count": 0,
                "response_count": 0,
                "response_hours": [],
            },
        )
        bucket["invite_count"] += 1

        status = str(row.get("status") or "").strip().lower()
        submitted_at = row.get("submitted_at")
        if status == "submitted" or submitted_at:
            bucket["response_count"] += 1
            opened_at = row.get("opened_at") or row.get("created_at")
            elapsed = _hours_between(opened_at, submitted_at)
            if elapsed is not None:
                bucket["response_hours"].append(elapsed)

        if status == "expired":
            supplier_norm = _normalize_text(row.get("supplier_id"))
            if supplier_norm:
                expired_by_supplier_norm[supplier_norm] = expired_by_supplier_norm.get(supplier_norm, 0) + 1

    normalized = {}
    for rfq_id, bucket in by_rfq.items():
        normalized[rfq_id] = {
            "invite_count": int(bucket["invite_count"]),
            "response_count": int(bucket["response_count"]),
            "avg_response_hours": _avg(bucket["response_hours"]),
        }
    return normalized, expired_by_supplier_norm


def _build_quote_stats(quote_rows: Sequence[dict], supplier_name_by_id: Dict[int, str]) -> Dict[int, dict]:
    per_rfq_supplier: Dict[int, Dict[int, float]] = {}
    for row in quote_rows:
        rfq_id = int(row.get("rfq_id") or 0)
        supplier_id = int(row.get("supplier_id") or 0)
        if rfq_id <= 0 or supplier_id <= 0:
            continue
        quantity = _safe_float(row.get("quantity")) or 1.0
        unit_price = _safe_float(row.get("unit_price"))
        if unit_price is None:
            continue
        total = unit_price * max(1.0, quantity)
        per_rfq_supplier.setdefault(rfq_id, {})
        per_rfq_supplier[rfq_id][supplier_id] = per_rfq_supplier[rfq_id].get(supplier_id, 0.0) + total

    summaries: Dict[int, dict] = {}
    for rfq_id, totals_by_supplier in per_rfq_supplier.items():
        totals = list(totals_by_supplier.values())
        if not totals:
            continue
        avg_total = mean(totals)
        winner_supplier_id, winner_total = min(totals_by_supplier.items(), key=lambda item: item[1])
        savings_abs = max(0.0, avg_total - winner_total)
        savings_pct = (savings_abs / avg_total * 100.0) if avg_total > 0 else 0.0
        winner_ratio = (winner_total / avg_total) if avg_total > 0 else None
        summaries[rfq_id] = {
            "quote_count": len(totals),
            "avg_total": float(avg_total),
            "winner_total": float(winner_total),
            "savings_abs": float(savings_abs),
            "savings_pct": float(savings_pct),
            "winner_vs_avg_ratio": winner_ratio,
            "winner_supplier_name": supplier_name_by_id.get(winner_supplier_id) or f"Fornecedor {winner_supplier_id}",
        }
    return summaries


def _build_po_event_stats(po_events: Sequence[dict]) -> Dict[int, dict]:
    by_po: Dict[int, list[dict]] = {}
    for event in po_events:
        po_id = int(event.get("entity_id") or 0)
        if po_id <= 0:
            continue
        by_po.setdefault(po_id, []).append(event)

    stats: Dict[int, dict] = {}
    for po_id, events in by_po.items():
        retry_count = 0
        integration_error_count = 0
        first_push_at = None
        rejected = False
        for event in events:
            reason = str(event.get("reason") or "").strip().lower()
            to_status = str(event.get("to_status") or "").strip().lower()
            occurred_at = event.get("occurred_at")
            if not first_push_at and (reason.startswith("po_push_") or to_status == "sent_to_erp"):
                first_push_at = occurred_at
            if reason == "po_push_retry_started":
                retry_count += 1
            if reason in {"po_push_failed", "po_push_rejected"} or to_status == "erp_error":
                integration_error_count += 1
            if reason == "po_push_rejected":
                rejected = True
        stats[po_id] = {
            "retry_count": retry_count,
            "integration_error_count": integration_error_count,
            "first_push_at": first_push_at,
            "rejected": rejected,
        }
    return stats


def _pick_rfq_for_request(rfq_ids: Iterable[int], rfq_by_id: Dict[int, dict]) -> int | None:
    ordered = sorted(
        [int(value) for value in rfq_ids if int(value) in rfq_by_id],
        key=lambda rfq_id: (
            _parse_datetime(rfq_by_id.get(rfq_id, {}).get("created_at")) or datetime.max.replace(tzinfo=timezone.utc),
            rfq_id,
        ),
    )
    return ordered[0] if ordered else None


def _purchase_type(priority: Any) -> str:
    return "emergencial" if str(priority or "").strip().lower() == "urgent" else "regular"


def _is_delayed(record: dict) -> bool:
    needed = _parse_date(record.get("needed_at"))
    if not needed:
        return False
    status = str(record.get("pr_status") or "").strip().lower()
    if status in {"received", "cancelled"}:
        return False
    return needed < datetime.now(timezone.utc).date()


def _erp_rejected_flag(po: dict | None, po_event: dict | None) -> bool:
    if not po:
        return False
    if bool((po_event or {}).get("rejected")):
        return True
    if str(po.get("status") or "").strip().lower() != "erp_error":
        return False
    error_details = str(po.get("erp_last_error") or "").strip().lower()
    if not error_details:
        return False
    return any(marker in error_details for marker in ERP_REJECTION_HINTS)


def _erp_ui_status(po: dict | None, rejected: bool) -> str:
    status = str((po or {}).get("status") or "").strip().lower()
    if status == "erp_error":
        return "rejeitado" if rejected else "reenvio_necessario"
    if status == "sent_to_erp":
        return "enviado"
    if status in {"erp_accepted", "received", "partially_received"}:
        return "aceito"
    if not status:
        return "nao_enviado"
    return "nao_enviado"


def _no_competition_flag(record: dict) -> bool:
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


def _approved_exception_flag(reason: str) -> bool:
    normalized = _normalize_text(reason)
    if not normalized:
        return False
    return any(marker in normalized for marker in EXCEPTION_REASON_HINTS)


def _record_link(record: dict) -> str:
    if record.get("po_id"):
        return f"/procurement/purchase-orders/{record['po_id']}"
    if record.get("rfq_id"):
        return f"/procurement/cotacoes/{record['rfq_id']}"
    return "/procurement/solicitacoes"


def _kpi(
    key: str,
    label: str,
    value: float | int,
    display_value: str,
    tooltip: str,
    trend: Dict[str, Any] | None,
) -> Dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "value": value,
        "display_value": display_value,
        "tooltip": tooltip,
        "trend": trend,
    }


def _trend(current: float, previous: float, lower_is_better: bool = False) -> Dict[str, Any]:
    current_value = float(current or 0.0)
    previous_value = float(previous or 0.0)
    if previous_value == 0:
        delta_pct = 0.0 if current_value == 0 else 100.0
    else:
        delta_pct = ((current_value - previous_value) / abs(previous_value)) * 100.0

    direction = "flat"
    if delta_pct > 1:
        direction = "up"
    elif delta_pct < -1:
        direction = "down"

    effective_direction = direction
    if lower_is_better and direction in {"up", "down"}:
        effective_direction = "up" if direction == "down" else "down"

    if effective_direction == "up":
        label = "Melhora"
    elif effective_direction == "down":
        label = "Piora"
    else:
        label = "Estavel"

    return {
        "direction": effective_direction,
        "delta_pct": round(delta_pct, 2),
        "display": f"{delta_pct:+.1f}%",
        "label": label,
    }


def _chart_items_from_mapping(
    mapping: Dict[str, float],
    value_formatter=None,
) -> List[Dict[str, Any]]:
    if not mapping:
        return []
    values = [float(value) for value in mapping.values()]
    max_value = max(values) if values else 0.0
    formatter = value_formatter or _fmt_number
    items: List[Dict[str, Any]] = []
    for label, value in mapping.items():
        numeric = float(value or 0.0)
        ratio = (numeric / max_value * 100.0) if max_value > 0 else 0.0
        items.append(
            {
                "label": label,
                "value": numeric,
                "display_value": formatter(numeric),
                "ratio": round(ratio, 2),
            }
        )
    return items


def _series_count_by_date(records: List[dict], date_key: str) -> List[Dict[str, Any]]:
    counters: Dict[str, float] = {}
    for row in records:
        label = _day_label(row.get(date_key))
        if not label:
            continue
        counters[label] = counters.get(label, 0.0) + 1.0
    return _chart_items_from_mapping(dict(sorted(counters.items())), value_formatter=_fmt_number)


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(round(float(value))):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "0"


def _fmt_number(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "0"
    if numeric.is_integer():
        return _fmt_int(numeric)
    return f"{numeric:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_currency(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    formatted = f"{numeric:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatted}"


def _fmt_percent(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    return f"{numeric:.1f}%"


def _fmt_duration(hours: Any) -> str:
    if hours is None:
        return "Nao informado"
    try:
        numeric = float(hours)
    except (TypeError, ValueError):
        return "Nao informado"
    if numeric >= 24:
        return f"{numeric / 24.0:.1f} dias"
    return f"{numeric:.1f} h"


def _avg(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    return float(round(mean(values), 2))


def _hours_between(start_raw: Any, end_raw: Any) -> float | None:
    start = _parse_datetime(start_raw)
    end = _parse_datetime(end_raw)
    if not start or not end:
        return None
    delta = end - start
    return round(delta.total_seconds() / 3600.0, 2)


def _day_label(raw_datetime: Any) -> str | None:
    value = _parse_datetime(raw_datetime)
    if not value:
        return None
    return value.strftime("%Y-%m-%d")


def _parse_risk_flags(raw_value: Any) -> dict:
    if raw_value in (None, ""):
        return {}
    if isinstance(raw_value, dict):
        return raw_value
    try:
        parsed = json.loads(str(raw_value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


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
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            continue
    return None


def _parse_date(raw_value: Any) -> date | None:
    if raw_value in (None, ""):
        return None
    value = str(raw_value).strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        parsed = _parse_datetime(value)
        return parsed.date() if parsed else None


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_actors(values: Iterable[Any]) -> set[str]:
    normalized = {_normalize_text(value) for value in values}
    return {value for value in normalized if value}


def _coerce_to_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [chunk.strip() for chunk in value.replace(";", ",").replace("\n", ",").split(",") if chunk.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _parse_csv_values(value: Any) -> List[str]:
    if value in (None, ""):
        return []
    return [chunk.strip() for chunk in str(value).split(",") if chunk.strip()]


def _chunked(values: Sequence[int], size: int) -> Iterable[Sequence[int]]:
    if size <= 0:
        size = 200
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]
