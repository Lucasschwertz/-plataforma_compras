from __future__ import annotations

from typing import Dict, List

from app.procurement.flow_policy import frontend_bundle as flow_frontend_bundle


FRIENDLY_TERMS: Dict[str, str] = {
    "app_name": "Plataforma Compras",
    "purchase_request": "Solicitacao de compra",
    "rfq": "Cotacao",
    "award": "Decisao",
    "purchase_order": "Ordem de compra",
    "supplier": "Fornecedor",
    "receipt": "Recebimento",
    "workspace": "Workspace",
}


STATUS_GROUPS: Dict[str, List[Dict[str, str]]] = {
    "solicitacao": [
        {
            "key": "pending_rfq",
            "label": "Pendente de cotacao",
            "description": "Solicitacao pronta para iniciar o processo de cotacao.",
        },
        {
            "key": "in_rfq",
            "label": "Em cotacao",
            "description": "Solicitacao ja vinculada a uma cotacao ativa.",
        },
        {
            "key": "awarded",
            "label": "Com decisao",
            "description": "Cotacao concluida com fornecedor definido.",
        },
        {
            "key": "ordered",
            "label": "Ordenada",
            "description": "Ordem de compra gerada para a solicitacao.",
        },
        {
            "key": "partially_received",
            "label": "Recebimento parcial",
            "description": "Recebimento iniciado, mas ainda incompleto.",
        },
        {
            "key": "received",
            "label": "Recebida",
            "description": "Solicitacao encerrada com recebimento total.",
        },
        {
            "key": "cancelled",
            "label": "Cancelada",
            "description": "Solicitacao encerrada sem continuidade.",
        },
    ],
    "cotacao": [
        {
            "key": "draft",
            "label": "Rascunho",
            "description": "Cotacao em preparacao, ainda sem execucao completa.",
        },
        {
            "key": "open",
            "label": "Aberta",
            "description": "Cotacao aberta para convites e envio de propostas.",
        },
        {
            "key": "collecting_quotes",
            "label": "Coletando propostas",
            "description": "Fornecedores convidados enviando propostas.",
        },
        {
            "key": "closed",
            "label": "Fechada",
            "description": "Janela de propostas encerrada para analise.",
        },
        {
            "key": "awarded",
            "label": "Com decisao",
            "description": "Fornecedor vencedor definido para a cotacao.",
        },
        {
            "key": "cancelled",
            "label": "Cancelada",
            "description": "Cotacao encerrada sem adjudicacao.",
        },
    ],
    "decisao": [
        {
            "key": "awarded",
            "label": "Aprovada",
            "description": "Decisao registrada com fornecedor selecionado.",
        },
        {
            "key": "converted_to_po",
            "label": "Convertida em OC",
            "description": "Decisao convertida em ordem de compra.",
        },
        {
            "key": "cancelled",
            "label": "Cancelada",
            "description": "Decisao cancelada no processo.",
        },
    ],
    "ordem_compra": [
        {
            "key": "draft",
            "label": "Rascunho",
            "description": "Ordem criada localmente e ainda nao finalizada.",
        },
        {
            "key": "approved",
            "label": "Aprovada",
            "description": "Ordem aprovada internamente e pronta para envio.",
        },
        {
            "key": "sent_to_erp",
            "label": "Enviada ao ERP",
            "description": "Ordem enviada e aguardando retorno do ERP.",
        },
        {
            "key": "erp_accepted",
            "label": "Aceita no ERP",
            "description": "Ordem aceita e registrada no ERP.",
        },
        {
            "key": "partially_received",
            "label": "Recebimento parcial",
            "description": "Recebimento iniciado, com saldo pendente.",
        },
        {
            "key": "received",
            "label": "Recebida",
            "description": "Ordem concluida com recebimento total.",
        },
        {
            "key": "erp_error",
            "label": "Erro no ERP",
            "description": "Falha no envio ou processamento no ERP.",
        },
        {
            "key": "cancelled",
            "label": "Cancelada",
            "description": "Ordem encerrada sem continuidade.",
        },
    ],
    "fornecedor": [
        {
            "key": "pending",
            "label": "Convite pendente",
            "description": "Convite enviado aguardando abertura.",
        },
        {
            "key": "opened",
            "label": "Convite aberto",
            "description": "Fornecedor abriu o convite de cotacao.",
        },
        {
            "key": "submitted",
            "label": "Proposta enviada",
            "description": "Fornecedor enviou proposta para os itens.",
        },
        {
            "key": "expired",
            "label": "Convite expirado",
            "description": "Prazo do convite encerrado sem submissao.",
        },
        {
            "key": "cancelled",
            "label": "Convite cancelado",
            "description": "Convite cancelado pelo comprador.",
        },
    ],
}


INTEGRATION_STATUS_GROUP: List[Dict[str, str]] = [
    {
        "key": "running",
        "label": "Em execucao",
        "description": "Sincronizacao em andamento.",
    },
    {
        "key": "succeeded",
        "label": "Sucesso",
        "description": "Sincronizacao concluida com sucesso.",
    },
    {
        "key": "failed",
        "label": "Falha",
        "description": "Sincronizacao finalizada com erro.",
    },
]


ERP_STATUS_GROUP: List[Dict[str, str]] = [
    {
        "key": "nao_enviado",
        "label": "Nao enviado",
        "description": "Ordem ainda nao enviada para integracao com ERP.",
    },
    {
        "key": "enviado",
        "label": "Enviado",
        "description": "Envio realizado para o ERP, aguardando retorno de processamento.",
    },
    {
        "key": "aceito",
        "label": "Aceito",
        "description": "ERP aceitou a ordem e confirmou o processamento.",
    },
    {
        "key": "rejeitado",
        "label": "Rejeitado",
        "description": "ERP rejeitou a ordem e exige revisao dos dados.",
    },
    {
        "key": "reenvio_necessario",
        "label": "Reenvio necessario",
        "description": "Falha temporaria de integracao; e necessario reenviar a ordem.",
    },
]


ERP_STATUS_TECHNICAL_MAP: Dict[str, str] = {
    "draft": "nao_enviado",
    "approved": "nao_enviado",
    "cancelled": "nao_enviado",
    "sent_to_erp": "enviado",
    "erp_accepted": "aceito",
    "partially_received": "aceito",
    "received": "aceito",
}


ERP_TIMELINE_EVENT_LABELS: Dict[str, str] = {
    "envio": "Envio",
    "resposta": "Resposta",
    "erro": "Erro",
    "reenvio": "Reenvio",
}


ERP_REJECTION_HINTS = ("rejeit", "recus", "reject", "invalid", "inval", "422")


ENTITY_LABELS: Dict[str, str] = {
    "purchase_request": "Solicitacao",
    "purchase_order": "Ordem de compra",
    "rfq": "Cotacao",
    "award": "Decisao",
    "supplier": "Fornecedor",
    "receipt": "Recebimento",
}


SCOPE_LABELS: Dict[str, str] = {
    "purchase_order": "Ordens de compra",
    "purchase_request": "Solicitacoes",
    "rfq": "Cotacoes",
    "supplier": "Fornecedores",
    "receipt": "Recebimentos",
    "quote": "Propostas",
    "quote_process": "Processos de cotacao",
    "quote_supplier": "Fornecedores da cotacao",
}


TYPE_LABELS: Dict[str, str] = {
    "purchase_request": "Solicitacao",
    "purchase_order": "Ordem de compra",
    "rfq": "Cotacao",
    "award": "Decisao",
    "supplier": "Fornecedor",
    "receipt": "Recebimento",
}


UI_TEXTS: Dict[str, str] = {
    "title.login": "Entrar - Plataforma Compras",
    "title.register": "Criar conta - Plataforma Compras",
    "title.supplier_portal": "Portal do fornecedor | Plataforma Compras",
    "title.home": "Dashboard | Plataforma Compras",
    "title.inbox": "Inbox | Plataforma Compras",
    "title.requests": "Solicitacoes | Plataforma Compras",
    "title.quotes": "Cotacoes | Plataforma Compras",
    "title.quote_open": "Abrir cotacao | Plataforma Compras",
    "title.orders": "Ordens de compra | Plataforma Compras",
    "title.approvals": "Aprovacoes | Plataforma Compras",
    "title.integrations": "Logs de Integracao | Plataforma Compras",
    "title.erp_followup": "Acompanhamento ERP | Plataforma Compras",
    "title.po_detail": "OC",
    "title.quote_detail": "Cotacao",
    "page.home.default": "Dashboard de compras",
    "page.home.approver": "Painel de aprovacoes",
    "page.home.admin": "Painel administrativo",
    "page.home.manager": "Painel gestor",
    "page.inbox": "Inbox de compras",
    "page.requests": "Solicitacoes de compra",
    "page.quotes": "Cotacoes",
    "page.quote_open": "Abertura de cotacao",
    "page.quote_detail": "Mesa de cotacao e decisao",
    "page.orders": "Ordens de compra",
    "page.po_detail": "Ordem de compra",
    "page.approvals": "Aprovacoes de cotacao",
    "page.integrations": "Integracoes e trilha de cotacoes",
    "page.erp_followup": "Acompanhamento de integracoes ERP",
    "label.all_statuses": "Todos os status",
    "label.erp_status": "Status ERP",
    "label.last_attempt": "Ultima tentativa",
    "label.next_action": "Proxima acao recomendada",
    "label.erp_message": "Mensagem ao operador",
    "label.timeline": "Linha do tempo ERP",
    "label.integration_history": "Historico de integracao",
    "label.view_integration_history": "Ver historico de integracao",
    "label.hide_integration_history": "Ocultar historico de integracao",
    "text.integration_history_on_demand": "O historico de integracao fica oculto e e carregado somente quando solicitado.",
    "analytics.action.view_actions": "Ver acoes",
    "analytics.action.view_actions_hint": "Veja a lista detalhada e execute a proxima acao recomendada.",
    "analytics.action.open_process": "Abrir processo",
    "analytics.action.open_quotes": "Abrir cotacoes",
    "analytics.action.open_rfq": "Abrir cotacao",
    "analytics.action.invite_supplier": "Convidar fornecedor",
    "analytics.action.manage_item_supplier": "Convidar por item",
    "analytics.action.award_rfq": "Registrar decisao",
    "analytics.action.create_purchase_order": "Gerar ordem de compra",
    "analytics.action.push_to_erp": "Enviar ao ERP",
    "analytics.action.view_quotes": "Acompanhar cotacao",
    "analytics.action.view_order": "Abrir ordem",
    "analytics.action.review_decision": "Revisar decisao",
    "analytics.action.refresh_order": "Atualizar dados",
    "analytics.action.track_receipt": "Acompanhar recebimento",
    "analytics.action.view_history": "Ver historico",
    "analytics.action.executed": "Acao executada com sucesso.",
    "analytics.alerts.title": "Alertas operacionais",
    "analytics.alerts.subtitle": "Sinais acionaveis baseados nos indicadores atuais.",
    "analytics.alerts.none": "Sem alertas ativos para os filtros aplicados.",
    "analytics.alerts.active_badge": "Alertas ativos",
    "analytics.alert.severity.high": "Alta",
    "analytics.alert.severity.medium": "Media",
    "analytics.alert.severity.low": "Baixa",
    "analytics.alert.suggested_action": "Acao sugerida",
    "analytics.alert.late_processes.title": "Processos com atraso",
    "analytics.alert.late_processes.description": "Existem processos com prazo vencido e impacto no atendimento.",
    "analytics.alert.supplier_response_low.title": "Baixa resposta de fornecedores",
    "analytics.alert.supplier_response_low.description": "A taxa de resposta esta abaixo da meta minima.",
    "analytics.alert.erp_waiting.title": "Ordens aguardando retorno do ERP",
    "analytics.alert.erp_waiting.description": "Ha ordens aguardando resposta do ERP acima do tempo esperado.",
    "analytics.alert.compliance_outlier.title": "Compras fora do padrao",
    "analytics.alert.compliance_outlier.description": "Foram detectadas compras com baixa concorrencia.",
    "analytics.drilldown.next_action": "Proxima acao",
    "analytics.drilldown.no_action": "Sem acao disponivel",
    "label.cancel_action": "Cancelar",
    "label.confirm_action": "Confirmar acao",
    "title.critical_confirmation": "Confirmar acao critica",
    "impact.cancel_request": "A solicitacao sera encerrada e removida do fluxo ativo.",
    "impact.cancel_rfq": "A cotacao sera encerrada sem continuidade para decisao.",
    "impact.cancel_order": "A ordem de compra sera cancelada e nao seguira para ERP.",
    "impact.cancel_invite": "O convite sera invalidado para o fornecedor selecionado.",
    "impact.push_to_erp": "A ordem sera enviada ao ERP e o status operacional sera atualizado.",
    "impact.award_rfq": "A decisao fixa o fornecedor vencedor da cotacao.",
    "impact.create_purchase_order": "Uma ordem de compra sera gerada a partir da decisao atual.",
    "impact.delete_supplier_proposal": "A proposta do fornecedor sera excluida da cotacao.",
    "erp.status.nao_enviado.message": "A ordem ainda nao foi enviada ao ERP.",
    "erp.next_action.await_response": "Aguardar retorno do ERP.",
    "erp.next_action.review_data": "Revisar dados da ordem e reenviar ao ERP.",
    "erp.next_action.resend": "Reenviar ao ERP",
}


MESSAGES: Dict[str, Dict[str, str]] = {
    "success": {
        "quote_cancelled": "Cotacao cancelada.",
        "quote_updated": "Cotacao atualizada.",
        "request_cancelled": "Solicitacao cancelada.",
        "request_saved": "Solicitacao salva com sucesso.",
        "order_saved": "Ordem de compra salva com sucesso.",
        "order_cancelled": "Ordem de compra cancelada.",
        "order_sent_to_erp": "Ordem enviada ao ERP.",
        "order_already_accepted": "Ordem ja aceita no ERP.",
        "erp_accepted": "ERP aceitou a ordem. Fluxo segue para recebimento.",
        "decision_saved": "Decisao registrada com sucesso.",
        "invite_updated": "Convite atualizado com sucesso.",
        "proposal_saved": "Proposta registrada com sucesso.",
    },
    "error": {
        "action_not_allowed_for_status": "Esta acao nao e permitida para o status atual.",
        "action_invalid": "Acao invalida para esta operacao.",
        "auth_required": "Autenticacao necessaria.",
        "auth_invalid_credentials": "Credenciais invalidas. Tente novamente.",
        "auth_missing_credentials": "Informe email e senha.",
        "confirmation_required": "Confirme explicitamente esta acao critica para continuar.",
        "email_already_registered": "Email ja cadastrado. Use outro email ou faca login.",
        "award_not_found": "Decisao nao encontrada.",
        "description_required": "Descricao obrigatoria para continuar.",
        "erp_managed_purchase_order_readonly": "Ordem com origem ERP e somente leitura na plataforma.",
        "erp_managed_request_readonly": "Solicitacao com origem ERP e somente leitura na plataforma.",
        "erp_push_failed": "Falha ao enviar ao ERP.",
        "erp_unavailable": "Nao conseguimos falar com o ERP agora. Vamos tentar novamente automaticamente.",
        "erp_rejected": "O ERP rejeitou a ordem. Revise os dados e tente novamente.",
        "erp_temporarily_unavailable": "Nao conseguimos falar com o ERP agora. Vamos tentar novamente automaticamente.",
        "erp_order_rejected": "O ERP recusou a ordem. Verifique os dados e tente novamente.",
        "invite_expired": "Convite expirado.",
        "invite_not_found": "Convite nao encontrado.",
        "item_not_found": "Item nao encontrado.",
        "items_required": "Informe itens validos para continuar.",
        "line_no_invalid": "Numero de linha invalido.",
        "no_changes": "Nenhuma alteracao informada.",
        "priority_invalid": "Prioridade informada e invalida.",
        "purchase_order_already_exists": "Esta decisao ja possui ordem de compra.",
        "purchase_order_not_found": "Ordem de compra nao encontrada.",
        "purchase_request_item_ids_required": "Selecione ao menos um item de solicitacao.",
        "purchase_request_items_not_found": "Itens de solicitacao nao encontrados ou indisponiveis.",
        "purchase_request_not_found": "Solicitacao nao encontrada.",
        "quantity_invalid": "Quantidade invalida.",
        "quote_not_found": "Proposta nao encontrada para este fornecedor.",
        "reason_required": "Motivo da decisao e obrigatorio.",
        "request_locked": "Solicitacao bloqueada para alteracoes nesta etapa.",
        "rfq_closed_for_quotes": "Cotacao nao aceita novas propostas neste status.",
        "rfq_item_not_found": "Item da cotacao nao encontrado.",
        "rfq_items_not_found": "Itens da cotacao nao encontrados.",
        "rfq_items_required": "Nenhum item valido para convite.",
        "rfq_not_found": "Cotacao nao encontrada.",
        "permission_denied": "Voce nao possui permissao para executar esta acao.",
        "scope_not_supported": "Escopo nao suportado neste MVP.",
        "scope_required": "Informe o escopo da operacao.",
        "status_invalid": "Status informado e invalido para esta etapa.",
        "supplier_id_invalid": "Fornecedor informado e invalido.",
        "supplier_id_required": "Informe o fornecedor.",
        "supplier_ids_required": "Selecione ao menos um fornecedor.",
        "supplier_name_required": "Informe o fornecedor.",
        "supplier_not_found": "Fornecedor nao encontrado.",
        "supplier_not_invited": "Fornecedor nao convidado para os itens informados.",
        "supplier_not_invited_for_items": "Fornecedor nao convidado para um ou mais itens.",
        "suppliers_not_found": "Nenhum fornecedor valido encontrado.",
        "sync_failed": "Falha ao sincronizar.",
        "total_amount_invalid": "Valor total invalido.",
        "unexpected_error": "Nao foi possivel concluir a operacao. Tente novamente em instantes.",
        "valid_items_required": "Informe ao menos um item valido com preco unitario.",
    },
    "confirm": {
        "cancel_request": "Confirma o cancelamento da solicitacao?",
        "cancel_quote": "Confirma o cancelamento da cotacao?",
        "cancel_order": "Confirma o cancelamento da ordem de compra?",
        "cancel_invite": "Confirma o cancelamento do convite?",
        "award_rfq": "Confirma a decisao da cotacao para este fornecedor?",
        "create_purchase_order": "Confirma a geracao da ordem de compra a partir desta decisao?",
        "push_order_erp": "Confirma o envio da ordem para o ERP?",
        "delete_supplier_proposal": "Confirma a exclusao da proposta do fornecedor?",
    },
}


def status_keys_for_group(group: str) -> List[str]:
    return [item["key"] for item in STATUS_GROUPS.get(group, [])]


def status_items_for_group(group: str) -> List[Dict[str, str]]:
    return list(STATUS_GROUPS.get(group, []))


def all_status_items() -> List[Dict[str, str]]:
    combined: List[Dict[str, str]] = []
    for group_items in STATUS_GROUPS.values():
        combined.extend(group_items)
    combined.extend(INTEGRATION_STATUS_GROUP)
    combined.extend(ERP_STATUS_GROUP)
    return combined


def build_status_labels() -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for item in all_status_items():
        labels[item["key"]] = item["label"]
    return labels


def build_status_descriptions() -> Dict[str, str]:
    descriptions: Dict[str, str] = {}
    for item in all_status_items():
        descriptions[item["key"]] = item["description"]
    return descriptions


STATUS_LABELS = build_status_labels()
STATUS_DESCRIPTIONS = build_status_descriptions()


def get_ui_text(key: str, default: str | None = None) -> str:
    if key in UI_TEXTS:
        return UI_TEXTS[key]
    if default is not None:
        return default
    return key


def get_message(category: str, key: str, default: str | None = None) -> str:
    message = MESSAGES.get(category, {}).get(key)
    if message:
        return message
    if default is not None:
        return default
    return key


def error_message(key: str, default: str | None = None) -> str:
    return get_message("error", key, default)


def success_message(key: str, default: str | None = None) -> str:
    return get_message("success", key, default)


def confirm_message(key: str, default: str | None = None) -> str:
    return get_message("confirm", key, default)


def erp_timeline_event_label(event_type: str, default: str | None = None) -> str:
    label = ERP_TIMELINE_EVENT_LABELS.get(str(event_type or "").strip())
    if label:
        return label
    if default is not None:
        return default
    return event_type


def _is_rejected_erp_error(error_details: str | None) -> bool:
    normalized = (error_details or "").strip().lower()
    if not normalized:
        return False
    return any(marker in normalized for marker in ERP_REJECTION_HINTS)


def erp_status_key(technical_status: str | None, erp_last_error: str | None = None) -> str:
    normalized = str(technical_status or "").strip().lower()
    if normalized == "erp_error":
        if _is_rejected_erp_error(erp_last_error):
            return "rejeitado"
        return "reenvio_necessario"
    return ERP_STATUS_TECHNICAL_MAP.get(normalized, "nao_enviado")


def erp_status_items() -> List[Dict[str, str]]:
    return list(ERP_STATUS_GROUP)


def _erp_status_message(status_key: str) -> str:
    if status_key == "aceito":
        return success_message("erp_accepted", "ERP aceitou a ordem.")
    if status_key == "rejeitado":
        return error_message("erp_rejected", error_message("erp_order_rejected"))
    if status_key == "reenvio_necessario":
        return error_message("erp_unavailable", error_message("erp_temporarily_unavailable"))
    if status_key == "enviado":
        return success_message("order_sent_to_erp", "Ordem enviada ao ERP.")
    return get_ui_text("erp.status.nao_enviado.message", "A ordem ainda nao foi enviada ao ERP.")


def erp_status_payload(
    technical_status: str | None,
    erp_last_error: str | None = None,
    *,
    last_updated_at: str | None = None,
) -> Dict[str, str | None]:
    key = erp_status_key(technical_status, erp_last_error=erp_last_error)
    status_meta = next((item for item in ERP_STATUS_GROUP if item.get("key") == key), None) or {
        "key": key,
        "label": key,
        "description": "",
    }
    payload: Dict[str, str | None] = {
        "key": key,
        "label": status_meta.get("label") or key,
        "description": status_meta.get("description") or "",
        "message": _erp_status_message(key),
        "last_updated_at": last_updated_at,
    }
    return payload


def frontend_bundle() -> Dict[str, object]:
    return {
        "terms": FRIENDLY_TERMS,
        "status_labels": STATUS_LABELS,
        "status_descriptions": STATUS_DESCRIPTIONS,
        "erp_statuses": ERP_STATUS_GROUP,
        "erp_status_labels": {item["key"]: item["label"] for item in ERP_STATUS_GROUP},
        "erp_timeline_event_labels": ERP_TIMELINE_EVENT_LABELS,
        "entity_labels": ENTITY_LABELS,
        "scope_labels": SCOPE_LABELS,
        "type_labels": TYPE_LABELS,
        "messages": MESSAGES,
        "flow": flow_frontend_bundle(),
    }


def template_bundle() -> Dict[str, object]:
    return {
        "ui_terms": FRIENDLY_TERMS,
        "ui_status_groups": STATUS_GROUPS,
        "ui_status_labels": STATUS_LABELS,
        "ui_status_descriptions": STATUS_DESCRIPTIONS,
        "ui_erp_statuses": ERP_STATUS_GROUP,
        "ui_erp_timeline_event_labels": ERP_TIMELINE_EVENT_LABELS,
        "ui_messages": MESSAGES,
        "ui_frontend_bundle": frontend_bundle(),
        "ui_texts": UI_TEXTS,
    }
