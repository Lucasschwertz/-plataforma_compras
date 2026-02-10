(function () {
  const BUNDLE = window.UI_STRINGS || {};
  const FLOW_BUNDLE = BUNDLE.flow || {};

  const DEFAULT_STATUS_LABELS = {
    pending_rfq: "Pendente de cotacao",
    in_rfq: "Em cotacao",
    awarded: "Aprovada",
    ordered: "Ordenada",
    partially_received: "Recebimento parcial",
    received: "Recebida",
    cancelled: "Cancelada",
    draft: "Rascunho",
    open: "Aberta",
    collecting_quotes: "Coletando cotacoes",
    closed: "Encerrada",
    approved: "Aprovada",
    sent_to_erp: "Enviada ao ERP",
    erp_accepted: "Aceita no ERP",
    erp_error: "Erro no ERP",
    converted_to_po: "Convertida em OC",
    running: "Em execucao",
    queued: "Na fila",
    succeeded: "Sucesso",
    failed: "Falha",
    submitted: "Proposta enviada",
    nao_enviado: "Nao enviado",
    enviado: "Enviado",
    aceito: "Aceito",
    rejeitado: "Rejeitado",
    reenvio_necessario: "Reenvio necessario",
  };

  const DEFAULT_ENTITY_LABELS = {
    purchase_request: "Solicitacao",
    purchase_order: "Ordem de compra",
    rfq: "Cotacao",
    award: "Decisao",
    supplier: "Fornecedor",
    receipt: "Recebimento",
  };

  const DEFAULT_SCOPE_LABELS = {
    purchase_order: "Ordens de compra",
    purchase_request: "Solicitacoes",
    rfq: "Cotacoes",
    supplier: "Fornecedores",
    receipt: "Recebimentos",
  };

  const DEFAULT_TYPE_LABELS = {
    purchase_request: "Solicitacao",
    purchase_order: "Ordem de compra",
    rfq: "Cotacao",
    award: "Decisao",
    supplier: "Fornecedor",
    receipt: "Recebimento",
  };

  const STATUS_LABELS = Object.assign({}, DEFAULT_STATUS_LABELS, BUNDLE.status_labels || {});
  const ERP_STATUS_LABELS = Object.assign({}, BUNDLE.erp_status_labels || {});
  const ENTITY_LABELS = Object.assign({}, DEFAULT_ENTITY_LABELS, BUNDLE.entity_labels || {});
  const SCOPE_LABELS = Object.assign({}, DEFAULT_SCOPE_LABELS, BUNDLE.scope_labels || {});
  const TYPE_LABELS = Object.assign({}, DEFAULT_TYPE_LABELS, BUNDLE.type_labels || {});
  const STATUS_DESCRIPTIONS = Object.assign({}, BUNDLE.status_descriptions || {});
  const FLOW_POLICY = Object.assign({}, FLOW_BUNDLE.policy || {});
  const FLOW_STAGES = Array.isArray(FLOW_BUNDLE.stages) ? FLOW_BUNDLE.stages : [];
  const ACTION_LABELS = Object.assign({}, FLOW_BUNDLE.action_labels || {});

  function prettyStatus(value) {
    return STATUS_LABELS[value] || value || "Nao informado";
  }

  function prettyErpStatus(value) {
    return ERP_STATUS_LABELS[value] || STATUS_LABELS[value] || value || "Nao informado";
  }

  function prettyEntity(value) {
    return ENTITY_LABELS[value] || value || "Nao informado";
  }

  function prettyScope(value) {
    return SCOPE_LABELS[value] || value || "Nao informado";
  }

  function prettyType(value) {
    return TYPE_LABELS[value] || value || "Nao informado";
  }

  function statusDescription(value) {
    return STATUS_DESCRIPTIONS[value] || "";
  }

  function allowedActions(stage, status) {
    if (!stage || !status) return [];
    const stagePolicy = FLOW_POLICY[stage] || {};
    const statusPolicy = stagePolicy[status] || {};
    const actions = statusPolicy.allowed_actions;
    return Array.isArray(actions) ? actions : [];
  }

  function primaryAction(stage, status) {
    if (!stage || !status) return null;
    const stagePolicy = FLOW_POLICY[stage] || {};
    const statusPolicy = stagePolicy[status] || {};
    const action = statusPolicy.primary_action;
    return action || null;
  }

  function isActionAllowed(stage, status, action) {
    if (!action) return false;
    return allowedActions(stage, status).includes(action);
  }

  function actionLabel(action) {
    return ACTION_LABELS[action] || action || "";
  }

  window.UiLabels = {
    STATUS_LABELS,
    ENTITY_LABELS,
    SCOPE_LABELS,
    TYPE_LABELS,
    FLOW_POLICY,
    FLOW_STAGES,
    ACTION_LABELS,
    ERP_STATUS_LABELS,
    prettyStatus,
    prettyErpStatus,
    prettyEntity,
    prettyScope,
    prettyType,
    statusDescription,
    allowedActions,
    primaryAction,
    isActionAllowed,
    actionLabel,
  };
})();
