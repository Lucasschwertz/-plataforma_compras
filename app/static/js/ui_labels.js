(function () {
  const STATUS_LABELS = {
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
    succeeded: "Sucesso",
    failed: "Falha",
    submitted: "Proposta enviada",
  };

  const ENTITY_LABELS = {
    purchase_request: "Solicitacao",
    purchase_order: "Ordem de compra",
    rfq: "Cotacao",
    award: "Decisao",
    supplier: "Fornecedor",
    receipt: "Recebimento",
  };

  const SCOPE_LABELS = {
    purchase_order: "Ordens de compra",
    purchase_request: "Solicitacoes",
    rfq: "Cotacoes",
    supplier: "Fornecedores",
    receipt: "Recebimentos",
  };

  const TYPE_LABELS = {
    purchase_request: "Solicitacao",
    purchase_order: "Ordem de compra",
    rfq: "Cotacao",
    award: "Decisao",
    supplier: "Fornecedor",
    receipt: "Recebimento",
  };

  function prettyStatus(value) {
    return STATUS_LABELS[value] || value || "n/a";
  }

  function prettyEntity(value) {
    return ENTITY_LABELS[value] || value || "n/a";
  }

  function prettyScope(value) {
    return SCOPE_LABELS[value] || value || "n/a";
  }

  function prettyType(value) {
    return TYPE_LABELS[value] || value || "n/a";
  }

  window.UiLabels = {
    STATUS_LABELS,
    ENTITY_LABELS,
    SCOPE_LABELS,
    TYPE_LABELS,
    prettyStatus,
    prettyEntity,
    prettyScope,
    prettyType,
  };
})();
