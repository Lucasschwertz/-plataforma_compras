-- Optional seed data for the procurement inbox prototype (SQLite).
-- Run manually if you want to see non-empty results.

INSERT INTO purchase_requests (number, status, priority, requested_by, department, needed_at, company_id)
VALUES
  ('SR-1001', 'pending_rfq', 'high', 'Joao', 'Manutencao', date('now', '+3 day'), 1),
  ('SR-1002', 'in_rfq', 'urgent', 'Maria', 'Operacoes', date('now', '+1 day'), 1);

INSERT INTO rfqs (title, status, company_id)
VALUES
  ('RFQ - Rolamentos', 'collecting_quotes', 1),
  ('RFQ - EPIs', 'awarded', 1);

INSERT INTO purchase_orders (number, status, company_id, erp_last_error)
VALUES
  ('OC-2001', 'draft', 1, NULL),
  ('OC-2002', 'erp_error', 1, 'Fornecedor sem codigo no ERP');