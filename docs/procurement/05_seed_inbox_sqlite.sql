-- Optional seed data for the procurement inbox prototype (SQLite).
-- Run manually if you want to see non-empty results.

INSERT INTO purchase_requests (number, status, priority, requested_by, department, needed_at, tenant_id)
VALUES
  ('SR-1001', 'pending_rfq', 'high', 'Joao', 'Manutencao', date('now', '+3 day'), 'tenant-1'),
  ('SR-1002', 'in_rfq', 'urgent', 'Maria', 'Operacoes', date('now', '+1 day'), 'tenant-1');

INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
SELECT id, 1, 'Rolamento 6202', 10, 'UN', tenant_id
FROM purchase_requests
WHERE number = 'SR-1001' AND tenant_id = 'tenant-1';

INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
SELECT id, 2, 'Correia dentada', 4, 'UN', tenant_id
FROM purchase_requests
WHERE number = 'SR-1001' AND tenant_id = 'tenant-1';

INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
SELECT id, 1, 'Luva nitrilica', 200, 'UN', tenant_id
FROM purchase_requests
WHERE number = 'SR-1002' AND tenant_id = 'tenant-1';

INSERT INTO purchase_request_items (purchase_request_id, line_no, description, quantity, uom, tenant_id)
SELECT id, 2, 'Mascara PFF2', 150, 'UN', tenant_id
FROM purchase_requests
WHERE number = 'SR-1002' AND tenant_id = 'tenant-1';

INSERT INTO rfqs (title, status, tenant_id)
VALUES
  ('RFQ - Rolamentos', 'collecting_quotes', 'tenant-1'),
  ('RFQ - EPIs', 'awarded', 'tenant-1');

INSERT INTO purchase_orders (number, status, tenant_id, erp_last_error)
VALUES
  ('OC-2001', 'draft', 'tenant-1', NULL),
  ('OC-2002', 'erp_error', 'tenant-1', 'Fornecedor sem codigo no ERP');
