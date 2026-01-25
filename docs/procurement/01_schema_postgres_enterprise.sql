-- Enterprise procurement schema (PostgreSQL) with strong multi-tenant integrity
-- Includes: composite PKs, enums for entity/scope/status, updated_at trigger,
-- integration watermarks, sync audit, and inbox-ready structures.

create extension if not exists pgcrypto;

-- updated_at automático
create or replace function set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

-- Enums base
-- Padronizacao: a mesma lista fechada deve ser usada em status_events.entity e sync_runs.scope.

do $$ begin
  create type integration_entity as enum (
    'purchase_request',
    'rfq',
    'award',
    'purchase_order',
    'receipt',
    'supplier',
    'category'
  );
exception when duplicate_object then null; end $$;

-- Status enums (Fase 1)

do $$ begin
  create type pr_status as enum (
    'pending_rfq',
    'in_rfq',
    'awarded',
    'ordered',
    'partially_received',
    'received',
    'cancelled'
  );
exception when duplicate_object then null; end $$;

do $$ begin
  create type rfq_status as enum (
    'draft',
    'open',
    'collecting_quotes',
    'closed',
    'awarded',
    'cancelled'
  );
exception when duplicate_object then null; end $$;

do $$ begin
  create type award_status as enum (
    'awarded',
    'converted_to_po',
    'cancelled'
  );
exception when duplicate_object then null; end $$;

do $$ begin
  create type po_status as enum (
    'draft',
    'approved',
    'sent_to_erp',
    'erp_accepted',
    'partially_received',
    'received',
    'cancelled',
    'erp_error'
  );
exception when duplicate_object then null; end $$;

do $$ begin
  create type priority_level as enum (
    'low',
    'medium',
    'high',
    'urgent'
  );
exception when duplicate_object then null; end $$;

-- Base

create table if not exists tenants (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  created_at timestamptz not null default now()
);

create table if not exists users (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  email text not null,
  full_name text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, email)
);

create table if not exists roles (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  name text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, name)
);

create table if not exists user_roles (
  tenant_id uuid not null,
  user_id uuid not null,
  role_id uuid not null,
  created_at timestamptz not null default now(),
  primary key (tenant_id, user_id, role_id),
  foreign key (tenant_id, user_id) references users(tenant_id, id),
  foreign key (tenant_id, role_id) references roles(tenant_id, id)
);

-- Domínio

create table if not exists suppliers (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  external_id text,
  name text not null,
  tax_id text,
  risk_flags jsonb not null default '{"no_supplier_response": false, "late_delivery": false, "sla_breach": false}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, external_id)
);

create table if not exists categories (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  code text,
  name text not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, code)
);

create table if not exists purchase_requests (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  external_id text,
  number text,
  status pr_status not null,
  priority priority_level not null default 'medium',
  requested_by text,
  department text,
  needed_at date,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, external_id)
);

create table if not exists purchase_request_items (
  tenant_id uuid not null,
  id uuid not null default gen_random_uuid(),
  purchase_request_id uuid not null,
  external_id text,
  line_no int,
  category_id uuid,
  sku text,
  description text not null,
  uom text,
  quantity numeric(18,4) not null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, external_id),
  foreign key (tenant_id, purchase_request_id) references purchase_requests(tenant_id, id),
  foreign key (tenant_id, category_id) references categories(tenant_id, id)
);

create table if not exists rfqs (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  status rfq_status not null,
  title text,
  cancel_reason text,
  created_by uuid,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  foreign key (tenant_id, created_by) references users(tenant_id, id)
);

create table if not exists rfq_items (
  tenant_id uuid not null,
  id uuid not null default gen_random_uuid(),
  rfq_id uuid not null,
  purchase_request_item_id uuid,
  description text not null,
  uom text,
  quantity numeric(18,4) not null,
  category_id uuid,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  foreign key (tenant_id, rfq_id) references rfqs(tenant_id, id),
  foreign key (tenant_id, purchase_request_item_id) references purchase_request_items(tenant_id, id),
  foreign key (tenant_id, category_id) references categories(tenant_id, id)
);

create table if not exists rfq_supplier_invites (
  tenant_id uuid not null,
  id uuid not null default gen_random_uuid(),
  rfq_id uuid not null,
  supplier_id uuid not null,
  status text not null default 'invited',
  invited_at timestamptz not null default now(),
  responded_at timestamptz,
  primary key (tenant_id, id),
  unique (tenant_id, rfq_id, supplier_id),
  foreign key (tenant_id, rfq_id) references rfqs(tenant_id, id),
  foreign key (tenant_id, supplier_id) references suppliers(tenant_id, id)
);

create table if not exists quotes (
  tenant_id uuid not null,
  id uuid not null default gen_random_uuid(),
  rfq_id uuid not null,
  supplier_id uuid not null,
  status text not null default 'submitted',
  currency text not null default 'BRL',
  valid_until date,
  submitted_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, rfq_id, supplier_id),
  foreign key (tenant_id, rfq_id) references rfqs(tenant_id, id),
  foreign key (tenant_id, supplier_id) references suppliers(tenant_id, id)
);

create table if not exists quote_items (
  tenant_id uuid not null,
  id uuid not null default gen_random_uuid(),
  quote_id uuid not null,
  rfq_item_id uuid not null,
  unit_price numeric(18,6) not null,
  quantity numeric(18,4) not null,
  tax_rate numeric(9,6),
  lead_time_days int,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, quote_id, rfq_item_id),
  foreign key (tenant_id, quote_id) references quotes(tenant_id, id),
  foreign key (tenant_id, rfq_item_id) references rfq_items(tenant_id, id)
);

create table if not exists awards (
  tenant_id uuid not null,
  id uuid not null default gen_random_uuid(),
  rfq_id uuid not null,
  supplier_id uuid not null,
  status award_status not null default 'awarded',
  reason text not null,
  decided_by uuid,
  decided_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, rfq_id),
  foreign key (tenant_id, rfq_id) references rfqs(tenant_id, id),
  foreign key (tenant_id, supplier_id) references suppliers(tenant_id, id),
  foreign key (tenant_id, decided_by) references users(tenant_id, id)
);

create table if not exists purchase_orders (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  external_id text,
  number text,
  supplier_id uuid not null,
  award_id uuid,
  status po_status not null default 'draft',
  currency text not null default 'BRL',
  total_amount numeric(18,2),
  erp_last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, external_id),
  foreign key (tenant_id, supplier_id) references suppliers(tenant_id, id),
  foreign key (tenant_id, award_id) references awards(tenant_id, id)
);

create table if not exists purchase_order_items (
  tenant_id uuid not null,
  id uuid not null default gen_random_uuid(),
  purchase_order_id uuid not null,
  line_no int not null,
  rfq_item_id uuid,
  description text not null,
  uom text,
  quantity numeric(18,4) not null,
  unit_price numeric(18,6) not null,
  tax_rate numeric(9,6),
  discount_amount numeric(18,2),
  total_line numeric(18,2),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, purchase_order_id, line_no),
  foreign key (tenant_id, purchase_order_id) references purchase_orders(tenant_id, id),
  foreign key (tenant_id, rfq_item_id) references rfq_items(tenant_id, id)
);

create table if not exists receipts (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  external_id text,
  purchase_order_id uuid,
  status text not null,
  received_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, external_id),
  foreign key (tenant_id, purchase_order_id) references purchase_orders(tenant_id, id)
);

-- Integração e auditoria

create table if not exists erp_links (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  system text not null default 'senior',
  entity integration_entity not null,
  external_id text not null,
  local_id uuid not null,
  last_seen_at timestamptz not null default now(),
  primary key (tenant_id, id),
  unique (tenant_id, system, entity, external_id),
  unique (tenant_id, entity, local_id)
);

create table if not exists integration_watermarks (
  tenant_id uuid not null references tenants(id),
  system text not null default 'senior',
  entity integration_entity not null,
  -- Watermark incremental baseado na fonte (ERP), nao no sync_runs.
  last_success_source_updated_at timestamptz,
  -- Desempate deterministico quando updated_at empata na fonte.
  last_success_source_id text,
  -- Cursor/token opcional quando a API da fonte suportar.
  last_success_cursor text,
  -- Momento em que o watermark acima foi persistido como sucesso.
  last_success_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  primary key (tenant_id, system, entity)
);

create table if not exists sync_runs (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  system text not null default 'senior',
  scope integration_entity not null,
  status text not null,
  attempt int not null default 1,
  parent_sync_run_id uuid,
  payload_ref text,
  payload_hash text,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  duration_ms int,
  records_in int not null default 0,
  records_upserted int not null default 0,
  records_failed int not null default 0,
  error_summary text,
  error_details jsonb,
  primary key (tenant_id, id),
  foreign key (tenant_id, parent_sync_run_id) references sync_runs(tenant_id, id)
);

create table if not exists status_events (
  tenant_id uuid not null references tenants(id),
  id uuid not null default gen_random_uuid(),
  entity integration_entity not null,
  entity_id uuid not null,
  from_status text,
  to_status text not null,
  reason text,
  actor_user_id uuid,
  occurred_at timestamptz not null default now(),
  primary key (tenant_id, id),
  foreign key (tenant_id, actor_user_id) references users(tenant_id, id)
);

-- Índices essenciais

create index if not exists idx_pr_tenant_status
  on purchase_requests (tenant_id, status);

create index if not exists idx_pr_priority
  on purchase_requests (tenant_id, priority, needed_at);

create index if not exists idx_rfq_tenant_status
  on rfqs (tenant_id, status);

create index if not exists idx_po_tenant_status
  on purchase_orders (tenant_id, status);

create index if not exists idx_events_lookup
  on status_events (tenant_id, entity, entity_id, occurred_at desc);

create index if not exists idx_sync_runs_lookup
  on sync_runs (tenant_id, scope, started_at desc);

create index if not exists idx_watermarks_lookup
  on integration_watermarks (tenant_id, system, entity);

-- Triggers updated_at

do $$ begin
  create trigger trg_users_updated_at
  before update on users
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_roles_updated_at
  before update on roles
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_suppliers_updated_at
  before update on suppliers
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_categories_updated_at
  before update on categories
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_pr_updated_at
  before update on purchase_requests
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_pr_items_updated_at
  before update on purchase_request_items
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_rfqs_updated_at
  before update on rfqs
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_rfq_items_updated_at
  before update on rfq_items
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_quotes_updated_at
  before update on quotes
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_quote_items_updated_at
  before update on quote_items
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_awards_updated_at
  before update on awards
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_pos_updated_at
  before update on purchase_orders
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_po_items_updated_at
  before update on purchase_order_items
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;

do $$ begin
  create trigger trg_receipts_updated_at
  before update on receipts
  for each row execute function set_updated_at();
exception when duplicate_object then null; end $$;