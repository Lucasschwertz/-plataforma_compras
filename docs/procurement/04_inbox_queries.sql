-- Inbox queries (PostgreSQL) alinhadas ao schema enterprise com PK composta.
-- Parâmetros esperados:
--   $1 = tenant_id (uuid)
--   $2 = limit (int)
--   $3 = offset (int)

-- 1) Cards / contadores
with base_pr as (
  select tenant_id, id, status
  from purchase_requests
  where tenant_id = $1
),
base_po as (
  select tenant_id, id, status
  from purchase_orders
  where tenant_id = $1
)
select
  (select count(*) from base_pr where status = 'pending_rfq') as pending_rfq,
  (
    select count(*)
    from rfqs r
    where r.tenant_id = $1
      and r.status in ('open','collecting_quotes')
  ) as awaiting_quotes,
  (
    select count(*)
    from rfqs r
    where r.tenant_id = $1
      and r.status = 'awarded'
  ) as awarded_waiting_po,
  (
    select count(*)
    from base_po
    where status in ('draft','approved','erp_error')
  ) as awaiting_erp_push;

-- 2) Lista unificada da inbox
with pr_pending as (
  select
    pr.tenant_id,
    pr.id,
    'purchase_request'::text as type,
    pr.number as ref,
    pr.status::text as status,
    pr.priority::text as priority,
    pr.needed_at,
    pr.updated_at,
    greatest(0, extract(day from now() - pr.created_at))::int as age_days
  from purchase_requests pr
  where pr.tenant_id = $1
    and pr.status in ('pending_rfq','in_rfq')
),
rfq_open as (
  select
    r.tenant_id,
    r.id,
    'rfq'::text as type,
    coalesce(r.title, r.id::text) as ref,
    r.status::text as status,
    null::text as priority,
    null::date as needed_at,
    r.updated_at,
    greatest(0, extract(day from now() - r.created_at))::int as age_days
  from rfqs r
  where r.tenant_id = $1
    and r.status in ('open','collecting_quotes','awarded')
),
po_pending_push as (
  select
    po.tenant_id,
    po.id,
    'purchase_order'::text as type,
    coalesce(po.number, po.id::text) as ref,
    po.status::text as status,
    null::text as priority,
    null::date as needed_at,
    po.updated_at,
    greatest(0, extract(day from now() - po.created_at))::int as age_days
  from purchase_orders po
  where po.tenant_id = $1
    and po.status in ('draft','approved','erp_error')
)
select *
from (
  select * from pr_pending
  union all
  select * from rfq_open
  union all
  select * from po_pending_push
) inbox
order by
  case priority
    when 'urgent' then 1
    when 'high' then 2
    when 'medium' then 3
    when 'low' then 4
    else 5
  end,
  needed_at nulls last,
  updated_at desc
limit $2 offset $3;