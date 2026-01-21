with a as (
  select
    deal_id,
    due_to,
    is_done,
    activity_type
  from {{ ref('stg_activity') }}
  where deal_id is not null
),
calls as (
  select
    deal_id,
    due_to as call_at
  from a
  where lower(activity_type) like '%call%'
),
ranked as (
  select
    deal_id,
    call_at,
    row_number() over (partition by deal_id order by call_at) as call_number
  from calls
)
select
  deal_id,
  call_number,
  call_at
from ranked
where call_number in (1,2)
