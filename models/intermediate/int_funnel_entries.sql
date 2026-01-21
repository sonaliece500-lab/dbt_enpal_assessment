with stages as (
  select stage_id, stage_name
  from {{ ref('stg_stages') }}
),

entries as (
  select
    e.deal_id,
    e.stage_id,
    e.entered_at,
    s.stage_name
  from {{ ref('int_stage_entries') }} e
  left join stages s using(stage_id)
),

mapped as (
  select
    deal_id,
    entered_at,
    case
      when lower(stage_name) like '%lead%' then 1
      when lower(stage_name) like '%qual%' then 2
      when lower(stage_name) like '%need%' then 3
      when lower(stage_name) like '%assess%' then 3
      when lower(stage_name) like '%proposal%' then 4
      when lower(stage_name) like '%quote%' then 4
      when lower(stage_name) like '%nego%' then 5
      when lower(stage_name) like '%clos%' then 6
      when lower(stage_name) like '%implement%' then 7
      when lower(stage_name) like '%onboard%' then 7
      when lower(stage_name) like '%follow%' then 8
      when lower(stage_name) like '%success%' then 8
      when lower(stage_name) like '%renew%' then 9
      when lower(stage_name) like '%expan%' then 9
      else null
    end as funnel_step
  from entries
),

fallback_step2 as (
  select
    deal_id,
    min(entered_at) as entered_at,
    2 as funnel_step
  from mapped
  where funnel_step is not null
    and funnel_step <> 1
  group by 1
),

unioned as (
  select deal_id, entered_at, funnel_step
  from mapped
  where funnel_step is not null

  union all

  select deal_id, entered_at, funnel_step
  from fallback_step2
)

select
  deal_id,
  entered_at,
  funnel_step
from unioned


