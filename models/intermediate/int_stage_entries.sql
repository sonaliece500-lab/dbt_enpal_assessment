with dc as (
  select
    deal_id,
    change_time,
    changed_field_key,
    new_value
  from {{ ref('stg_deal_changes') }}
  where changed_field_key in ('stage_id','stage')
),
stage_changes as (
  select
    deal_id,
    change_time,
    nullif(new_value,'')::int as stage_id
  from dc
  where nullif(new_value,'') is not null
),
first_time_in_stage as (
  select
    deal_id,
    stage_id,
    min(change_time) as entered_at
  from stage_changes
  where stage_id is not null
  group by 1,2
)
select
  deal_id,
  stage_id,
  entered_at
from first_time_in_stage
