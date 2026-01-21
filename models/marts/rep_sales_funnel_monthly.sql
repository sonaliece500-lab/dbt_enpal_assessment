with stage_steps as (
  select
    date_trunc('month', entered_at)::date as month,
    funnel_step,
    deal_id
  from {{ ref('int_funnel_entries') }}
),

call_steps as (
  select
    date_trunc('month', call_at)::date as month,
    case when call_number = 1 then 21 when call_number = 2 then 31 end as funnel_step,
    deal_id
  from {{ ref('int_sales_calls') }}
),

unioned as (
  select * from stage_steps
  union all
  select * from call_steps
),

dedup as (
  select
    month,
    funnel_step,
    deal_id,
    row_number() over (partition by deal_id, funnel_step order by month) as rn
  from unioned
),

agg as (
  select
    month,
    funnel_step,
    count(distinct deal_id) as deals_count
  from dedup
  where rn = 1
  group by 1,2
),

months as (
  select distinct month from agg
),

steps as (
  select * from (values
    (1,'Lead Generation','Step 1'),
    (2,'Qualified Lead','Step 2'),
    (21,'Sales Call 1','Step 2.1'),
    (3,'Needs Assessment','Step 3'),
    (31,'Sales Call 2','Step 3.1'),
    (4,'Proposal/Quote Preparation','Step 4'),
    (5,'Negotiation','Step 5'),
    (6,'Closing','Step 6'),
    (7,'Implementation/Onboarding','Step 7'),
    (8,'Follow-up/Customer Success','Step 8'),
    (9,'Renewal/Expansion','Step 9')
  ) as t(funnel_step_num, kpi_name, funnel_step)
),

final as (
  select
    m.month,
    s.kpi_name,
    s.funnel_step,
    coalesce(a.deals_count, 0) as deals_count
  from months m
  cross join steps s
  left join agg a
    on a.month = m.month and a.funnel_step = s.funnel_step_num
)

select
  month,
  kpi_name,
  funnel_step,
  deals_count
from final
order by month, funnel_step

