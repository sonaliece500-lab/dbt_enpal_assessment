select
  activity_id::int as activity_id,
  deal_id::int as deal_id,
  type as activity_type,
  assigned_to_user::int as assigned_to_user_id,
  done as is_done,
  due_to::timestamp as due_to
from {{ source('pipedrive','activity') }}

