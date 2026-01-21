select
  deal_id::int as deal_id,
  change_time::timestamp as change_time,
  changed_field_key,
  new_value
from {{ source('pipedrive','deal_changes') }}

