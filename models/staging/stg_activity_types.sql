select
  id::int as activity_type_id,
  name as activity_type_name,
  type as activity_type_code,
  active
from {{ source('pipedrive','activity_types') }}

