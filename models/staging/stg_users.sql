select
  id::int as user_id,
  name as user_name,
  email,
  modified::timestamp as modified_at
from {{ source('pipedrive','users') }}

