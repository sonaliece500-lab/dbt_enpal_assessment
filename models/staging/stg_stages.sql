select
  stage_id::int as stage_id,
  stage_name
from {{ source('pipedrive','stages') }}

