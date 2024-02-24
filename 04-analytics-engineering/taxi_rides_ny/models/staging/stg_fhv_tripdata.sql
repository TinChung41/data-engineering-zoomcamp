{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_tripdata') }}
  where dispatching_base_num is not null 
  and extract(year from cast(pickup_datetime as timestamp)) = 2019
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropOff_datetime,
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as PUlocationID,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as DOlocationID,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as SR_Flag,
    Affiliated_base_number
from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}