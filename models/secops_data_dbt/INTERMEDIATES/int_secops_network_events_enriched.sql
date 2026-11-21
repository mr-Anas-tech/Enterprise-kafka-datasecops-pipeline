with deduplicated as (
    select
    *,
    row_number() over(
        partition by event_id
        order by event_timestamp desc,  kafka_offset  desc
    ) as row_num
    from {{ ref("stg_secops_network_logs")}}
), 

enriched as(
    select
    event_id,
    user_id,
    source_ip,
    country_code,
    country_name,
    http_method,
    endpoint,
    case
    when endpoint like '/admin%' then 'privileged_admin'
    when endpoint in('/api/v1/login' , '/api/v1/token') then 'authentication'
    when endpoint = '/api/v1/checkout' then 'transaction_checkout'
    when endpoint like '/api/v1/products%' then 'catalog_browse'
    else 'other_endpoint'
    end as endpoint_category,
    case 
    when endpoint like '/admin%' or endpoint in('/api/v1/login' , '/api/v1/token') then true
    else false
    end as is_sensitive_endpoint, 
    case 
    when endpoint like '/admin%' then true else false end as is_admin_endpoint_hit,
    status_code,
    Case 
    when status_code like '1%' then 'informational_1xx'
    when status_code like '2%' then 'success_2xx'
    when status_code like '3%' then 'redirect_3xx'
    when status_code like '4%' then 'client_error_4xx'
    when status_code like '5%' then  'server_error_5xx'
    else 'other_status'
    end as status_code_category,
    response_time_ms,
    case when endpoint = '/api/v1/login' and status_code = '401' then true
    else false
    end as is_failed_login,
    case 
    when response_time_ms < 200 then 'fast'
    when response_time_ms between 200 AND 1000 then 'moderate'
    else 'slow_anomaly'
    end as latency_tier,
    case 
    when response_time_ms > 1000 then true
    else false
    end as is_slow_response,
    bytes_transferred,
    cast(event_timestamp as date) event_date,
    date_trunc('hour', event_timestamp) as event_hour,
    date_trunc('minute', event_timestamp) as event_minute,
    event_timestamp,
    kafka_ingested_at_utc,
    kafka_offset,
    kafka_partition,
    loaded_at
    from deduplicated
    where row_num = 1
)


select * from enriched



