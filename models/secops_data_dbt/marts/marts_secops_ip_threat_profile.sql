with enriched_events as(
    select *  from 
    {{ ref('int_secops_network_events_enriched') }}
),

ip_profile as (
    select
      source_ip,
      country_name,
      country_code,
      min(event_timestamp) as first_event_seen_at,
      max(event_timestamp) as last_event_seen_at,
      count(*) as total_requests,
      count(Distinct endpoint) as distinct_endpint_hit,
      count(endpoint_category) as distinct_endpint_category_hit,
      count_if(status_code_category = 'informational_1xx') as informational_1xx_count,
      count_if(status_code_category = 'success_2xx') as success_2xx_count,
      count_if(status_code_category = 'redirect_3xx') as redirect_3xx_count,
      count_if(status_code_category = 'client_error_4xx') as client_error_4xx_count,
      count_if(status_code_category = 'server_error_5xx') as server_error_5xx_count,
      count_if(is_failed_login) as failed_login_attempts_count,
      count_if(is_sensitive_endpoint) as sensitive_endpoint_count,
      count_if(is_admin_endpoint_hit) as admin_endpoint_hits,
      count_if(is_slow_response) as slow_response_count,
      round(avg(response_time_ms), 3) as avg_response_time_ms
    from enriched_events
    group by 
    source_ip,
    country_name,
    country_code
)


select * from 
ip_profile 
order by
failed_login_attempts_count desc,
sensitive_endpoint_count desc,
total_requests desc
