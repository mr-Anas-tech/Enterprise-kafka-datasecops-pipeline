with enriched_events as(
    select  
    * 
    from 
    {{ ref('int_secops_network_events_enriched') }}
),

hourly_summary as(
    select 
      event_date,
      event_hour,
      count(*) as total_events,
      count( Distinct source_ip) as unique_source_ips,
      count_if(status_code_category = 'success_2xx') as success_2xx_count,
      count_if(status_code_category = 'redirect_3xx') as redirect_3xx_count,
      count_if(status_code_category = 'client_error_4xx') as client_error_4xx_count,
      count_if(status_code_category = 'server_error_5xx') as server_error_5xx_count,
      count_if(is_failed_login) as failed_login_attempts_count,
      count_if(is_sensitive_endpoint) as sensitive_endpoint_count,
      count_if(is_slow_response) as slow_response_count,
      round(avg(response_time_ms),3) as avg_response_time_ms,
      round(max(response_time_ms), 3) as max_response_time_ms
      from enriched_events
      group by
      event_date,
      event_hour
)

select * from 
hourly_summary
order by
event_date desc,
event_hour desc