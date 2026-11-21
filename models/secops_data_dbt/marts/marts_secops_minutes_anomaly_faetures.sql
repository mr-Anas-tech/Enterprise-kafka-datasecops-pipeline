with enriched_events as (

    select *
    from {{ ref('int_secops_network_events_enriched') }}

),

minute_agg as (

    select
        date_trunc('minute', event_timestamp) as minute_ts,
        event_date,
        event_hour,
        event_minute,

        -- Volume & Diversity
        count(*) as total_events,
        count(distinct source_ip) as unique_source_ips,
        count(distinct endpoint) as unique_endpoints,
        round(count(*) / nullif(count(distinct source_ip), 0), 2) as avg_requests_per_ip,

        -- Security & Error Counts
        count_if(status_code_category = '4xx_client_error') as error_4xx_count,
        count_if(status_code_category = '5xx_server_error') as error_5xx_count,
        count_if(is_failed_login) as failed_login_count,
        count_if(is_sensitive_endpoint) as sensitive_endpoint_hits,
        count_if(is_admin_endpoint_hit) as admin_endpoint_hits,
        count_if(is_slow_response) as slow_response_count,

        -- Normalized Ratios (ML models ke liye ideal)
        round(count_if(status_code_category = 'client_error_4xx') / nullif(count(*), 0), 4) as error_4xx_rate,
        round(count_if(status_code_category = 'server_error_5xx') / nullif(count(*), 0), 4) as error_5xx_rate,
        round(count_if(is_failed_login) / nullif(count(*), 0), 4) as failed_login_rate,
        round(count_if(is_sensitive_endpoint) / nullif(count(*), 0), 4) as sensitive_endpoint_rate,
        round(count_if(is_slow_response) / nullif(count(*), 0), 4) as slow_response_rate,

        -- Latency Metrics
        round(avg(response_time_ms), 2) as avg_response_time_ms,
        round(approx_percentile(response_time_ms, 0.95), 2) as p95_response_time_ms,
        max(response_time_ms) as max_response_time_ms

    from enriched_events
    group by
        date_trunc('minute', event_timestamp),
        event_date,
        event_hour,
        event_minute

),

with_baselines_and_context as (

    select
        *,

        -- Hourly Context 
        round(
            total_events / nullif(sum(total_events) over (partition by event_hour), 0),
            4
        ) as pct_of_hourly_traffic,

        round(
            failed_login_count / nullif(sum(failed_login_count) over (partition by event_hour), 0),
            4
        ) as pct_of_hourly_failed_logins,

        -- Continuous 15-Minute Rolling Baselines
        round(
            avg(total_events) over (
                order by minute_ts
                rows between 15 preceding and 1 preceding
            ), 2
        ) as rolling_15m_avg_events,

        round(
            stddev(total_events) over (
                order by minute_ts
                rows between 15 preceding and 1 preceding
            ), 2
        ) as rolling_15m_stddev_events,

        round(
            avg(avg_response_time_ms) over (
                order by minute_ts
                rows between 15 preceding and 1 preceding
            ), 2
        ) as rolling_15m_avg_latency_ms

    from minute_agg

)

select
    *,

    -- Final ML Anomaly Features (Spike Ratio, Z-Score & Latency Jump)
    round(
        total_events / nullif(rolling_15m_avg_events, 0),
        2
    ) as traffic_spike_ratio,

    round(
        coalesce(
            (total_events - rolling_15m_avg_events) / nullif(rolling_15m_stddev_events, 0),
            0
        ),
        2
    ) as traffic_z_score,

    round(
        avg_response_time_ms / nullif(rolling_15m_avg_latency_ms, 0),
        2
    ) as latency_spike_ratio

from with_baselines_and_context
where rolling_15m_avg_events is not null
order by minute_ts desc