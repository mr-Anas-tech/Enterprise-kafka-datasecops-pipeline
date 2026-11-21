with source_data as(
    select RAW_PAYLOAD,
    LOADED_AT
    from {{ source('secops_raw', 'secops_network_logs_raw') }}
    ),

staged AS(
    select
    RAW_PAYLOAD:event_id::VARCHAR AS event_id,
    RAW_PAYLOAD:user_id::VARCHAR as user_id,
    RAW_PAYLOAD:source_ip::VARCHAR AS source_ip,
    RAW_PAYLOAD:country_code::VARCHAR  AS country_code,
    RAW_PAYLOAD:country_name::VARCHAR AS country_name,
    upper(RAW_PAYLOAD:http_method::VARCHAR) as http_method,
    RAW_PAYLOAD:endpoint::VARCHAR AS endpoint,
    RAW_PAYLOAD:status_code::VARCHAR as status_code,
    COALESCE(RAW_PAYLOAD:response_time_ms::INTEGER, 0) as  response_time_ms,
    COALESCE(RAW_PAYLOAD:bytes_transferred::INTEGER, 0) AS bytes_transferred,
    TRY_TO_TIMESTAMP_NTZ(RAW_PAYLOAD:event_timestamp::VARCHAR) AS event_timestamp,
    TRY_TO_TIMESTAMP_NTZ(RAW_PAYLOAD:_kafka_ingested_at_utc::VARCHAR) AS kafka_ingested_at_utc,
    RAW_PAYLOAD:_kafka_offset::INTEGER AS kafka_offset,
    RAW_PAYLOAD:_kafka_partition::INTEGER AS kafka_partition,
    LOADED_AT::TIMESTAMP_NTZ AS loaded_at 
    from source_data
)


select * from staged