{{ config(materialized='table') }}

with source as (
    select * from {{ source('hoover', 'COST_REPORT_ACK_MID_TABLE') }}
),

f_billing_hourly as (
    select
        coalesce(partner.value:network_id, -1)                                                                                    as network_id
        , coalesce(partner.value:content_owner_network_id, -1)                                                                   as content_owner_id
        , coalesce(partner.value:distributor_network_id, -1)                                                                     as distributor_id
        , coalesce(partner.value:reseller_network_id, -1)                                                                        as reseller_id
        , coalesce(partner.value:role, '')                                                                                       as transaction_type
        , coalesce(ack:traffic_type, 0)                                                                                    as traffic_type
        , IFF(partner.value:network_is_ad_owner = true, coalesce(advertisement:placement_id, -1), -1)                                    as placement_id
        , IFF(partner.value:network_is_ad_owner = true, coalesce(advertisement:ad_id, -1), -1)                                           as ad_id
        , coalesce(advertisement:creative_id, -1)                                                                          as creative_id
        , coalesce(ack:creative_rendition_id, -1)                                                                          as rendition_id
        , IFF(partner.value:network_is_extra_item_owner = true, coalesce(advertisement:ad_unit_id, -1), -1)                              as ad_unit_id
        , CASE
              WHEN advertisement:is_bumper and advertisement:is_owned_by_cro THEN 'BUMPER'
              WHEN advertisement:is_external and partner.value:network_is_ad_owner THEN 'EXTERNAL_AD'
              WHEN ack:is_embedded_tracking_ad_event and partner.value:network_is_ad_owner THEN 'TRACKING_AD'
              WHEN ack:is_tracking_url_event and partner.value:network_is_ad_owner THEN 'TRACKING_URL'
              ELSE 'NORMAL'
        END                                                                                                              as ad_type
        , coalesce(slot:time_position_class, 'Unknown')                                                                    as time_position_class
        , coalesce(partner.value:rule_id, -1)                                                                                    as outbound_mrm_rule_id
        , coalesce(partner.value:inbound_rule_id, -1)                                                                            as inbound_mrm_rule_id
        , coalesce(partner.value:bit_flags, 0) + coalesce(advertisement:bit_flags, 0) + coalesce(request:bit_flags, 0)           as bit_flag
        , sum(coalesce(ack:metrics.ad_impression, 0))                                                                      as ad_views
        , sum(coalesce(ack:metrics.click, 0))                                                                              as clicks
        , sum(IFF(partner.value:network_is_ad_owner = true, coalesce(ack:metrics.no_ad_impression, 0), 0))                               as no_ad_views
        , sum(coalesce(partner.value:revenue, 0.0) * coalesce(ack:metrics.fire_event_revenue_ratio, 0))                          as revenue
        , sum(coalesce(partner.value:reseller_revenue, 0.0) * coalesce(ack:metrics.fire_event_revenue_ratio, 0))                 as reseller_revenue
        , sum(coalesce(partner.value:content_owner_revenue, 0.0) * coalesce(ack:metrics.fire_event_revenue_ratio, 0))            as content_owner_fee
        , sum(coalesce(partner.value:distributor_revenue, 0.0) * coalesce(ack:metrics.fire_event_revenue_ratio, 0))              as distributor_fee
        , '20231224010000'                                                                                           as process_batch_id
        , date_trunc('HOUR', cast(ack:timestamp as TIMESTAMP))                                                             as event_date
    from source
            ,lateral flatten(input => partners) as partner
    where coalesce(partner.value:role, '') in ('CRO', 'R', 'D')
      and (ack:is_private_impression = false or partner.value:network_is_ad_owner = true or partner.value:network_is_extra_item_owner = true)
      and ack:ack_entity_type = 'ad'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 24, 25
)

select * from f_billing_hourly