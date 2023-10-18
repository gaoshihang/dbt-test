{{ config(materialized='incremental') }}

with ack_source as (
    select * from {{ source('hoover', 'COST_REPORT_ACK_MID_WITH_BATCHID_TABLE') }}
),

f_order_hourly as (
SELECT process_batch_id                                                                                           as process_batch_id
            , coalesce(network.value:network_id, cast(-1 as bigint))                                                                   as network_id
            , coalesce(network.value:content_owner_network_id, cast(-1 as bigint))                                                     as content_owner_id
            , IFF(bitand(coalesce(request:extra_flags, cast(0 as bigint)), 1073741824) = 1073741824 and coalesce(network.value:role, '') = 'CRO', cast(-3 as bigint),
                coalesce(network.value:distributor_network_id, cast(-1 as bigint)))                                                     as distributor_id
            , coalesce(network.value:reseller_network_id, cast(-1 as bigint))                                                          as reseller_id
            , coalesce(request:context.tv_network_id, cast(-1 as bigint))                                                        as tv_network_id
            , coalesce(network.value:role, '')                                                                                       as transaction_type
            , coalesce(ack:traffic_type, cast(0 as bigint))                                                                      as traffic_type
            , coalesce(network.value:bit_flags, cast(0 as bigint))
      + coalesce(advertisement:bit_flags, cast(0 as bigint))
      + coalesce(request:bit_flags, cast(0 as bigint))
      + coalesce(ack:bit_flags, cast(0 as bigint))                                                                     as bit_flag
            , coalesce(network.value:asset_id, cast(-1 as bigint))                                                                     as asset_id
            , coalesce(network.value:series_id, cast(-1 as bigint))                                                                    as series_id
            , coalesce(network.value:asset_group_ids, [])                                                  as asset_group_ids
            , coalesce(network.value:site_section_id, cast(-1 as bigint))                                                              as site_section_id
            , coalesce(network.value:site_id, cast(-1 as bigint))                                                                      as site_id
            , coalesce(network.value:site_section_group_ids, [])                                           as site_section_group_ids
            , network.value:airing_id                                                                                                as airing_id
            , network.value:airing_channel_id                                                                                        as channel_id
            , coalesce(network.value:break_id, cast(-1 as bigint))                                                                     as break_id
            , coalesce(slot:time_position_class, 'Unknown')                                                                    as time_position_class
            , coalesce(network.value:inbound_rule_id, cast(-1 as bigint))                                                              as inbound_mrm_rule_id
            , coalesce(network.value:rule_id, cast(-1 as bigint))                                                                      as mrm_rule_id
            , IFF(network.value:network_is_ad_owner = true, coalesce(advertisement:campaign_id, cast(-1 as bigint)), cast(-1 as bigint))         as campaign_id
            , IFF(network.value:network_is_ad_owner = true, coalesce(advertisement:io_id, cast(-1 as bigint)), cast(-1 as bigint))               as io_id
            , IFF(network.value:network_is_ad_owner = true, coalesce(advertisement:placement_id, cast(-1 as bigint)), cast(-1 as bigint))        as placement_id
            , IFF(network.value:network_is_ad_owner = true, coalesce(advertisement:ad_id, cast(-1 as bigint)), cast(-1 as bigint))               as ad_id
            , IFF(network.value:network_is_ad_owner = true, coalesce(advertisement:creative_id, cast(-1 as bigint)), cast(-1 as bigint))         as creative_id
            , IFF(bitand(coalesce(request:extra_flags, cast(0 as bigint)), 1073741824) = 1073741824 and coalesce(network.value:role, '') = 'CRO', cast(-3 as bigint),
                IFF(network.value:network_is_ad_owner = true, coalesce(ack:creative_rendition_id, cast(-1 as bigint)), cast(-1 as bigint)))       as rendition_id
            , coalesce(advertisement:ad_delivery_method, '')                                                                                 as delivery_method
            , IFF(network.value:network_is_ad_owner = true, coalesce(advertisement:targeting_criteria_id, cast(-1 as bigint)), cast(-1 as bigint))                                                       as targeting_criteria_id
            , IFF(network.value:network_is_extra_item_owner = true, coalesce(advertisement:ad_unit_id, cast(-1 as bigint)), cast(-1 as bigint))                                                          as ad_unit_id
            , IFF(network.value:network_is_extra_item_owner = true, coalesce(advertisement:matched_audience_item_ids, []), [])                   as matched_audience_item_ids
            , IFF(network.value:network_is_extra_item_owner = true, coalesce(advertisement:matched_key_value_ids, []), [])                       as matched_keyvalue_item_ids
            , IFF(network.value:network_is_extra_item_owner = true, coalesce(advertisement:matched_daypart, false), false)                                                                           as matched_daypart
            , IFF(network.value:network_is_ad_owner = true, coalesce(advertisement:placement_type_priority, 'Unknown'), 'Unknown')           as placement_type_priority
            , coalesce(visitor:platform_group, '-1')                                                                           as platform_group
            , coalesce(network.value:geo_visibility.report_aggregate, 'FULL_VISIBILITY')                                             as geo_visibility
            , coalesce(network.value:user_agent_visibility.report_aggregate, 'FULL_VISIBILITY')                                      as user_agent_visibility
            , coalesce(visitor:postal_code, '-1')                                                                              as postal_code
            , coalesce(network.value:postal_code_package_id, [])                                            as postal_code_package_ids
            , coalesce(visitor:city_id, cast(-1 as int))                                                                       as user_city_id
            , coalesce(visitor:state_id, cast(-1 as int))                                                                      as user_state_id
            , coalesce(visitor:dma_code, cast(-1 as int))                                                                      as user_dma_code
            , coalesce(cast(visitor:country_id as bigint), cast(-1 as bigint))                                                     as user_country_id
            , coalesce(visitor:platform_browser_id, cast(-1 as bigint))                                                          as delivered_platform_browser_id
            , coalesce(visitor:platform_device_id, cast(-1 as bigint))                                                           as delivered_platform_device_id
            , coalesce(visitor:platform_os_id, cast(-1 as bigint))                                                               as delivered_platform_os_id
            , coalesce(visitor:operator_zone_id, cast(-1 as bigint))                                                             as operator_zone_id
            , coalesce(request:delivery_method, 'MRMADS')                                                                      as integration_delivery_method
            , coalesce(network.value:scenario_id, cast(-1 as bigint))                                                                  as scenario_id
            , sum(coalesce(ack:metrics.ad_impression, cast(0 as bigint)))                                                        as ad_views
            , sum(IFF(network.value:network_is_ad_owner = true, coalesce(ack:metrics.no_ad_impression, cast(0 as bigint)), cast(0 as bigint)))   as no_ad_views
            , sum(coalesce(ack:metrics.raw_ad_impression, cast(0 as bigint)))                                                    as gross_ad_views
            , sum(coalesce(ack:metrics.click, cast(0 as bigint)))                                                                as clicks
            , sum(coalesce(ack:metrics.no_click, cast(0 as bigint)))                                                             as no_clicks
            , sum(coalesce(ack:metrics.first_quartile, cast(0 as bigint)))                                                       as first_quartile
            , sum(coalesce(ack:metrics.middle_quartile, cast(0 as bigint)))                                                      as middle_quartile
            , sum(coalesce(ack:metrics.third_quartile, cast(0 as bigint)))                                                       as third_quartile
            , sum(coalesce(ack:metrics.complete_quartile, cast(0 as bigint)))                                                    as complete_quartile
            , sum(coalesce(ack:metrics.can_quartile, cast(0 as bigint)))                                                         as can_quartile
            , sum(coalesce(ack:metrics.ad_expand, cast(0 as bigint)))                                                            as ad_expand
            , sum(coalesce(ack:metrics.ad_collapse, cast(0 as bigint)))                                                          as ad_collapse
            , sum(coalesce(ack:metrics.measurable_ad_expand_collapse_impression, cast(0 as bigint)))                             as measurable_ad_expand_collapse_impression
            , sum(coalesce(ack:metrics.ad_mute, cast(0 as bigint)))                                                              as ad_mute
            , sum(coalesce(ack:metrics.ad_unmute, cast(0 as bigint)))                                                            as ad_unmute
            , sum(coalesce(ack:metrics.measurable_ad_mute_unmute_impression, cast(0 as bigint)))                                 as measurable_ad_mute_unmute_impression
            , sum(coalesce(ack:metrics.ad_rewind, cast(0 as bigint)))                                                            as ad_rewind
            , sum(coalesce(ack:metrics.measurable_ad_rewind_impression, cast(0 as bigint)))                                      as measurable_ad_rewind_impression
            , sum(coalesce(ack:metrics.ad_pause, cast(0 as bigint)))                                                             as ad_pause
            , sum(coalesce(ack:metrics.ad_resume, cast(0 as bigint)))                                                            as ad_resume
            , sum(coalesce(ack:metrics.measurable_ad_pause_resume_impression, cast(0 as bigint)))                                as measurable_ad_pause_resume_impression
            , sum(coalesce(ack:metrics.ad_close, cast(0 as bigint)))                                                             as ad_close
            , sum(coalesce(ack:metrics.measurable_ad_close_impression, cast(0 as bigint)))                                       as measurable_ad_close_impression
            , sum(coalesce(ack:metrics.ad_accept_invitation, cast(0 as bigint)))                                                 as ad_accept_invitation
            , sum(coalesce(ack:metrics.ad_minimize, cast(0 as bigint)))                                                          as ad_minimize
            , sum(coalesce(ack:metrics.measurable_ad_accept_invitation_minimize_impression, cast(0 as bigint)))                  as measurable_ad_accept_invitation_minimize_impression
            , sum(IFF(network.value:network_is_ad_owner = true and coalesce(network.value:role, '') = 'CRO', coalesce(ack:metrics.hylda_replacement_impression_gains, cast(0 as bigint)), cast(0 as bigint)))    as hylda_replacement_impression_gains
            , cast(0 as bigint)                                                                                                                                                            as hylda_replacement_impression_forfeits
            , sum(IFF(coalesce(request:is_ssp_bidder_request, false) = true and (bitand(coalesce(network.value:value:bit_flags, cast(0 as bigint)), BITSHIFTLEFT(cast(1 as bigint), 49)) > 0 or bitand(coalesce(network.value:value:bit_flags, cast(0 as bigint)), BITSHIFTLEFT(cast(1 as bigint), 50)) > 0), 1, 0)
                * coalesce(ack:metrics.ad_bid_won, cast(0 as bigint)))                                                                                                                   as ad_bid_won
            , sum(IFF(bitand(coalesce(request:extra_flags, cast(0 as bigint)), 1073741824) = 1073741824 and coalesce(network.value:role, '') = 'CRO' and bitand(coalesce(request:extra_flags, cast(0 as bigint)), 16384) = 16384, cast(0 as bigint), coalesce(ack:metrics.ad_insertion, cast(0 as bigint))))
      as ad_insertion
            , sum(coalesce(network.value:revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_revenue_ratio, cast(0 as int)))                                                         as revenue
            , sum(coalesce(network.value:content_owner_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_revenue_ratio, cast(0 as int)))                                           as co_revenue
            , sum(coalesce(network.value:distributor_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_revenue_ratio, cast(0 as int)))                                             as d_revenue
            , sum(coalesce(network.value:reseller_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_revenue_ratio, cast(0 as int)))                                                as r_revenue
            , sum(coalesce(network.value:margin, cast(0 as double)) * coalesce(ack:metrics.fire_margin_ratio, cast(0 as int)))                                                                 as margin
            , sum(coalesce(network.value:bidding_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_revenue_ratio, cast(0 as int)))                                                 as bidding_revenue
            , sum(coalesce(network.value:content_owner_bidding_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_revenue_ratio, cast(0 as int)))                                   as co_bidding_revenue
            , sum(coalesce(network.value:distributor_bidding_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_revenue_ratio, cast(0 as int)))                                     as d_bidding_revenue
            , sum(coalesce(network.value:reseller_bidding_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_revenue_ratio, cast(0 as int)))                                        as r_bidding_revenue
            , cast(0 as bigint)                                                                                                                                                            as total_ad_opportunity
            , cast(0 as bigint)                                                                                                                                                            as win_slot_opportunity
            , cast(0 as bigint)                                                                                                                                                            as total_slot_opportunity
            , IFF(ARRAY_SIZE(coalesce(network.value:marketplace_audience_extension_deal_ids, [])) = 0
      , cast(-1 as bigint)
      , network.value:marketplace_audience_extension_deal_ids[0])                                                                                                                      as audience_extension_deal_id
            , sum(coalesce(network.value:revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_bid_revenue_ratio, cast(0 as int)))                                                     as bid_won_revenue
            , sum(coalesce(network.value:content_owner_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_bid_revenue_ratio, cast(0 as int)))                                       as co_bid_won_revenue
            , sum(coalesce(network.value:distributor_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_bid_revenue_ratio, cast(0 as int)))                                         as d_bid_won_revenue
            , sum(coalesce(network.value:reseller_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_bid_revenue_ratio, cast(0 as int)))                                            as r_bid_won_revenue
            , sum(coalesce(network.value:bidding_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_bid_revenue_ratio, cast(0 as int)))                                             as bid_won_bidding_revenue
            , sum(coalesce(network.value:content_owner_bidding_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_bid_revenue_ratio, cast(0 as int)))                               as co_bid_won_bidding_revenue
            , sum(coalesce(network.value:distributor_bidding_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_bid_revenue_ratio, cast(0 as int)))                                 as d_bid_won_bidding_revenue
            , sum(coalesce(network.value:reseller_bidding_revenue, cast(0 as double)) * coalesce(ack:metrics.fire_event_bid_revenue_ratio, cast(0 as int)))                                    as r_bid_won_bidding_revenue
            , coalesce(network.value:tracked_audience_item_ids, [])                                                                                                  as tracked_audience_item_ids
            , sum(coalesce(network.value:margin, cast(0 as double)) * coalesce(ack:metrics.fire_event_bid_revenue_ratio, cast(0 as int)))                                                      as bid_won_margin
            , coalesce(network.value:geo_state_visibility.report_aggregate, 'FULL_VISIBILITY')                                       as geo_state_visibility
            , coalesce(network.value:geo_dma_visibility.report_aggregate, 'FULL_VISIBILITY')                                         as geo_dma_visibility
            , coalesce(network.value:geo_city_visibility.report_aggregate, 'FULL_VISIBILITY')                                        as geo_city_visibility
            , coalesce(network.value:geo_zip_code_visibility.report_aggregate, 'FULL_VISIBILITY')                                    as geo_zipcode_visibility
            , coalesce(network.value:key_value_visibility.report_aggregate, 'FULL_VISIBILITY')                                       as key_value_visibility
            , coalesce(slot:avail_type, '')                                                                                    as slot_avail_type
            , IFF(network.value:network_is_ad_owner = true, coalesce(advertisement:linear_decision_type, 'Not Applicable'), 'Not Applicable')                                                          as linear_decision_type
            , IFF(visitor:standard_device_type_child_id is null,
                [],
                to_array(visitor:standard_device_type_child_id))                                                                  as standard_device_type_ids
            , coalesce(visitor:standard_environment_id, cast(-1 as int))                                                       as standard_environment_id
            , coalesce(visitor:standard_os_id, cast(-1 as int))                                                                as standard_os_id
            , IFF(network.value:standard_brand_visibility.report_aggregate is not null or network.value:supply_source != 3,
                coalesce(request:context.standard_brand_id, cast(-1 as int)),
                cast(-1 as int))                                                                                               as standard_brand_id
            , cast(-1 as int)                                                                                                  as standard_channel_id
            , IFF(network.value:standard_genre_visibility.report_aggregate is not null or network.value:supply_source != 3,
                coalesce(request:context.standard_genre_ids, []),
                [])                                                                                   as standard_genre_ids
            , IFF(network.value:content_form_visibility.report_aggregate is not null or network.value:supply_source != 3,
                coalesce(request:context.content_form_id, cast(-1 as int)),
                cast(-1 as int))                                                                                               as content_form_id
            , IFF(network.value:content_rating_visibility.report_aggregate is not null or network.value:supply_source != 3,
                coalesce(request:context.content_rating_id, cast(-1 as int)),
                cast(-1 as int))                                                                                               as content_rating_id
            , IFF(network.value:standard_language_visibility.report_aggregate is not null or network.value:supply_source != 3,
                coalesce(request:context.standard_language_ids, []),
                [])                                                                                   as standard_language_ids
            , coalesce(request:context.stream_mode_id, cast(-1 as int))                                                        as stream_mode_id
            , coalesce(request:context.inventory_location_id, cast(-1 as int))                                                 as inventory_location_id
            , coalesce(network.value:rule_type_priority, 'Unknown')                                                                  as mrm_rule_type_priority
            , coalesce(network.value:listing_id, [])                                                       as listing_ids
            , coalesce(network.value:inbound_order_id, cast(-1 as bigint))                                                             as inbound_order_id
            , []                                                                                     as inbound_listing_ids
            , coalesce(network.value:outbound_order_id, cast(-1 as bigint))                                                            as outbound_order_id
            , coalesce(network.value:outbound_listing_id, [])                                              as outbound_listing_ids
            , coalesce(request:context.ip_enabled_audience_id, cast(-1 as int))                                                as ip_enabled_audience_id
            , IFF(network.value:standard_programmer_visibility.report_aggregate is not null or network.value:supply_source != 3,
                coalesce(request:context.standard_programmer_id, cast(-1 as int)),
                cast(-1 as int))                                                                                               as standard_programmer_id
            , coalesce(network.value:geo_country_visibility.report_aggregate, 'FULL_VISIBILITY')                                     as geo_country_visibility
            , coalesce(network.value:standard_brand_visibility.report_aggregate, 'FULL_VISIBILITY')                                  as standard_brand_visibility
            , coalesce(network.value:standard_genre_visibility.report_aggregate, 'FULL_VISIBILITY')                                  as standard_genre_visibility
            , coalesce(network.value:content_rating_visibility.report_aggregate, 'FULL_VISIBILITY')                                  as content_rating_visibility
            , coalesce(network.value:supply_source, cast(-1 as int))                                                                 as supply_source
            , coalesce(network.value:sales_channel, cast(-1 as int))                                                                 as sales_channel
            , coalesce(network.value:inbound_order_auction_type, 'UNKNOWN')                                                          as inbound_order_auction_type
            , sum(IFF(coalesce(request:is_ssp_bidder_request, false) = true and (bitand(coalesce(network.value:value:bit_flags, cast(0 as bigint)), BITSHIFTLEFT(cast(1 as bigint), 49)) > 0 or bitand(coalesce(network.value:value:bit_flags, cast(0 as bigint)), BITSHIFTLEFT(cast(1 as bigint), 50)) > 0), 1, 0)
                * coalesce(network.value:ssp_clearing_revenue, cast(0 as double))
                * coalesce(ack:metrics.fire_event_revenue_ratio, cast(0 as int)))                                            as ssp_clearing_revenue
            , coalesce(request:bid_request.publisher_id, 'Unknown')                                                            as ssp_external_publisher_id
            , IFF(coalesce(network.value:role, '') in ('CRO', 'R'),
                coalesce(advertisement:global_advertiser_ids, []),
                [])                                                                                 as global_advertiser_ids
            , IFF(coalesce(network.value:role, '') in ('CRO', 'R'),
                coalesce(advertisement:global_brand_ids, []),
                [])                                                                                 as global_brand_ids
            , coalesce(visitor:dma_code_id, cast(-1 as int))                                                                   as user_dma_code_id
            , IFF(network.value:demand_dim_awareability or network.value:network_is_ad_owner,
                coalesce(advertisement:global_industry_ids, []),
                [])                                                                                 as global_industry_ids
            , coalesce(network.value:standard_programmer_visibility.report_aggregate, 'FULL_VISIBILITY')                             as standard_programmer_visibility
            , sum(IFF(advertisement:is_fallback = true, cast(0 as bigint), coalesce(ack:metrics.raw_ad_impression, cast(0 as bigint))))    as gross_ad_views_primary
            , sum(IFF(advertisement:is_fallback = true, coalesce(ack:metrics.raw_ad_impression, cast(0 as bigint)), cast(0 as bigint)))    as gross_ad_views_fallback
            , sum(IFF(advertisement:is_fallback = true, cast(0 as bigint), coalesce(ack:metrics.ad_impression, cast(0 as bigint))))        as ad_views_primary
            , sum(IFF(advertisement:is_fallback = true, coalesce(ack:metrics.ad_impression, cast(0 as bigint)), cast(0 as bigint)))        as ad_views_fallback
            , coalesce(network.value:bidder_seat_id, cast(-1 as bigint))                                                               as bidder_seat_id
            , coalesce(request:global_currency_version, '')                                                                    as global_currency_version
            , coalesce(network.value:global_currency_id, cast(-1 as bigint))                                                           as global_currency_id
            , IFF(bitand(coalesce(request:extra_flags, cast(0 as bigint)), 1073741824) = 1073741824 and coalesce(network.value:role, '') = 'CRO', cast(-3 as bigint),
                coalesce(request:context.profile_id, cast(-1 as bigint)))                                                       as profile_id
            , coalesce(request:context.profile_type, 'UNKNOWN')                                                                as profile_type
            , IFF(network.value:network_is_extra_item_owner = true,
                coalesce(advertisement:matched_inventory_package_ids, []),
                [])                                                                                 as matched_inventory_package_ids
            , date_trunc('HOUR', cast(ack:timestamp as timestamp))                                                             as event_date
      from ack_source
            ,lateral flatten(input => partners) as network
      where process_batch_id = {{ var('var_current_batch_id') }}
      and ack:ack_entity_type = 'ad'
      and advertisement:is_bumper = false
      and (ack:is_private_impression = false or network.value:network_is_ad_owner = true or network.value:network_is_extra_item_owner = true)
      group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 92, 101, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 137, 138, 139, 140, 141, 142, 147, 148, 149, 150, 151, 152, 153

      union all

      -- hylda_replacement_impression_forfeits
      select process_batch_id                                                                                           as process_batch_id
            , coalesce(network.value:network_id, cast(-1 as bigint))                                                                   as network_id
            , coalesce(network.value:content_owner_network_id, cast(-1 as bigint))                                                     as content_owner_id
            , IFF(bitand(coalesce(request:extra_flags, cast(0 as bigint)), 1073741824) = 1073741824 and coalesce(network.value:role, '') = 'CRO', cast(-3 as bigint),
                coalesce(network.value:distributor_network_id, cast(-1 as bigint)))                                                   as distributor_id
            , coalesce(network.value:reseller_network_id, cast(-1 as bigint))                                                          as reseller_id
            , coalesce(request:context.tv_network_id, cast(-1 as bigint))                                                        as tv_network_id
            , coalesce(network.value:role, '')                                                                                       as transaction_type
            , coalesce(ack:traffic_type, cast(0 as bigint))                                                                      as traffic_type
            , coalesce(network.value:bit_flags, cast(0 as bigint)) + coalesce(request:bit_flags, cast(0 as bigint))                      as bit_flag -- the replaced out ad is not delivered to audience. The flag about delivered ad is not suitable here
            , coalesce(network.value:asset_id, cast(-1 as bigint))                                                                     as asset_id
            , coalesce(network.value:series_id, cast(-1 as bigint))                                                                    as series_id
            , coalesce(network.value:asset_group_ids, [])                                                  as asset_group_ids
            , coalesce(network.value:site_section_id, cast(-1 as bigint))                                                              as site_section_id
            , coalesce(network.value:site_id, cast(-1 as bigint))                                                                      as site_id
            , coalesce(network.value:site_section_group_ids, [])                                           as site_section_group_ids
            , network.value:airing_id                                                                                                as airing_id
            , network.value:airing_channel_id                                                                                        as channel_id
            , coalesce(network.value:break_id, cast(-1 as bigint))                                                                     as break_id
            , coalesce(slot:time_position_class, 'Unknown')                                                                    as time_position_class
            , cast(-1 as bigint)                                                                                                 as inbound_mrm_rule_id
            , cast(-1 as bigint)                                                                                                 as mrm_rule_id
            , cast(-1 as bigint)                                                                                                 as campaign_id
            , cast(-1 as bigint)                                                                                                 as io_id
            , coalesce(advertisement:replaced_placement_id, cast(-1 as bigint))                                                  as placement_id
            , coalesce(advertisement:replaced_ad_id, cast(-1 as bigint))                                                         as ad_id
            , coalesce(advertisement:replaced_creative_id, cast(-1 as bigint))                                                   as creative_id
            , IFF(bitand(coalesce(request:extra_flags, cast(0 as bigint)), 1073741824) = 1073741824 and coalesce(network.value:role, '') = 'CRO', cast(-3 as bigint),
                coalesce(advertisement:replaced_rendition_id, cast(-1 as bigint)))                                                as rendition_id
            , 'Static'                                                                                                         as delivery_method
            , cast(-1 as bigint)                                                                                                 as targeting_criteria_id
            , coalesce(advertisement:replaced_ad_unit_id, cast(-1 as bigint))                                                    as ad_unit_id
            , []                                                                                     as matched_audience_item_ids
            , []                                                                                     as matched_keyvalue_item_ids
            , false                                                                                                            as matched_daypart
            , 'Unknown'                                                                                                        as placement_type_priority
            , coalesce(visitor:platform_group, '-1')                                                                           as platform_group
            , coalesce(network.value:geo_visibility.report_aggregate, 'FULL_VISIBILITY')                                             as geo_visibility
            , coalesce(network.value:user_agent_visibility.report_aggregate, 'FULL_VISIBILITY')                                      as user_agent_visibility
            , coalesce(visitor:postal_code, '-1')                                                                              as postal_code
            , coalesce(network.value:postal_code_package_id, [])                                            as postal_code_package_ids
            , coalesce(visitor:city_id, cast(-1 as int))                                                                       as user_city_id
            , coalesce(visitor:state_id, cast(-1 as int))                                                                      as user_state_id
            , coalesce(visitor:dma_code, cast(-1 as int))                                                                      as user_dma_code
            , coalesce(cast(visitor:country_id as bigint), cast(-1 as bigint))                                                     as user_country_id
            , coalesce(visitor:platform_browser_id, cast(-1 as bigint))                                                          as delivered_platform_browser_id
            , coalesce(visitor:platform_device_id, cast(-1 as bigint))                                                           as delivered_platform_device_id
            , coalesce(visitor:platform_os_id, cast(-1 as bigint))                                                               as delivered_platform_os_id
            , cast(-1 as bigint)                                                                                                 as operator_zone_id
            , coalesce(request:delivery_method, 'MRMADS')                                                                      as integration_delivery_method
            , coalesce(network.value:scenario_id, cast(-1 as bigint))                                                                  as scenario_id
            , cast(0 as bigint)                                                                                                  as ad_views
            , cast(0 as bigint)                                                                                                  as no_ad_views
            , cast(0 as bigint)                                                                                                  as gross_ad_views
            , cast(0 as bigint)                                                                                                  as clicks
            , cast(0 as bigint)                                                                                                  as no_clicks
            , cast(0 as bigint)                                                                                                  as first_quartile
            , cast(0 as bigint)                                                                                                  as middle_quartile
            , cast(0 as bigint)                                                                                                  as third_quartile
            , cast(0 as bigint)                                                                                                  as complete_quartile
            , cast(0 as bigint)                                                                                                  as can_quartile
            , cast(0 as bigint)                                                                                                  as ad_expand
            , cast(0 as bigint)                                                                                                  as ad_collapse
            , cast(0 as bigint)                                                                                                  as measurable_ad_expand_collapse_impression
            , cast(0 as bigint)                                                                                                  as ad_mute
            , cast(0 as bigint)                                                                                                  as ad_unmute
            , cast(0 as bigint)                                                                                                  as measurable_ad_mute_unmute_impression
            , cast(0 as bigint)                                                                                                  as ad_rewind
            , cast(0 as bigint)                                                                                                  as measurable_ad_rewind_impression
            , cast(0 as bigint)                                                                                                  as ad_pause
            , cast(0 as bigint)                                                                                                  as ad_resume
            , cast(0 as bigint)                                                                                                  as measurable_ad_pause_resume_impression
            , cast(0 as bigint)                                                                                                  as ad_close
            , cast(0 as bigint)                                                                                                  as measurable_ad_close_impression
            , cast(0 as bigint)                                                                                                  as ad_accept_invitation
            , cast(0 as bigint)                                                                                                  as ad_minimize
            , cast(0 as bigint)                                                                                                  as measurable_ad_accept_invitation_minimize_impression
            , cast(0 as bigint)                                                                                                  as hylda_replacement_impression_gains
            , sum(coalesce(ack:metrics.hylda_replacement_impression_forfeits, cast(0 as bigint)))                                as hylda_replacement_impression_forfeits
            , cast(0 as bigint)                                                                                                  as ad_bid_won
            , cast(0 as bigint)                                                                                                  as ad_insertion
            , cast(0 as double)                                                                                                as revenue
            , cast(0 as double)                                                                                                as co_revenue
            , cast(0 as double)                                                                                                as d_revenue
            , cast(0 as double)                                                                                                as r_revenue
            , cast(0 as double)                                                                                                as margin
            , cast(0 as double)                                                                                                as bidding_revenue
            , cast(0 as double)                                                                                                as co_bidding_revenue
            , cast(0 as double)                                                                                                as d_bidding_revenue
            , cast(0 as double)                                                                                                as r_bidding_revenue
            , cast(0 as bigint)                                                                                                  as total_ad_opportunity
            , cast(0 as bigint)                                                                                                  as win_slot_opportunity
            , cast(0 as bigint)                                                                                                  as total_slot_opportunity
            , cast(-1 as bigint)                                                                                                 as audience_extension_deal_id
            , cast(0 as double)                                                                                                as bid_won_revenue
            , cast(0 as double)                                                                                                as co_bid_won_revenue
            , cast(0 as double)                                                                                                as d_bid_won_revenue
            , cast(0 as double)                                                                                                as r_bid_won_revenue
            , cast(0 as double)                                                                                                as bid_won_bidding_revenue
            , cast(0 as double)                                                                                                as co_bid_won_bidding_revenue
            , cast(0 as double)                                                                                                as d_bid_won_bidding_revenue
            , cast(0 as double)                                                                                                as r_bid_won_bidding_revenue
            , []                                                                                     as tracked_audience_item_ids
            , cast(0 as double)                                                                                                as bid_won_margin
            , coalesce(network.value:geo_state_visibility.report_aggregate, 'FULL_VISIBILITY')                                       as geo_state_visibility
            , coalesce(network.value:geo_dma_visibility.report_aggregate, 'FULL_VISIBILITY')                                         as geo_dma_visibility
            , coalesce(network.value:geo_city_visibility.report_aggregate, 'FULL_VISIBILITY')                                        as geo_city_visibility
            , coalesce(network.value:geo_zip_code_visibility.report_aggregate, 'FULL_VISIBILITY')                                    as geo_zipcode_visibility
            , coalesce(network.value:key_value_visibility.report_aggregate, 'FULL_VISIBILITY')                                       as key_value_visibility
            , coalesce(slot:avail_type, '')                                                                                    as slot_avail_type
            , 'Not Applicable'                                                                                                 as linear_decision_type
            , IFF(visitor:standard_device_type_child_id is null,
                [],
                to_array(visitor:standard_device_type_child_id))                                                                  as standard_device_type_ids
            , coalesce(visitor:standard_environment_id, cast(-1 as int))                                                       as standard_environment_id
            , coalesce(visitor:standard_os_id, cast(-1 as int))                                                                as standard_os_id
            , coalesce(request:context.standard_brand_id, cast(-1 as int))                                                     as standard_brand_id
            , cast(-1 as int)                                                                                                  as standard_channel_id
            , coalesce(request:context.standard_genre_ids, [])                                        as standard_genre_ids
            , coalesce(request:context.content_form_id, cast(-1 as int))                                                       as content_form_id
            , coalesce(request:context.content_rating_id, cast(-1 as int))                                                     as content_rating_id
            , coalesce(request:context.standard_language_ids, [])                                     as standard_language_ids
            , coalesce(request:context.stream_mode_id, cast(-1 as int))                                                        as stream_mode_id
            , coalesce(request:context.inventory_location_id, cast(-1 as int))                                                 as inventory_location_id
            , 'Unknown'                                                                                                        as mrm_rule_type_priority
            , coalesce(network.value:listing_id, [])                                                       as listing_ids
            , cast(-1 as bigint)                                                                                                 as inbound_order_id
            , []                                                                                     as inbound_listing_ids
            , cast(-1 as bigint)                                                                                                 as outbound_order_id
            , []                                                                                     as outbound_listing_ids
            , coalesce(request:context.ip_enabled_audience_id, cast(-1 as int))                                                as ip_enabled_audience_id
            , coalesce(request:context.standard_programmer_id, cast(-1 as int))                                                as standard_programmer_id
            , coalesce(network.value:geo_country_visibility.report_aggregate, 'FULL_VISIBILITY')                                     as geo_country_visibility
            , coalesce(network.value:standard_brand_visibility.report_aggregate, 'FULL_VISIBILITY')                                  as standard_brand_visibility
            , coalesce(network.value:standard_genre_visibility.report_aggregate, 'FULL_VISIBILITY')                                  as standard_genre_visibility
            , coalesce(network.value:content_rating_visibility.report_aggregate, 'FULL_VISIBILITY')                                  as content_rating_visibility
            , coalesce(network.value:supply_source, cast(-1 as int))                                                                 as supply_source
            , coalesce(network.value:sales_channel, cast(-1 as int))                                                                 as sales_channel
            , 'UNKNOWN'                                                                                                        as inbound_order_auction_type
            , cast(0 as double)                                                                                                as ssp_clearing_revenue
            , 'Unknown'                                                                                                        as ssp_external_publisher_id
            , coalesce(advertisement:global_advertiser_ids, [])                                      as global_advertiser_ids
            , coalesce(advertisement:global_brand_ids, [])                                           as global_brand_ids
            , coalesce(visitor:dma_code_id, cast(-1 as int))                                                                   as user_dma_code_id
            , coalesce(advertisement:global_industry_ids, [])                                        as global_industry_ids
            , coalesce(network.value:standard_programmer_visibility.report_aggregate, 'FULL_VISIBILITY')                             as standard_programmer_visibility
            , cast(0 as bigint)                                                                                                  as gross_ad_views_primary
            , cast(0 as bigint)                                                                                                  as gross_ad_views_fallback
            , cast(0 as bigint)                                                                                                  as ad_views_primary
            , cast(0 as bigint)                                                                                                  as ad_views_fallback
            , cast(-1 as bigint)                                                                                                 as bidder_seat_id
            , ''                                                                                                               as global_currency_version
            , cast(-1 as bigint)                                                                                                 as global_currency_id
            , IFF(bitand(coalesce(request:extra_flags, cast(0 as bigint)), 1073741824) = 1073741824 and coalesce(network.value:role, '') = 'CRO', cast(-3 as bigint),
                coalesce(request:context.profile_id, cast(-1 as bigint)))                                                       as profile_id
            , coalesce(request:context.profile_type, 'UNKNOWN')                                                                as profile_type
            , []                                                                                     as matched_inventory_package_ids
            , date_trunc('HOUR', cast(ack:timestamp as timestamp))                                                             as event_date
      from ack_source
            ,lateral flatten(input => partners) as network
      where process_batch_id = {{ var('var_current_batch_id') }}
      and ack:ack_entity_type = 'ad'
      and network.value:network_is_ad_owner = true
      and coalesce(network.value:role, '') = 'CRO'
      and advertisement:is_bumper = false -- no bumper ad
      and advertisement:is_replacement = true -- ad is replaced
      and coalesce(ack:metrics.hylda_replacement_impression_forfeits, cast(0 as bigint)) != 0 -- remove zero rows
      group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 92, 101, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 137, 138, 139, 140, 141, 142, 147, 148, 149, 150, 151, 152, 153

      union all

      -- win_slot_opportunity, total_slot_opportunity
      select process_batch_id                                                                                           as process_batch_id
            , slot_opportunity_context.network_id                                                                              as network_id
            , cast(-1 as bigint)                                                                                                 as content_owner_id
            , cast(-1 as bigint)                                                                                                 as distributor_id
            , cast(-1 as bigint)                                                                                                 as reseller_id
            , cast(-1 as bigint)                                                                                                 as tv_network_id
            , 'CROP'                                                                                                           as transaction_type
            , slot_opportunity_context.traffic_type                                                                            as traffic_type
            , slot_opportunity_context.bit_flag                                                                                as bit_flag
            , cast(-1 as bigint)                                                                                                 as asset_id
            , cast(-1 as bigint)                                                                                                 as series_id
            , []                                                                                     as asset_group_ids
            , cast(-1 as bigint)                                                                                                 as site_section_id
            , cast(-1 as bigint)                                                                                                 as site_id
            , []                                                                                     as site_section_group_ids
            , cast(-1 as bigint)                                                                                                 as airing_id
            , cast(-1 as bigint)                                                                                                 as channel_id
            , cast(-1 as bigint)                                                                                                 as break_id
            , 'Unknown'                                                                                                        as time_position_class
            , cast(-1 as bigint)                                                                                                 as inbound_mrm_rule_id
            , coalesce(outbound_rule.value:rule_id, cast(-1 as bigint))                                                                as mrm_rule_id
            , cast(-1 as bigint)                                                                                                 as campaign_id
            , cast(-1 as bigint)                                                                                                 as io_id
            , cast(-1 as bigint)                                                                                                 as placement_id
            , cast(-1 as bigint)                                                                                                 as ad_id
            , cast(-1 as bigint)                                                                                                 as creative_id
            , cast(-1 as bigint)                                                                                                 as rendition_id
            , ''                                                                                                               as delivery_method
            , cast(-1 as bigint)                                                                                                 as targeting_criteria_id
            , cast(-1 as bigint)                                                                                                 as ad_unit_id
            , []                                                                                     as matched_audience_item_ids
            , []                                                                                     as matched_keyvalue_item_ids
            , false                                                                                                            as matched_daypart
            , 'Unknown'                                                                                                        as placement_type_priority
            , '-1'                                                                                                             as platform_group
            , 'FULL_VISIBILITY'                                                                                                as geo_visibility
            , 'FULL_VISIBILITY'                                                                                                as user_agent_visibility
            , '-1'                                                                                                             as postal_code
            , []                                                                                      as postal_code_package_ids
            , cast(-1 as int)                                                                                                  as user_city_id
            , cast(-1 as int)                                                                                                  as user_state_id
            , cast(-1 as int)                                                                                                  as user_dma_code
            , cast(-1 as bigint)                                                                                                 as user_country_id
            , cast(-1 as bigint)                                                                                                 as delivered_platform_browser_id
            , cast(-1 as bigint)                                                                                                 as delivered_platform_device_id
            , cast(-1 as bigint)                                                                                                 as delivered_platform_os_id
            , cast(-1 as bigint)                                                                                                 as operator_zone_id
            , 'MRMADS'                                                                                                         as integration_delivery_method
            , cast(-1 as bigint)                                                                                                 as scenario_id
            , cast(0 as bigint)                                                                                                  as ad_views
            , cast(0 as bigint)                                                                                                  as no_ad_views
            , cast(0 as bigint)                                                                                                  as gross_ad_views
            , cast(0 as bigint)                                                                                                  as clicks
            , cast(0 as bigint)                                                                                                  as no_clicks
            , cast(0 as bigint)                                                                                                  as first_quartile
            , cast(0 as bigint)                                                                                                  as middle_quartile
            , cast(0 as bigint)                                                                                                  as third_quartile
            , cast(0 as bigint)                                                                                                  as complete_quartile
            , cast(0 as bigint)                                                                                                  as can_quartile
            , cast(0 as bigint)                                                                                                  as ad_expand
            , cast(0 as bigint)                                                                                                  as ad_collapse
            , cast(0 as bigint)                                                                                                  as measurable_ad_expand_collapse_impression
            , cast(0 as bigint)                                                                                                  as ad_mute
            , cast(0 as bigint)                                                                                                  as ad_unmute
            , cast(0 as bigint)                                                                                                  as measurable_ad_mute_unmute_impression
            , cast(0 as bigint)                                                                                                  as ad_rewind
            , cast(0 as bigint)                                                                                                  as measurable_ad_rewind_impression
            , cast(0 as bigint)                                                                                                  as ad_pause
            , cast(0 as bigint)                                                                                                  as ad_resume
            , cast(0 as bigint)                                                                                                  as measurable_ad_pause_resume_impression
            , cast(0 as bigint)                                                                                                  as ad_close
            , cast(0 as bigint)                                                                                                  as measurable_ad_close_impression
            , cast(0 as bigint)                                                                                                  as ad_accept_invitation
            , cast(0 as bigint)                                                                                                  as ad_minimize
            , cast(0 as bigint)                                                                                                  as measurable_ad_accept_invitation_minimize_impression
            , cast(0 as bigint)                                                                                                  as hylda_replacement_impression_gains
            , cast(0 as bigint)                                                                                                  as hylda_replacement_impression_forfeits
            , cast(0 as bigint)                                                                                                  as ad_bid_won
            , cast(0 as bigint)                                                                                                  as ad_insertion
            , cast(0 as double)                                                                                                as revenue
            , cast(0 as double)                                                                                                as co_revenue
            , cast(0 as double)                                                                                                as d_revenue
            , cast(0 as double)                                                                                                as r_revenue
            , cast(0 as double)                                                                                                as margin
            , cast(0 as double)                                                                                                as bidding_revenue
            , cast(0 as double)                                                                                                as co_bidding_revenue
            , cast(0 as double)                                                                                                as d_bidding_revenue
            , cast(0 as double)                                                                                                as r_bidding_revenue
            , cast(0 as bigint)                                                                                                  as total_ad_opportunity
            , sum(coalesce(outbound_rule.value:win_opp, cast(0 as bigint)) * slot_opportunity_context.coefficient)                     as win_slot_opportunity
            , sum(coalesce(outbound_rule.value:total_opp, cast(0 as bigint)) * slot_opportunity_context.coefficient)                   as total_slot_opportunity
            , cast(-1 as bigint)                                                                                                 as audience_extension_deal_id
            , cast(0 as double)                                                                                                as bid_won_revenue
            , cast(0 as double)                                                                                                as co_bid_won_revenue
            , cast(0 as double)                                                                                                as d_bid_won_revenue
            , cast(0 as double)                                                                                                as r_bid_won_revenue
            , cast(0 as double)                                                                                                as bid_won_bidding_revenue
            , cast(0 as double)                                                                                                as co_bid_won_bidding_revenue
            , cast(0 as double)                                                                                                as d_bid_won_bidding_revenue
            , cast(0 as double)                                                                                                as r_bid_won_bidding_revenue
            , []                                                                                     as tracked_audience_item_ids
            , cast(0 as double)                                                                                                as bid_won_margin
            , 'FULL_VISIBILITY'                                                                                                as geo_state_visibility
            , 'FULL_VISIBILITY'                                                                                                as geo_dma_visibility
            , 'FULL_VISIBILITY'                                                                                                as geo_city_visibility
            , 'FULL_VISIBILITY'                                                                                                as geo_zipcode_visibility
            , 'FULL_VISIBILITY'                                                                                                as key_value_visibility
            , ''                                                                                                               as slot_avail_type
            , 'Not Applicable'                                                                                                 as linear_decision_type
            , []                                                                                      as standard_device_type_ids
            , cast(-1 as int)                                                                                                  as standard_environment_id
            , cast(-1 as int)                                                                                                  as standard_os_id
            , cast(-1 as int)                                                                                                  as standard_brand_id
            , cast(-1 as int)                                                                                                  as standard_channel_id
            , []                                                                                      as standard_genre_ids
            , cast(-1 as int)                                                                                                  as content_form_id
            , cast(-1 as int)                                                                                                  as content_rating_id
            , []                                                                                      as standard_language_ids
            , cast(-1 as int)                                                                                                  as stream_mode_id
            , cast(-1 as int)                                                                                                  as inventory_location_id
            , 'Unknown'                                                                                                        as mrm_rule_type_priority
            , []                                                                                     as listing_ids
            , cast(-1 as bigint)                                                                                                 as inbound_order_id
            , []                                                                                     as inbound_listing_ids
            , cast(-1 as bigint)                                                                                                 as outbound_order_id
            , []                                                                                     as outbound_listing_ids
            , cast(-1 as int)                                                                                                  as ip_enabled_audience_id
            , cast(-1 as int)                                                                                                  as standard_programmer_id
            , 'FULL_VISIBILITY'                                                                                                as geo_country_visibility
            , 'FULL_VISIBILITY'                                                                                                as standard_brand_visibility
            , 'FULL_VISIBILITY'                                                                                                as standard_genre_visibility
            , 'FULL_VISIBILITY'                                                                                                as content_rating_visibility
            , cast(0 as int)                                                                                                   as supply_source
            , cast(0 as int)                                                                                                   as sales_channel
            , 'UNKNOWN'                                                                                                        as inbound_order_auction_type
            , cast(0 as double)                                                                                                as ssp_clearing_revenue
            , 'Unknown'                                                                                                        as ssp_external_publisher_id
            , []                                                                                     as global_advertiser_ids
            , []                                                                                     as global_brand_ids
            , cast(-1 as int)                                                                                                  as user_dma_code_id
            , []                                                                                     as global_industry_ids
            , 'FULL_VISIBILITY'                                                                                                as standard_programmer_visibility
            , cast(0 as bigint)                                                                                                  as gross_ad_views_primary
            , cast(0 as bigint)                                                                                                  as gross_ad_views_fallback
            , cast(0 as bigint)                                                                                                  as ad_views_primary
            , cast(0 as bigint)                                                                                                  as ad_views_fallback
            , cast(-1 as bigint)                                                                                                 as bidder_seat_id
            , ''                                                                                                               as global_currency_version
            , cast(-1 as bigint)                                                                                                 as global_currency_id
            , cast(-1 as bigint)                                                                                                 as profile_id
            , 'UNKNOWN'                                                                                                        as profile_type
            , []                                                                                     as matched_inventory_package_ids
            , slot_opportunity_context.event_date                                                                              as event_date
      from (
            select
                process_batch_id                                                                                     as process_batch_id
                , date_trunc('HOUR', cast(ack:timestamp as timestamp))                                               as event_date
                , coalesce(network.value:network_id, cast(-1 as bigint))                                                   as network_id
                , coalesce(network.value:bit_flags, cast(0 as bigint)) + coalesce(request:bit_flags, cast(0 as bigint))      as bit_flag
                , coalesce(ack:traffic_type, cast(0 as bigint))                                                      as traffic_type
                , network.value:outbound_rules                                                                           as outbound_rules
                , coalesce(ack:multiplier, cast(1 as int)) * coalesce(request:magnifier, cast(1 as int)) * coalesce(request:log_sampling.magnifier, cast(1 as int))           as coefficient
            from ack_source
                      ,lateral flatten(input => partners) as network
            where process_batch_id = {{ var('var_current_batch_id') }}
                and ack:ack_entity_type = 'slot'
                and (coalesce(network.value:avails_category.slot_opp_avails_in_played_slot, cast(0 as int)) * coalesce(ack:metrics.avails_event_count, cast(0 as int))) != 0 -- avails != 0
            ) slot_opportunity_context
      ,lateral flatten(input => outbound_rules) as outbound_rule
      where coalesce(outbound_rule.value:win_opp, cast(0 as bigint)) != 0 or coalesce(outbound_rule.value:total_opp, cast(0 as bigint)) != 0
      group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 92, 101, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 137, 138, 139, 140, 141, 142, 147, 148, 149, 150, 151, 152, 153

      union all

      -- total_ad_opportunity
      select process_batch_id                                                                                           as process_batch_id
            , coalesce(ad_opportunity_rule.value:network_id, cast(-1 as bigint))                                                       as network_id
            , cast(-1 as bigint)                                                                                                 as content_owner_id
            , cast(-1 as bigint)                                                                                                 as distributor_id
            , cast(-1 as bigint)                                                                                                 as reseller_id
            , cast(-1 as bigint)                                                                                                 as tv_network_id
            , 'CROP'                                                                                                           as transaction_type
            , coalesce(ack:traffic_type, cast(0 as bigint))                                                                      as traffic_type
            , coalesce(advertisement:bit_flags, cast(0 as bigint)) + coalesce(request:bit_flags, cast(0 as bigint))                as bit_flag
            , cast(-1 as bigint)                                                                                                 as asset_id
            , cast(-1 as bigint)                                                                                                 as series_id
            , []                                                                                     as asset_group_ids
            , cast(-1 as bigint)                                                                                                 as site_section_id
            , cast(-1 as bigint)                                                                                                 as site_id
            , []                                                                                     as site_section_group_ids
            , cast(-1 as bigint)                                                                                                 as airing_id
            , cast(-1 as bigint)                                                                                                 as channel_id
            , cast(-1 as bigint)                                                                                                 as break_id
            , 'Unknown'                                                                                                        as time_position_class
            , cast(-1 as bigint)                                                                                                 as inbound_mrm_rule_id
            , coalesce(ad_opportunity_rule.value:rule_id, cast(-1 as bigint))                                                          as mrm_rule_id
            , cast(-1 as bigint)                                                                                                 as campaign_id
            , cast(-1 as bigint)                                                                                                 as io_id
            , cast(-1 as bigint)                                                                                                 as placement_id
            , cast(-1 as bigint)                                                                                                 as ad_id
            , cast(-1 as bigint)                                                                                                 as creative_id
            , cast(-1 as bigint)                                                                                                 as rendition_id
            , ''                                                                                                               as delivery_method
            , cast(-1 as bigint)                                                                                                 as targeting_criteria_id
            , cast(-1 as bigint)                                                                                                 as ad_unit_id
            , []                                                                                     as matched_audience_item_ids
            , []                                                                                     as matched_keyvalue_item_ids
            , false                                                                                                            as matched_daypart
            , 'Unknown'                                                                                                        as placement_type_priority
            , '-1'                                                                                                             as platform_group
            , 'FULL_VISIBILITY'                                                                                                as geo_visibility
            , 'FULL_VISIBILITY'                                                                                                as user_agent_visibility
            , '-1'                                                                                                             as postal_code
            , []                                                                                      as postal_code_package_ids
            , cast(-1 as int)                                                                                                  as user_city_id
            , cast(-1 as int)                                                                                                  as user_state_id
            , cast(-1 as int)                                                                                                  as user_dma_code
            , cast(-1 as bigint)                                                                                                 as user_country_id
            , cast(-1 as bigint)                                                                                                 as delivered_platform_browser_id
            , cast(-1 as bigint)                                                                                                 as delivered_platform_device_id
            , cast(-1 as bigint)                                                                                                 as delivered_platform_os_id
            , cast(-1 as bigint)                                                                                                 as operator_zone_id
            , 'MRMADS'                                                                                                         as integration_delivery_method
            , cast(-1 as bigint)                                                                                                 as scenario_id
            , cast(0 as bigint)                                                                                                  as ad_views
            , cast(0 as bigint)                                                                                                  as no_ad_views
            , cast(0 as bigint)                                                                                                  as gross_ad_views
            , cast(0 as bigint)                                                                                                  as clicks
            , cast(0 as bigint)                                                                                                  as no_clicks
            , cast(0 as bigint)                                                                                                  as first_quartile
            , cast(0 as bigint)                                                                                                  as middle_quartile
            , cast(0 as bigint)                                                                                                  as third_quartile
            , cast(0 as bigint)                                                                                                  as complete_quartile
            , cast(0 as bigint)                                                                                                  as can_quartile
            , cast(0 as bigint)                                                                                                  as ad_expand
            , cast(0 as bigint)                                                                                                  as ad_collapse
            , cast(0 as bigint)                                                                                                  as measurable_ad_expand_collapse_impression
            , cast(0 as bigint)                                                                                                  as ad_mute
            , cast(0 as bigint)                                                                                                  as ad_unmute
            , cast(0 as bigint)                                                                                                  as measurable_ad_mute_unmute_impression
            , cast(0 as bigint)                                                                                                  as ad_rewind
            , cast(0 as bigint)                                                                                                  as measurable_ad_rewind_impression
            , cast(0 as bigint)                                                                                                  as ad_pause
            , cast(0 as bigint)                                                                                                  as ad_resume
            , cast(0 as bigint)                                                                                                  as measurable_ad_pause_resume_impression
            , cast(0 as bigint)                                                                                                  as ad_close
            , cast(0 as bigint)                                                                                                  as measurable_ad_close_impression
            , cast(0 as bigint)                                                                                                  as ad_accept_invitation
            , cast(0 as bigint)                                                                                                  as ad_minimize
            , cast(0 as bigint)                                                                                                  as measurable_ad_accept_invitation_minimize_impression
            , cast(0 as bigint)                                                                                                  as hylda_replacement_impression_gains
            , cast(0 as bigint)                                                                                                  as hylda_replacement_impression_forfeits
            , cast(0 as bigint)                                                                                                  as ad_bid_won
            , cast(0 as bigint)                                                                                                  as ad_insertion
            , cast(0 as double)                                                                                                as revenue
            , cast(0 as double)                                                                                                as co_revenue
            , cast(0 as double)                                                                                                as d_revenue
            , cast(0 as double)                                                                                                as r_revenue
            , cast(0 as double)                                                                                                as margin
            , cast(0 as double)                                                                                                as bidding_revenue
            , cast(0 as double)                                                                                                as co_bidding_revenue
            , cast(0 as double)                                                                                                as d_bidding_revenue
            , cast(0 as double)                                                                                                as r_bidding_revenue
            , sum(coalesce(ad_opportunity_rule.value:total_opp, cast(0 as bigint)) * coalesce(ack:multiplier, cast(1 as int)) * coalesce(request:magnifier, cast(1 as int)))                            as total_ad_opportunity
            , cast(0 as bigint)                                                                                                  as win_slot_opportunity
            , cast(0 as bigint)                                                                                                  as total_slot_opportunity
            , cast(-1 as bigint)                                                                                                 as audience_extension_deal_id
            , cast(0 as double)                                                                                                as bid_won_revenue
            , cast(0 as double)                                                                                                as co_bid_won_revenue
            , cast(0 as double)                                                                                                as d_bid_won_revenue
            , cast(0 as double)                                                                                                as r_bid_won_revenue
            , cast(0 as double)                                                                                                as bid_won_bidding_revenue
            , cast(0 as double)                                                                                                as co_bid_won_bidding_revenue
            , cast(0 as double)                                                                                                as d_bid_won_bidding_revenue
            , cast(0 as double)                                                                                                as r_bid_won_bidding_revenue
            , []                                                                                     as tracked_audience_item_ids
            , cast(0 as double)                                                                                                as bid_won_margin
            , 'FULL_VISIBILITY'                                                                                                as geo_state_visibility
            , 'FULL_VISIBILITY'                                                                                                as geo_dma_visibility
            , 'FULL_VISIBILITY'                                                                                                as geo_city_visibility
            , 'FULL_VISIBILITY'                                                                                                as geo_zipcode_visibility
            , 'FULL_VISIBILITY'                                                                                                as key_value_visibility
            , ''                                                                                                               as slot_avail_type
            , 'Not Applicable'                                                                                                 as linear_decision_type
            , []                                                                                      as standard_device_type_ids
            , cast(-1 as int)                                                                                                  as standard_environment_id
            , cast(-1 as int)                                                                                                  as standard_os_id
            , cast(-1 as int)                                                                                                  as standard_brand_id
            , cast(-1 as int)                                                                                                  as standard_channel_id
            , []                                                                                      as standard_genre_ids
            , cast(-1 as int)                                                                                                  as content_form_id
            , cast(-1 as int)                                                                                                  as content_rating_id
            , []                                                                                      as standard_language_ids
            , cast(-1 as int)                                                                                                  as stream_mode_id
            , cast(-1 as int)                                                                                                  as inventory_location_id
            , 'Unknown'                                                                                                        as mrm_rule_type_priority
            , []                                                                                     as listing_ids
            , cast(-1 as bigint)                                                                                                 as inbound_order_id
            , []                                                                                     as inbound_listing_ids
            , cast(-1 as bigint)                                                                                                 as outbound_order_id
            , []                                                                                     as outbound_listing_ids
            , cast(-1 as int)                                                                                                  as ip_enabled_audience_id
            , cast(-1 as int)                                                                                                  as standard_programmer_id
            , 'FULL_VISIBILITY'                                                                                                as geo_country_visibility
            , 'FULL_VISIBILITY'                                                                                                as standard_brand_visibility
            , 'FULL_VISIBILITY'                                                                                                as standard_genre_visibility
            , 'FULL_VISIBILITY'                                                                                                as content_rating_visibility
            , cast(0 as int)                                                                                                   as supply_source
            , cast(0 as int)                                                                                                   as sales_channel
            , 'UNKNOWN'                                                                                                        as inbound_order_auction_type
            , cast(0 as double)                                                                                                as ssp_clearing_revenue
            , 'Unknown'                                                                                                        as ssp_external_publisher_id
            , []                                                                                     as global_advertiser_ids
            , []                                                                                     as global_brand_ids
            , cast(-1 as int)                                                                                                  as user_dma_code_id
            , []                                                                                     as global_industry_ids
            , 'FULL_VISIBILITY'                                                                                                as standard_programmer_visibility
            , cast(0 as bigint)                                                                                                  as gross_ad_views_primary
            , cast(0 as bigint)                                                                                                  as gross_ad_views_fallback
            , cast(0 as bigint)                                                                                                  as ad_views_primary
            , cast(0 as bigint)                                                                                                  as ad_views_fallback
            , cast(-1 as bigint)                                                                                                 as bidder_seat_id
            , ''                                                                                                               as global_currency_version
            , cast(-1 as bigint)                                                                                                 as global_currency_id
            , cast(-1 as bigint)                                                                                                 as profile_id
            , 'UNKNOWN'                                                                                                        as profile_type
            , []                                                                                     as matched_inventory_package_ids
            , date_trunc('HOUR', cast(ack:timestamp as timestamp))                                                             as event_date
      from ack_source
            ,lateral flatten(input => advertisement:ad_opportunity_rules) as ad_opportunity_rule
      where process_batch_id = {{ var('var_current_batch_id') }}
      and ack:ack_entity_type = 'ad'
      and coalesce(ack:metrics.raw_ad_impression, cast(0 as bigint)) != 0
      and coalesce(ad_opportunity_rule.value:total_opp, cast(0 as bigint)) != 0 -- remove zero rows
      group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 92, 101, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 137, 138, 139, 140, 141, 142, 147, 148, 149, 150, 151, 152, 153
)

select * from f_order_hourly