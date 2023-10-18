{{ config(materialized='incremental') }}

with f_domain_reseller_sold_ni_opportunity_hourly as (
    SELECT process_batch_id                                                                                              as process_batch_id
            , network_id                                                                                                          as network_id
            , mrm_rule_id                                                                                                         as mrm_rule_id
            , sum(IFF(traffic_type = 0, total_ad_opportunity, cast(0 as bigint)))                                                    as total_ad_opportunities
            , sum(IFF(traffic_type = 0, total_slot_opportunity, cast(0 as bigint)))                                                  as total_slot_opportunities
            , sum(IFF(traffic_type = 0, win_slot_opportunity, cast(0 as bigint)))                                                    as total_slot_opportunities_won
            , sum(IFF(traffic_type = 0, ad_views, cast(0 as bigint)))                                                                as net_counted_ads
            , sum(IFF(traffic_type = 0 and bitand(bit_flag, 2048) <> 2048 and
                    bitand(bit_flag, 4294967296) <> 4294967296, ad_views, cast(0 as bigint)))                                             as net_delivered_impressions
            , sum(IFF(traffic_type in (-1, 0, 1, 2), gross_ad_views, cast(0 as bigint)))                                             as gross_counted_ads
            , sum(IFF(traffic_type in (-1, 0, 1, 2) and bitand(bit_flag, 2048) <> 2048, gross_ad_views,
                    cast(0 as bigint)))                                                                                            as gross_delivered_impressions
            , sum(IFF(traffic_type = 0, clicks, cast(0 as bigint)))                                                                  as delivered_clicks
            , sum(IFF(traffic_type = 0, revenue, cast(0 as double)))                                                               as run_revenue
            , sum(IFF(traffic_type = 0, revenue - co_revenue - d_revenue,
                    cast(0 as double)))                                                                                          as gross_profit
            , sum(IFF(traffic_type = 0, co_revenue, cast(0 as double)))                                                            as content_owner_share_run_revenue
            , sum(IFF(traffic_type = 0, d_revenue, cast(0 as double)))                                                             as distributor_share_run_revenue
            , sum(IFF(traffic_type = 0, ad_views + no_ad_views, cast(0 as bigint)))                                                  as ads_selected_primary
            , sum(IFF(traffic_type = 0, ad_views - no_clicks, cast(0 as bigint)))                                                    as clickable_ad_impressions
            , sum(IFF(traffic_type = 0, can_quartile, cast(0 as bigint)))                                                            as measurable_video_ads_quartile_impressions
            , sum(IFF(traffic_type = 0, first_quartile, cast(0 as bigint)))                                                          as video_ads_25_percent_complete
            , sum(IFF(traffic_type = 0, middle_quartile, cast(0 as bigint)))                                                         as video_ads_50_percent_complete
            , sum(IFF(traffic_type = 0, third_quartile, cast(0 as bigint)))                                                          as video_ads_75_percent_complete
            , sum(IFF(traffic_type = 0, complete_quartile, cast(0 as bigint)))                                                       as video_ads_100_percent_complete
            , event_date                                                                                                          as event_date
        from {{ref('f_order_hourly')}}
        where process_batch_id = {{ var('var_current_batch_id') }}
            and (sales_channel = 3 -- only keep reseller sold 
            and transaction_type in ('CRO', 'R') -- only report for CRO, R
            and hylda_replacement_impression_forfeits = 0 -- reduce all zero rows 
            )                             -- for delivery metrics
        or (total_ad_opportunity <> 0 or total_slot_opportunity <> 0 or
            win_slot_opportunity <> 0) --  for opportunity metrics
        group by 1, 2, 3, 23
)

select * from f_domain_reseller_sold_ni_opportunity_hourly