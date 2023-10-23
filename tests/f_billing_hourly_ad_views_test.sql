select *
 from {{ ref('f_billing_hourly') }}
where ad_views = 0