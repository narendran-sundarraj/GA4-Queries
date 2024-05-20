with GA as (SELECT 
    user_pseudo_id,
    event_date,
    concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id ,
    collected_traffic_source.manual_source as source, 
    collected_traffic_source.manual_medium as medium,
from
`steam-mantis-108908.analytics_216367908.events_*` 
    where
    _table_suffix between '20240320' AND '20240402' AND event_name = "session_start"
),

events as (
select
    event_date as Date,
    geo.region as Region,
    geo.country as Country,
    device.category as Device,
    (select value.string_value from unnest(event_params) where key = 'Interface_Brand') as Brand,
    (select value.string_value from unnest(event_params) where key = 'EventAction') as EA,
    (select value.string_value from unnest(event_params) where key = 'Customer_Status_Event') as CS,
    concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) as session_id,
    count(event_name) as events
from
`steam-mantis-108908.analytics_216367908.events_*` where
    _table_suffix between '20240320' AND '20240402' AND REGEXP_CONTAINS((select value.string_value from unnest(event_params) where key = 'EventAction'), 'NRC|NDC|Registration Form Open|Registration Submit|Registration Failed|Registration Complete|page_view') OR event_name = "page_view"
    group by session_id, EA, CS, Date, Region, Country, Device, Brand

)

select Date, Region,Country,Device,Brand,coalesce(EA,'Pageview') as EA,CS, coalesce(source,'(direct)') as source_session,
    coalesce(medium,'(none)') as medium_session,events.session_id from events join GA on (events.session_id=ga.session_id)

    