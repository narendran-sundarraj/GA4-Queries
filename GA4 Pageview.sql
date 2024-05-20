select countif(event_name="page_view") as Views,
    coalesce(collected_traffic_source.manual_source,'(direct)') as source_session, 
    coalesce(collected_traffic_source.manual_medium,'(none)') as source_medium,
    event_date as Date,
    geo.region as Region,
    geo.country as Country,
    device.category as Device,
    (select value.string_value from unnest(event_params) where key = 'Interface_Brand') as Brand,
    (select value.string_value from unnest(event_params) where key = 'Customer_Status_Event') as CS
from
`steam-mantis-108908.analytics_216367908.events_*` where
    _table_suffix between '20240320' AND '20240402'
group by source_session,source_medium,CS, Date, Region, Country, Device, Brand