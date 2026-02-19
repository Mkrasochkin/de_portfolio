insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select
    dc.date_id,
    item_id,
    customer_id,
    city_id,
    quantity
    payment_amount,
    CASE 
        WHEN EXISTS (SELECT 1 FROM information_schema.columns 
            WHERE table_schema = 'staging' 
            AND table_name = 'user_order_log' 
            AND column_name = 'status') THEN uol.status
        ELSE 'shipped' 
    END as status
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';

