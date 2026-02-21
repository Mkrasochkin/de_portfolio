-- Таблица group_log
CREATE TABLE STV2025061617__STAGING.group_log (
	group_id INT,
	user_id INT,
	user_id_from INT,
	event VARCHAR(10) CHECK (event in ('create', 'add', 'leave')),
	datetime TIMESTAMP,
	CONSTRAINT fk_group_log_groups FOREIGN KEY (group_id) REFERENCES STV2025061617__STAGING.groups(id),
	CONSTRAINT fk_group_log_users FOREIGN KEY (user_id) REFERENCES STV2025061617__STAGING.users(id),
	CONSTRAINT fk_group_log_users_from FOREIGN KEY (user_id_from) REFERENCES STV2025061617__STAGING.users(id)
)
ORDER BY datetime, group_id, user_id  
SEGMENTED BY HASH(group_id) ALL NODES  
PARTITION BY datetime::date  
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);


-- Таблица l_user_group_activity
DROP TABLE IF EXISTS STV2025061617__DWH.l_user_group_activity;

CREATE TABLE STV2025061617__DWH.l_user_group_activity
(
	hk_l_user_group_activity int PRIMARY KEY,
	hk_user_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_user  REFERENCES STV2025061617__DWH.h_users (hk_user_id),
	hk_group_id INT NOT NULL CONSTRAINT fk_l_user_group_activity_group REFERENCES STV2025061617__DWH.h_groups (hk_group_id),
	load_dt datetime,
    load_src varchar(20)
)
ORDER BY load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


-- Скрипт миграции данных из group_log в l_user_group_activity
INSERT INTO STV2025061617__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
	hash(hu.hk_user_id, hg.hk_group_id) AS hk_l_user_group_activity,
	hu.hk_user_id,
	hg.hk_group_id,
	now() as load_dt,
	's3' as load_src
from STV2025061617__STAGING.group_log as gl
left join STV2025061617__DWH.h_users hu on gl.user_id = hu.user_id 
left join STV2025061617__DWH.h_groups hg on gl.group_id = hg.group_id;


-- Создание таблицы-сателлита  
CREATE TABLE STV2025061617__DWH.s_auth_history  
(
    hk_l_user_group_activity INT NOT NULL CONSTRAINT fk_s_auth_history_user_group 
        REFERENCES STV2025061617__DWH.l_user_group_activity (hk_l_user_group_activity),
    user_id_from INT,
    event VARCHAR(10) NOT NULL,
    event_dt TIMESTAMP NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(20) NOT NULL  
)
ORDER BY event_dt  
SEGMENTED BY hk_l_user_group_activity ALL NODES  
PARTITION BY event_dt::date  
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);


-- Наполнение сателлита данными  
INSERT INTO STV2025061617__DWH.s_auth_history(
    hk_l_user_group_activity, 
    user_id_from,
    event,
    event_dt,
    load_dt,
    load_src  
)
SELECT 
    luga.hk_l_user_group_activity,
    CASE 
        WHEN gl.user_id_from = 0 THEN NULL 
        ELSE gl.user_id_from 
    END AS user_id_from,
    gl.event,
    gl.datetime AS event_dt,
    now() AS load_dt,
    's3' AS load_src  
FROM 
    STV2025061617__STAGING.group_log gl  
LEFT JOIN 
    STV2025061617__DWH.h_groups hg ON gl.group_id = hg.group_id  
LEFT JOIN 
    STV2025061617__DWH.h_users hu ON gl.user_id = hu.user_id  
LEFT JOIN 
    STV2025061617__DWH.l_user_group_activity luga ON 
        hg.hk_group_id = luga.hk_group_id AND 
        hu.hk_user_id = luga.hk_user_id;





-- CTE для ответов бизнесу
with user_group_log as (
    select 
        luga.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users  
    from 
        STV2025061617__DWH.l_user_group_activity luga  
    left join 
        STV2025061617__DWH.s_auth_history sah on luga.hk_l_user_group_activity = sah.hk_l_user_group_activity  
    where 
        sah.event = 'add'  
        and luga.hk_group_id in (
            select hk_group_id 
            from STV2025061617__DWH.h_groups  
            order by registration_dt  
            limit 10  
        )
    group by 
        luga.hk_group_id  
),
user_group_messages as (
    select 
        lgd.hk_group_id,
        count(distinct lum.hk_user_id) as cnt_users_in_group_with_messages  
    from 
        STV2025061617__DWH.l_groups_dialogs lgd  
    left join 
        STV2025061617__DWH.l_user_message lum on lgd.hk_message_id = lum.hk_message_id  
    where 
        lgd.hk_group_id in (
            select hk_group_id 
            from STV2025061617__DWH.h_groups  
            order by registration_dt  
            limit 10  
        )
    group by 
        lgd.hk_group_id  
)
select  
    ugl.hk_group_id,
    ugl.cnt_added_users,
    COALESCE(ugm.cnt_users_in_group_with_messages, 0) as cnt_users_in_group_with_messages,
    CASE 
        WHEN ugl.cnt_added_users > 0 
        THEN ROUND(COALESCE(ugm.cnt_users_in_group_with_messages, 0)::float / ugl.cnt_added_users, 4) 
        ELSE 0 
    END as group_conversion  
from user_group_log as ugl  
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id  
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc;



