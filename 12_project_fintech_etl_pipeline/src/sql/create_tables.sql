-- Создание таблицы транзакций
CREATE TABLE VT251107138D4A__STAGING.transactions (
    operation_id varchar(60) NULL,
	account_number_from INTEGER NULL,
	account_number_to INTEGER NULL,
	currency_code INTEGER NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount INTEGER NULL,
	transaction_dt timestamp NULL
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES
PARTITION BY transaction_dt:: DATE
GROUP BY
calendar_hierarchy_day(transaction_dt:: date, 3, 2);

-- Создание проекции для таблицы транзакций
CREATE PROJECTION VT251107138D4A__STAGING.transactions_projection_by_date (
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
)
AS
SELECT *
FROM VT251107138D4A__STAGING.transactions
ORDER BY transaction_dt ASC
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES KSAFE 1;


-- Создание таблицы курсов валют
CREATE TABLE VT251107138D4A__STAGING.currencies (
    date_update timestamp NULL,
	currency_code INTEGER NULL,
	currency_code_with INTEGER NULL,
	currency_with_div numeric(5, 3) NULL
)
ORDER BY date_update
SEGMENTED BY HASH(date_update) ALL NODES
PARTITION BY date_update:: DATE
GROUP BY
calendar_hierarchy_day(date_update:: date, 3, 2);

-- Создание проекции для таблицы курсов валют
CREATE PROJECTION VT251107138D4A__STAGING.currencies_projection_by_date (
    date_update,
    currency_code,
    currency_code_with,
    currency_with_div
)
AS
SELECT *
FROM VT251107138D4A__STAGING.currencies
ORDER BY date_update ASC
SEGMENTED BY HASH(date_update) ALL NODES KSAFE 1;


-- Создание витрины
CREATE TABLE VT251107138D4A__DWH.global_metrics (
    date_update DATE NOT NULL,
    currency_from INTEGER NOT NULL,
    amount_total NUMERIC(18, 5) NULL,
    cnt_transactions INTEGER NULL,
    avg_transactions_per_account NUMERIC(18, 5) NULL,
    cnt_accounts_make_transactions INTEGER NULL  
)
ORDER BY date_update, currency_from  
SEGMENTED BY HASH(date_update, currency_from) ALL NODES  
PARTITION BY date_update::DATE  
GROUP BY calendar_hierarchy_day(date_update::DATE, 3, 2);