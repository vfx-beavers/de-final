-- Подметаем
DROP TABLE IF EXISTS STV2023081241__STAGING.transactions CASCADE;
DROP PROJECTION IF EXISTS STV2023081241__STAGING.transactions_proj CASCADE;

-- Создание таблицы transactions
CREATE TABLE STV2023081241__STAGING.transactions (
    operation_id varchar(60) NOT NULL,
	account_number_from int NOT NULL,
	account_number_to int NOT NULL,
	currency_code int NOT NULL,
	country varchar(30) NOT NULL,
	status varchar(30) NOT NULL,
	transaction_type varchar(30) NOT NULL,
	amount int NOT NULL,
	transaction_dt TIMESTAMP(3) NOT NULL
) 
SEGMENTED BY hash(operation_id,transaction_dt) ALL NODES;

-- Создание проекции таблицы transactions
CREATE PROJECTION STV2023081241__STAGING.transactions_proj (
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
  SELECT transactions.operation_id,
 		transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
  FROM STV2023081241__STAGING.transactions
  ORDER BY transactions.transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt) ALL NODES;
