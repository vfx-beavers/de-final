MERGE INTO STV2023081241__DWH.global_metrics AS global_metrics USING
    (
WITH 
all_curr_transactions AS (
    SELECT distinct operation_id, currency_code, account_number_from, amount, transaction_dt, status 
    FROM STV2023081241__STAGING.transactions
    WHERE
        transaction_dt::date='{count_date}'::date-1 AND
        account_number_from > 0 AND
		status='done' AND
        transaction_type !='authorisation'
	),
count_transactions AS (
	SELECT *, COUNT(1) OVER (partition by operation_id) AS COUNT FROM all_curr_transactions
	),
in_trans AS (
	SELECT * FROM count_transactions WHERE COUNT = 1
	),
currencies AS (
	SELECT * FROM STV2023081241__STAGING.currencies WHERE date_update = '{count_date}' AND currency_code_with = 420
	),
tmp AS (
	SELECT trans.operation_id, trans.account_number_from, trans.currency_code, 
	   trans.amount * coalesce(currencies.currency_with_div,1) AS usd_amount, 
	   date(trans.transaction_dt) AS trn_date
	FROM in_trans AS trans LEFT JOIN currencies 
  	ON trans.transaction_dt = currencies.date_update 
  	AND trans.currency_code = currencies.currency_code
	)
SELECT tmp.trn_date AS date_update, 
	   tmp.currency_code AS currency_from,
	   SUM(tmp.usd_amount) AS amount_total, 
	   COUNT(tmp.operation_id) AS cnt_transactions,
	   COUNT(tmp.operation_id)/COUNT(distinct tmp.account_number_from) AS avg_transactions_per_account,
	   COUNT(distinct tmp.account_number_from) AS cnt_accounts_make_transactions
FROM tmp 
GROUP BY date_update, currency_from) AS xyz
    
ON xyz.date_update=global_metrics.date_update

WHEN MATCHED THEN UPDATE SET
                currency_from = xyz.currency_from,
                amount_total = xyz.amount_total, 
                cnt_transactions = xyz.cnt_transactions,  
                avg_transactions_per_account = xyz.avg_transactions_per_account,
                cnt_accounts_make_transactions = xyz.cnt_accounts_make_transactions

WHEN NOT MATCHED THEN INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
    VALUES (xyz.date_update, xyz.currency_from, xyz.amount_total, xyz.cnt_transactions, xyz.avg_transactions_per_account, xyz.cnt_accounts_make_transactions);