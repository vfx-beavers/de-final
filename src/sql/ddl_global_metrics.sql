-- Подметаем
DROP TABLE IF EXISTS STV2023081241__DWH.global_metrics CASCADE;

-- Создание таблицы global_metrics
CREATE TABLE STV2023081241__DWH.global_metrics(
    date_update DATE NOT NULL,
    currency_from int NOT NULL,
    amount_total  numeric(18,2) NOT NULL,
    cnt_transactions int NOT NULL,
    avg_transactions_per_account numeric(18,2) NOT NULL,
	cnt_accounts_make_transactions int NOT NULL
)
ORDER BY date_update;