-- Подметаем
DROP TABLE IF EXISTS STV2023081241__STAGING.currencies CASCADE;
DROP PROJECTION IF EXISTS STV2023081241__STAGING.currencies_proj CASCADE;

-- Создание таблицы currencies
CREATE TABLE IF NOT EXISTS STV2023081241__STAGING.currencies (
	date_update TIMESTAMP(0) NOT NULL,
	currency_code int NOT NULL,
	currency_code_with int NOT NULL,
	currency_with_div NUMERIC(5, 3) NOT NULL
)
ORDER BY date_update;

-- Создание проекции таблицы currencies
CREATE PROJECTION STV2023081241__STAGING.currencies_proj (
  date_update,
  currency_code,
  currency_code_with,
  currency_with_div
 )
AS
  SELECT * FROM STV2023081241__STAGING.currencies
  ORDER BY date_update;

