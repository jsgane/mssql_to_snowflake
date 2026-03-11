--alter table neemba.mines.a_bronze_eventchain

--with eventchaincmtval as (
--    select * from NEEMBA.mines.a_bronze_eventchaincmtval
--)
--select * from eventchaincmtval
--where eventchaincmtid = 6573

SELECT TO_TIMESTAMP(
  REGEXP_REPLACE('May 1 2025 2:15AM', '(\\w+) (\\d) (\\d{4}) (\\d):', '\\1 0\\2 \\3 0\\4:'),
  'MON DD YYYY HH12:MIAM'
)
