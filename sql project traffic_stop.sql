select * from db1.traffic_stops
select country_name,vehicle_number,violation_raw from db1.traffic_stops
where violation_raw="drunk driving"
limit 10;
select vehicle_number,count(distinct vehicle_number) as search_count 
from db1.traffic_stops
where search_conducted=1
group by vehicle_number
order by search_count desc
limit 10;
SELECT CASE 
    WHEN driver_age BETWEEN 18 AND 25 THEN '18–25'
    WHEN driver_age BETWEEN 26 AND 35 THEN '26–35'
    WHEN driver_age BETWEEN 36 AND 45 THEN '36–45'
    WHEN driver_age BETWEEN 46 AND 60 THEN '46–60'
    ELSE '60+' 
  END AS driver_age_group,COUNT(*) AS total_stops,
  SUM(CASE WHEN is_arrested = 1 THEN 1 ELSE 0 END) AS arrests
FROM db1.traffic_stops
GROUP BY driver_age_group
ORDER BY arrests DESC;
select country_name,driver_gender,count(*) as gender_distribution
from db1.traffic_stops
group by country_name,driver_gender
order by country_name,driver_gender desc;
SELECT 
  driver_race,
  driver_gender,
  COUNT(*) AS total_stops,
  SUM(search_conducted) AS total_searches,
  ROUND(SUM(search_conducted) / COUNT(*) * 100, 2) AS search_rate
FROM db1.traffic_stops
GROUP BY driver_race, driver_gender
ORDER BY search_rate DESC;
SELECT 
  stop_date,
  HOUR(stop_time) AS hour_of_day,
  COUNT(*) AS total_stops
FROM db1.traffic_stops
GROUP BY hour_of_day,stop_date
ORDER BY total_stops DESC;
select violation,
round(avg(stop_duration),2) as avg_stop_duration
from db1.traffic_stops
group by violation
order by avg_stop_duration;
SELECT
CASE
WHEN HOUR(STOP_TIME) BETWEEN 6 AND 17 THEN 'DAY'
ELSE 'NIGHT'
END AS TIME_PERIOD,
COUNT(*) AS TOTAL_STOPS_DAYNIGHT,
SUM(IS_ARRESTED) AS TOTAL_ARREST,
ROUND(SUM(IS_ARRESTED)/COUNT(*)*100,2) AS ARREST_RATE
FROM DB1.TRAFFIC_STOPS
GROUP BY TIME_PERIOD
ORDER BY ARREST_RATE DESC;
SELECT 
  violation,
  COUNT(*) AS total_stops,
  SUM(search_conducted) AS total_searches,
  ROUND(SUM(search_conducted) / COUNT(*) * 100, 2) AS search_rate,
  SUM(IS_ARRESTED) AS total_arrests,
  ROUND(SUM(IS_ARRESTED) / COUNT(*) * 100, 2) AS arrest_rate
FROM DB1.traffic_stops
GROUP BY violation
ORDER BY search_rate DESC, arrest_rate DESC;
SELECT VIOLATION,COUNT(VIOLATION) AS TOTL_VIOLATION,
SUM(SEARCH_CONDUCTED) AS TOTAL_SEARCH,
SUM(IS_ARRESTED) AS TOTAL_ARREST
FROM DB1.TRAFFIC_STOPS
GROUP BY VIOLATION
ORDER BY TOTAL_SEARCH,TOTAL_ARREST DESC;
SELECT COUNTRY_NAME,VIOLATION_RAW,COUNT(*),
SUM(IS_ARRESTED) AS TOTAL_ARREST,
ROUND(SUM(IS_ARRESTED)/COUNT(*)*100,2) AS ARREST_RATE
FROM DB1.TRAFFIC_STOPS
GROUP BY COUNTRY_NAME,VIOLATION_RAW
ORDER BY ARREST_RATE DESC,COUNTRY_NAME DESC;
SELECT COUNTRY_NAME,COUNT(*) AS TOTAL_STOPS,
SUM(SEARCH_CONDUCTED) AS TOTAL_SEARCH
FROM DB1.TRAFFIC_STOPS
GROUP BY COUNTRY_NAME
ORDER BY TOTAL_STOPS  DESC;
SELECT COUNTRY_NAME,COUNT(*),
SUM(CASE WHEN VIOLATION_RAW LIKE 'DRUNK%' THEN 1 ELSE 0 END) AS TOTAL_DRUG_STOPS,
ROUND(SUM(CASE WHEN VIOLATION_RAW LIKE 'DRUNK%' THEN 1 ELSE 0 END)/COUNT(*)*100,2) AS DRUG_RATE
FROM DB1.TRAFFIC_STOPS
GROUP BY COUNTRY_NAME
ORDER BY TOTAL_DRUG_STOPS DESC,DRUG_RATE DESC;
SELECT VIOLATION,COUNT(*),
SUM(CASE WHEN DRIVER_AGE<25 THEN 1 ELSE 0 END) AS YOUNG_DRIVERS
FROM DB1.TRAFFIC_STOPS
GROUP BY VIOLATION
ORDER BY YOUNG_DRIVERS;
SELECT VIOLATION,COUNT(*) AS TOTAL_STOPS,
SUM(SEARCH_CONDUCTED) AS SEARCHES,
SUM(IS_ARRESTED) AS ARRESTS,
ROUND(SUM(SEARCH_CONDUCTED)/COUNT(*)*100,2) AS SEARCH_RATE,
ROUND(SUM(IS_ARRESTED)/COUNT(*)*100,2) AS ARREST_RATE
FROM DB1.TRAFFIC_STOPS
GROUP BY VIOLATION
ORDER BY SEARCH_RATE DESC,ARREST_RATE DESC;
SELECT
  country_NAME,
  year,
  total_stops,
  total_arrests,
  SUM(total_stops) OVER (PARTITION BY country_NAME ORDER BY year DESC) AS cumulative_stops,
  SUM(total_arrests) OVER (PARTITION BY country_NAME ORDER BY year DESC) AS cumulative_arrests
FROM (
  SELECT
    country_NAME,
    EXTRACT(YEAR FROM stop_date) AS year,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN IS_ARRESTED = 'Yes' THEN 1 ELSE 0 END) AS total_arrests
  FROM DB1.traffic_stops
  GROUP BY country_NAME, EXTRACT(YEAR FROM stop_date)
) AS yearly_data;
SELECT
  d.age_group,
  d.driver_race,
  v.violation,
  COUNT(*) AS violation_count
FROM (
  SELECT
    driver_age,
    driver_race,
    violation,
    CASE
      WHEN driver_age < 18 THEN 'Under 18'
      WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
      WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'
      WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'
      ELSE '60+'
    END AS age_group
  FROM db1.traffic_stops
) AS d
JOIN (
  SELECT
    driver_age,
    driver_race,
    violation
  FROM db1.traffic_stops
) AS v
ON d.driver_age = v.driver_age AND d.driver_race = v.driver_race AND d.violation = v.violation
GROUP BY d.age_group, d.driver_race, v.violation
ORDER BY d.age_group desc, violation_count DESC;
SELECT
  t.stop_year,
  t.stop_month,
  t.stop_hour,
  COUNT(*) AS total_stops
FROM (
  SELECT
    violation,
    stop_date,
    EXTRACT(YEAR FROM stop_date) AS stop_year,
    EXTRACT(MONTH FROM stop_date) AS stop_month,
    EXTRACT(HOUR FROM stop_time) AS stop_hour
  FROM db1.traffic_stops
) AS t
GROUP BY t.stop_year, t.stop_month, t.stop_hour
ORDER BY t.stop_year, t.stop_month, t.stop_hour,total_stops;
SELECT
  violation,
  COUNT(*) AS total_stops,
  SUM(CASE WHEN search_conducted = 'Yes' THEN 1 ELSE 0 END) AS total_searches,
  SUM(CASE WHEN is_arrested= 'Yes' THEN 1 ELSE 0 END) AS total_arrests,
  ROUND(100.0 * SUM(CASE WHEN search_conducted = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS search_rate,
  ROUND(100.0 * SUM(CASE WHEN is_arrested = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate,
  RANK() OVER (ORDER BY SUM(CASE WHEN search_conducted = 'Yes' THEN 1 ELSE 0 END) DESC) AS search_rank,
  RANK() OVER (ORDER BY SUM(CASE WHEN is_arrested = 'Yes' THEN 1 ELSE 0 END) DESC) AS arrest_rank
FROM db1.traffic_stops
GROUP BY violation
ORDER BY search_rate DESC, arrest_rate DESC;
SELECT
  country_name,
  CASE
    WHEN driver_age < 18 THEN 'Under 18'
    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
    WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'
    WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'
    ELSE '60+'
  END AS age_group,
  driver_gender,
  driver_race,
  COUNT(*) AS total_drivers
FROM db1.traffic_stops
GROUP BY
  country_name,
  CASE
    WHEN driver_age < 18 THEN 'Under 18'
    WHEN driver_age BETWEEN 18 AND 25 THEN '18-25'
    WHEN driver_age BETWEEN 26 AND 40 THEN '26-40'
    WHEN driver_age BETWEEN 41 AND 60 THEN '41-60'
    ELSE '60+'
  END,
  driver_gender,
  driver_race
ORDER BY country_name, age_group, total_drivers DESC;
SELECT
  violation,
  COUNT(*) AS total_stops,
  SUM(CASE WHEN is_arrested = 'Yes' THEN 1 ELSE 0 END) AS total_arrests,
  ROUND(100.0 * SUM(CASE WHEN is_arrested = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate,
  RANK() OVER (ORDER BY ROUND(100.0 * SUM(CASE WHEN is_arrested = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) DESC) AS arrest_rank
FROM db1.traffic_stops
GROUP BY violation
ORDER BY arrest_rate DESC
LIMIT 5;