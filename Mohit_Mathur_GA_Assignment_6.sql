#Finding Metrics 1 by 1 and joining them together

#For Metric DAU
drop table if exists metrics_dau;

CREATE TEMPORARY TABLE metrics_dau AS
SELECT event_dt As 'Date',
COUNT(DISTINCT player_id) As 'DAU'
FROM (SELECT player_id, device_type, date(event_ts) As event_dt
	FROM game.raw_event re
	WHERE event_id=1004
	AND event='session' 
	AND `domain`='start'
	GROUP BY 1,2,3
	ORDER BY 3) A
GROUP BY 1
ORDER BY 1
;

-- select * from metrics_dau;

#For Metric MAU
DROP TABLE IF EXISTS dau;

/* DAU Table */
CREATE TEMPORARY TABLE dau
AS
SELECT date(event_ts) as date,
player_id
FROM raw_event 
WHERE event_id=1004
AND event='session' 
AND `domain`='start'
GROUP BY 1,2
;

/* MAU Has 30 Day Lookback Window */
/* Use date_lookup to create 30 day view */

drop table if exists metrics_mau;

create temporary table metrics_mau AS
SELECT a.dt as Date, 
COUNT(DISTINCT d.player_id) AS MAU
FROM date_lookup a,
dau d
WHERE d.date BETWEEN DATE_ADD(a.dt, INTERVAL -29 DAY) AND a.dt 
AND a.dt >='2023-02-01'
GROUP BY 1
ORDER BY 1
;

-- select * from metrics_mau;


#Joining DAU and MAU
select mm.Date, DAU, MAU from 
metrics_dau md
join 
metrics_mau mm 
on md.Date = mm.Date;

#For Metric Installs (New Users)
drop table if exists metrics_installs;

create temporary table metrics_installs AS
select date(event_ts) as 'Date',
COUNT(DISTINCT player_id) As 'Total_Installs'
FROM (SELECT player_id, device_type, MIN(event_ts) as event_ts
	  FROM game.raw_event re
	  WHERE event_id=1002 
	  AND event='registration' 
	  AND `domain`='end'
	  GROUP BY 1,2 
	  ORDER BY 3) A
GROUP BY 1
ORDER BY 1
;

#Joining DAU, MAU and Installs
select mm.Date, DAU, MAU, Total_Installs from 
metrics_dau md
join 
metrics_mau mm
on md.Date = mm.Date
join
metrics_installs mi
on md.Date = mi.Date;

#For Metric DARPU (Daily Average Revenue Per User)
drop table if exists metrics_DARPU;

create temporary table metrics_DARPU AS
SELECT date(event_ts) as date,  
SUM(CASE WHEN event_id=4001 THEN numeric_01 ELSE 0 END)/COUNT(DISTINCT player_id) as DARPU
FROM raw_event
WHERE player_id IS NOT NULL
AND event_id=4001
AND event='shop'
GROUP BY 1
ORDER BY 1
;

-- select * from metrics_DARPU;

#Joining DAU, MAU, Installs and DARPU
select mm.Date, DAU, MAU, Total_Installs,DARPU from 
metrics_dau md
left outer join 
metrics_mau mm
on md.Date = mm.Date
left outer join 
metrics_installs mi
on md.Date = mi.Date
left outer join 
metrics_DARPU mda
on md.Date = mda.date; #DARPU for first day is 0 dollars


#For Metric Retention
/* Table Clean Up */
DROP TABLE IF EXISTS installs;
DROP TABLE IF exists dau;
DROP TABLE IF exists retention;

/* Installs Table */

CREATE TEMPORARY TABLE installs AS
SELECT device_type, 
date(min(event_ts)) AS install_date,
player_id
FROM raw_event re 
WHERE event_id=1002
AND event='registration'
AND `domain`='end'
GROUP BY 1,3
;

/* DAU Table */

CREATE TEMPORARY TABLE dau AS
SELECT date(event_ts) as dau_date,
device_type, 
a.player_id
FROM raw_event a
WHERE event_id=1004
AND event='session'
AND `domain`='start'
GROUP BY 1,2,3
;

CREATE INDEX player_id ON installs (player_id);
CREATE INDEX player_id ON dau (player_id);

/* Retention Table */

CREATE TEMPORARY TABLE retention AS
SELECT install_date, 
i.device_type, 
i.player_id, 
datediff(dau_date, install_date) as days
FROM installs i, 
dau d
where i.player_id=d.player_id
AND i.device_type=d.device_type
;





drop table if exists metrics_Retention;

create temporary table metrics_Retention AS
SELECT install_date,
100.0*COUNT(DISTINCT case when days = 30 then player_id else null end)/
      COUNT(DISTINCT case when days = 0  then player_id else null end) as D30_Retention
FROM retention
group by 1
ORDER BY 1
;

-- select * from metrics_Retention ;

#Joining DAU, MAU, Installs, DARPU and D30_Retention
drop table if exists metrics_Final

create temporary table metrics_Final as
select mm.Date, DAU, MAU, Total_Installs,DARPU, D30_Retention from 
metrics_dau md
left outer join 
metrics_mau mm
on md.Date = mm.Date
left outer join 
metrics_installs mi
on md.Date = mi.Date
left outer join 
metrics_DARPU mda
on md.Date = mda.date
left outer join
metrics_Retention mr
on md.Date = mr.install_date;

UPDATE metrics_Final SET DARPU = 0 WHERE DARPU IS NULL;

select * from metrics_Final;