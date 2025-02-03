Create Database Project3;
show databases; 
USE Project3;  

CREATE TABLE users ( 
                        user_id INT ,
                        created_at VARCHAR(100),
                        company_id INT ,
                        language VARCHAR(50) ,
                        activated_at VARCHAR(100),
                        state VARCHAR(50)
                        );
                        
SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM users;

#change the type of created at and activated at by creating a temp column

ALTER TABLE users add column  temp_created_at datetime;

UPDATE users SET temp_created_at = STR_TO_DATE(created_at, '%d-%m-%Y %H:%i');

ALTER table users DROP COLUMN created_at;

ALTER TABLE users  CHANGE COLUMN  temp_created_at created_at datetime;

ALTER TABLE users add column  temp_created_at datetime;

UPDATE users SET temp_created_at = STR_TO_DATE(activated_at, '%d-%m-%Y %H:%i');

ALTER table users DROP COLUMN activated_at;

ALTER TABLE users  CHANGE COLUMN  temp_created_at activated_at datetime;


CREATE TABLE events ( 
                        user_id INT ,
                        occurred_at VARCHAR(100) ,
                        event_type VARCHAR(50) ,
						event_name VARCHAR(100) ,
                        location VARCHAR(50) ,
                        device VARCHAR(50) ,
                        user_type INT 
                        );
            
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
INTO TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT * FROM events;

ALTER TABLE events add column  temp_created_at datetime;

UPDATE events SET temp_created_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

ALTER table events DROP COLUMN occurred_at;

ALTER TABLE events  CHANGE COLUMN  temp_created_at occurred_at datetime;

CREATE TABLE emailevents ( 
                        user_id INT ,
                        occurred_at VARCHAR(100) ,
                        action VARCHAR(100) ,
                        user_type INT 
                        );

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
INTO TABLE emailevents
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

ALTER TABLE emailevents add column  temp_created_at datetime;

UPDATE emailevents SET temp_created_at = STR_TO_DATE(occurred_at, '%d-%m-%Y %H:%i');

ALTER table emailevents DROP COLUMN occurred_at;

ALTER TABLE emailevents  CHANGE COLUMN  temp_created_at occurred_at datetime;

SELECT * FROM emailevents;

SELECT week(occurred_at) as Week,
COUNT(DISTINCT user_id)as Weekly_User_engagement
FROM events
GROUP BY week(occurred_at)
ORDER BY week(occurred_at);

SELECT sub.month_num, sub.users, SUM(sub.users) - LAG(sub.users,1) OVER() AS user_growth
FROM
(SELECT MONTH(occurred_at) AS month_num, COUNT(user_id) AS users
FROM events
GROUP BY month_num) AS sub
GROUP BY month_num;

SELECT sub1.*, sub1.users_weekly - sub1.users_prev_wk AS user_retention
FROM (SELECT sub.week_num, sub.users AS users_weekly , LAG(sub.users,1) OVER(ORDER BY week_num) AS users_prev_wk
FROM(SELECT WEEK(users.created_at) AS week_num,
            COUNT(users.user_id) AS users,
            event_type,
            state
    FROM users
    JOIN events ON users.user_id = events.user_id
    GROUP BY week_num , event_type , state
    HAVING events.event_type = 'engagement'
        AND users.state = 'active') AS sub
GROUP BY sub.week_num, sub.users) AS sub1;

SELECT emailevents.user_type,
    action,
    COUNT(emailevents.user_id) AS users
FROM
    emailevents
GROUP BY action , user_type
ORDER BY user_type;

SELECT week(occurred_at) as Weeks,
device,
COUNT(distinct user_id)as User_engagement
FROM events
GROUP BY device,
week(occurred_at)
ORDER BY week(occurred_at);

SELECT
	sub.device,
    sub.week_num,
    sub.user_engagement_device,
    sum(sub.user_engagement_device) OVER(PARTITION BY sub.week_num) AS user_engagement_wk
FROM (SELECT 
    device,
    event_type,
    COUNT(event_type) AS user_engagement_device,
    WEEK(occurred_at) AS week_num
FROM
    events
GROUP BY device , event_type , WEEK(occurred_at)
HAVING
 event_type = 'engagement') AS sub
GROUP BY sub.device, sub.week_num, sub.user_engagement_device
ORDER BY sub.week_num;

SELECT user_id,
activated_at
FROM users
WHERE activated_at > '2014-05-01'
ORDER BY user_id;

SELECT DISTINCT u.user_id,
e.occurred_at
FROM users u join events e on u.user_id = e.user_id
WHERE u.activated_at > '2014-05-01' and e.event_name = 'login'
GROUP BY week( e.occurred_at)
ORDER BY e.occurred_at;