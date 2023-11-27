/* This is a query for CLV project. It contains 2 CTEs and a main query.
* It uses 'raw_events' as a source table. Returns a table with 14 rows and 14 columns.
* Read inline comments to gain understanding on how it works. */

-- In the 1st CTE we cast the date from STRING to DATE format and for each user_id retract the date of the earliest event as the first visit.
-- We will create cohorts based on this date in the main query.
-- As a result of the CTE we'll get a table with 1 row for each user: pseudo_id and corresponding date.
WITH early_date AS (
  SELECT user_pseudo_id, MIN(CAST(event_date AS DATE FORMAT 'YYYYMMDD')) as first_visit_date
  FROM `tc-da-1.turing_data_analytics.raw_events`
  GROUP BY user_pseudo_id
),
-- The 2nd CTE extracts only 4 necessary columns from the source table.
-- It joins source table with the 1st CTE and adds the same date of the earliest event for all events of a user
-- as a 'cohort_start' feature, which we will use as a unique cohort identifier.
user_cohort AS(
  SELECT  CAST(event_date AS DATE FORMAT 'YYYYMMDD') AS event_date,
          user_pseudo_id,
          event_name,
          purchase_revenue_in_usd,
          -- Truncate first_visit_date to a week, so we would be able to group records into cohorts on the next step.
          DATE_TRUNC(ed.first_visit_date, week) AS cohort_start
  FROM `tc-da-1.turing_data_analytics.raw_events`
  JOIN early_date AS ed
  USING(user_pseudo_id)
)
-- The main query groups all the rows by 'cohort_start' feature (column 1).
-- It counts DISTINCT users for each cohort, since a user can have a lot of corresponding events (column 2).
-- It calculates the SUM of purchases maden by users from the same cohorts during weeks 0-12 and divides by the number of users (columns 3-14).
-- Week_0 is the week of the first visit.
SELECT  cohort_start,
        COUNT(DISTINCT user_pseudo_id) AS total_users,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN cohort_start AND DATE_ADD(cohort_start, INTERVAL 6 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_0,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 1 WEEK) AND DATE_ADD(cohort_start, INTERVAL 13 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_1,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 2 WEEK) AND DATE_ADD(cohort_start, INTERVAL 20 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_2,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 3 WEEK) AND DATE_ADD(cohort_start, INTERVAL 27 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_3,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 4 WEEK) AND DATE_ADD(cohort_start, INTERVAL 34 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_4,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 5 WEEK) AND DATE_ADD(cohort_start, INTERVAL 41 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_5,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 6 WEEK) AND DATE_ADD(cohort_start, INTERVAL 48 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_6,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 7 WEEK) AND DATE_ADD(cohort_start, INTERVAL 55 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_7,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 8 WEEK) AND DATE_ADD(cohort_start, INTERVAL 62 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_8,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 9 WEEK) AND DATE_ADD(cohort_start, INTERVAL 69 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_9,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 10 WEEK) AND DATE_ADD(cohort_start, INTERVAL 76 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_10,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 11 WEEK) AND DATE_ADD(cohort_start, INTERVAL 83 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_11,
        SUM(
          CASE  WHEN event_name = 'purchase'
                  AND event_date BETWEEN DATE_ADD(cohort_start, INTERVAL 12 WEEK) AND DATE_ADD(cohort_start, INTERVAL 90 DAY)
                THEN purchase_revenue_in_usd ELSE 0
          END
        )/COUNT(DISTINCT user_pseudo_id) AS week_12
FROM user_cohort
GROUP BY cohort_start
ORDER BY cohort_start;
