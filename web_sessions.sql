WITH session_data AS (
    SELECT
        session_id,
        session_startdate,
        session.market_code_entry AS market,
        TIMESTAMP_DIFF(session.endtime, session.starttime, MINUTE) AS session_duration_length,
        CASE WHEN ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY session_id) = 1 
             THEN COUNT(DISTINCT session_id) OVER (PARTITION BY client_id)
             ELSE NULL
        END AS distinct_session_count,
        FORMAT_TIMESTAMP('%Y-%m', session_startdate) AS year_month,
        CASE WHEN logged_in = TRUE THEN 'Logged In' ELSE 'Not Logged In' END AS login_status
    FROM
        `ingka-online-analytics-prod.episod_web.sessions`
    WHERE
        session_startdate >= '2023-01-01'
        AND session_startdate <= '2023-01-31'
)
SELECT 
    market, 
    year_month,
    login_status,
    COUNT(DISTINCT session_id) AS total_sessions,
    AVG(session_duration_length) AS average_session_duration,
    AVG(distinct_session_count) AS average_distinct_session_count
FROM 
(
    SELECT
        session_id,
        market,
        year_month,
        login_status,
        MAX(distinct_session_count) AS distinct_session_count,
        session_duration_length 
    FROM 
        session_data
    GROUP BY 
        session_id, session_duration_length, market, year_month, login_status
) aggregated_data
GROUP BY market, year_month, login_status
ORDER BY market, year_month, login_status;
