/* Part C: SQL Transformations
File: 1_transform_unified_view.sql

Description: 
This query creates the "Silver Layer" view. It standardizes the column names 
between AWS (account_id) and GCP (project_id) and unions the datasets 
into a single master billing table.
*/

CREATE OR REPLACE VIEW unified_cloud_billing AS

WITH aws_clean AS (
    SELECT 
        date,
        'AWS' AS provider,
        account_id AS billing_entity,
        service,
        -- Handle missing tags to prevent data loss in aggregation
        COALESCE(team, 'Unassigned') AS team,
        COALESCE(env, 'Unknown') AS env,
        CAST(cost_usd AS DECIMAL(10,2)) AS cost
    FROM raw_aws_billing
),

gcp_clean AS (
    SELECT 
        date,
        'GCP' AS provider,
        project_id AS billing_entity,
        -- Note: In production, we would apply a CASE statement here 
        -- to map GCP 'EC2' -> 'Compute Engine' if needed.
        service,
        COALESCE(team, 'Unassigned') AS team,
        COALESCE(env, 'Unknown') AS env,
        CAST(cost_usd AS DECIMAL(10,2)) AS cost
    FROM raw_gcp_billing
)

-- Combine datasets
SELECT * FROM aws_clean
UNION ALL
SELECT * FROM gcp_clean;