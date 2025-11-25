/* Part C: Analytical Queries
File: 2_analytical_queries.sql
Description: FinOps reporting queries based on the unified view.
*/

-- 1. Monthly Spend by Cloud Provider
SELECT 
    DATE_TRUNC('month', date) AS billing_month,
    provider,
    SUM(cost) AS total_spend
FROM unified_cloud_billing
GROUP BY 1, 2
ORDER BY 1 DESC, 2;

/* Sample Output:
| billing_month | provider | total_spend |
|:--------------|:---------|:------------|
| 2025-12-01    | AWS      | 15,402.50   |
| 2025-12-01    | GCP      | 12,100.25   |
*/


-- 2. Monthly Spend by Team + Environment
SELECT 
    DATE_TRUNC('month', date) AS billing_month,
    team,
    env,
    SUM(cost) AS total_spend
FROM unified_cloud_billing
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC;

/* Sample Output:
| billing_month | team | env     | total_spend |
|:--------------|:-----|:--------|:------------|
| 2025-12-01    | Core | prod    | 8,500.00    |
| 2025-12-01    | Web  | staging | 2,100.50    |
*/


-- 3. Top 5 Most Expensive Services (Cross-Cloud)
SELECT 
    service,
    SUM(cost) AS total_annual_spend
FROM unified_cloud_billing
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;

/* Sample Output:
| service | total_annual_spend |
|:--------|:-------------------|
| EC2     | 145,000.00         |
| RDS     | 85,200.00          |
| EKS     | 40,000.00          |
*/