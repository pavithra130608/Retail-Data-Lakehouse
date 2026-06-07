-- Databricks notebook source
--Revenue by Status
SELECT *
FROM gold_revenue_status
ORDER BY total_revenue DESC;
--Top 10 Customers
SELECT *
FROM gold_top_customers
LIMIT 10;
--Monthly Revenue Trend
SELECT *
FROM gold_monthly_revenue
ORDER BY year, month;

