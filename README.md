# 📊 Layoffs Data Pipeline

## 🔍 Overview
This project builds an end-to-end data pipeline to process layoff datasets from multiple CSV files into a clean, structured SQL table.

---

## 🎯 Problem
Raw data was spread across multiple CSV files with inconsistent formats, missing values, and data quality issues.

---

## 🛠️ Solution
The pipeline:
- Combines multiple CSV files
- Cleans and standardizes data
- Handles missing values and inconsistencies
- Applies validation rules
- Loads cleaned data into MySQL
- Logs pipeline execution

---

## ⚙️ Tech Stack
- Python (pandas, os, logging)
- MySQL
- SQLAlchemy

---

## 🔄 Pipeline Steps

### 1. Data Ingestion
Reads and combines multiple CSV files.

### 2. Data Cleaning
- Standardizes column names
- Handles missing values
- Cleans string fields
- Converts data types

### 3. Data Validation
- Removes invalid records
- Ensures data consistency
- Drops duplicates

### 4. Data Loading
Stores cleaned data into MySQL table `layoffs_clean`.

### 5. Logging
Tracks pipeline execution and errors.

---

## 📦 Output
Clean dataset ready for analysis and reporting.

---

## 💡 Sample SQL Queries

```sql
SELECT country, SUM(total_laid_off)
FROM layoffs_clean
GROUP BY country;

SELECT company, SUM(total_laid_off) AS total
FROM layoffs_clean
GROUP BY company
ORDER BY total DESC
LIMIT 5;
