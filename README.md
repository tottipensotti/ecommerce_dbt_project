# 🧠 dbt Analytics Challenge – E-commerce Data Modeling

Welcome to the dbt challenge! This exercise is designed to test your skills as an **Analytics Engineer / Data Engineer**, with a focus on **data modeling**, **dbt best practices**, and **analytical thinking** using an e-commerce dataset.

---

## 🗂 Project Structure

This project uses dbt and DuckDB to model data from three raw CSV sources:

- `users.csv`
- `orders.csv`
- `sessions.csv`

These are located in the `data/` folder and loaded using `dbt seed`.

---

## 🧪 Challenge Overview

Your task is to:

### 1. **Seed the Raw Data**
Use `dbt seed` to load the CSVs into the `raw` schema.

### 2. **Build Staging Models**
Create `stg_users`, `stg_orders`, and `stg_sessions` models that:
- Rename fields to `snake_case`
- Cast fields to correct types
- Add surrogate keys if necessary
- Clean / deduplicate data

### 3. **Build a Core Model**
Create a `fct_user_orders_summary` model that:
- Joins `users` and `orders`
- Aggregates order data per user:
  - Total orders
  - Total amount spent
  - First and last order timestamps

### 4. **(Optional) Build a Marketing Model**
Create a `fct_user_sessions` model that:
- Joins users with their sessions
- Shows session counts and most common source per user

---

## ✅ Goals

- Demonstrate data modeling logic using dbt
- Showcase modular SQL design
- Use `ref()` correctly to connect models
- Follow dbt best practices for naming, folder structure, and config
- Use DuckDB for local execution (no cloud resources required)

---

## ▶️ How to Run It

1. Install dbt with DuckDB adapter:
   ```bash
   pip install dbt-duckdb
  ```
2. Initialize the project:
  ```bash
  dbt debug
  dbt seed
  dbt run
  dbt test
  dbt docs generate && dbt docs serve
  ```
3. If using a custom `profiles.yml`, run with:
  ```bash
  dbt run --profiles-dir ./dbt/ecommerce_project
  ```