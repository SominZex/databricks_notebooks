# Databricks notebook source
# 00_config
catalog_name = "billing"
schema_name  = "sales_data"
volume_name  = "new_volume"

API_BASE_URL = "https://api.thenewshop.in"
API_USERNAME = "nssomin"
API_PASSWORD = "nssomin"

from datetime import datetime, timedelta

# ── Widget allows TARGET_DATE to be overridden from Workflow params ───────────
dbutils.widgets.text("target_date", "")
TARGET_DATE = dbutils.widgets.get("target_date")

# If not passed (e.g. running manually), default to yesterday
if not TARGET_DATE:
    TARGET_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"USE SCHEMA {schema_name}")

print(f"✅ Config loaded : {catalog_name}.{schema_name}")
print(f"📅 Target date   : {TARGET_DATE}")