from __future__ import annotations
import os
import logging
import pandas as pd
import pendulum
from sqlalchemy import text

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator

# Đường dẫn trong CONTAINER (đã được mount ở docker-compose.yml)
EXCEL_PATH = os.environ.get("EXCEL_PATH", "/opt/airflow/dags/data/input.xlsx")
DDL_PATH   = os.environ.get("DDL_PATH",   "/opt/airflow/sql/create_table.sql")

MYSQL_CONN_ID = os.environ.get("MYSQL_CONN_ID", "mysql_target")
TARGET_TABLE  = os.environ.get("TARGET_TABLE", "excel_combined")

LOCAL_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

@dag(
    dag_id="excel_to_mysql_daily_7am",
    description="Read Excel (4 sheets) -> unify -> load to 1 MySQL table daily 07:00 Asia/Ho_Chi_Minh",
    schedule="0 7 * * *",  # 07:00 hằng ngày
    start_date=pendulum.datetime(2025, 8, 1, 7, 0, tz=LOCAL_TZ),
    catchup=False,
    tags=["excel", "mysql", "etl"],
)
def excel_to_mysql_pipeline():

    # 1) Đảm bảo bảng tồn tại
    ensure_table = MySqlOperator(
        task_id="ensure_table",
        mysql_conn_id=MYSQL_CONN_ID,
        sql=open(DDL_PATH, "r", encoding="utf-8").read()
    )

    # 2) Extract + Transform
    @task()
    def extract_transform(**context) -> str:
        ds = context["ds"]  # yyyy-mm-dd
        if not os.path.exists(EXCEL_PATH):
            raise FileNotFoundError(f"Excel not found at {EXCEL_PATH}")

        # Đọc toàn bộ sheet, giữ an toàn kiểu bằng dtype=str
        xls = pd.read_excel(EXCEL_PATH, sheet_name=None, dtype=str)
        frames = []
        for sheet_name, df in xls.items():
            df = df.copy()
            df["source_sheet"] = sheet_name
            frames.append(df)

        combined = pd.concat(frames, ignore_index=True, sort=False)
        combined["load_date"] = ds

        # Chuẩn hóa tên cột
        combined.columns = [str(c).strip().lower().replace(" ", "_") for c in combined.columns]

        # Lấy các cột dữ liệu ngoài meta
        data_cols = [c for c in combined.columns if c not in ("source_sheet", "load_date")]
        data_cols = sorted(data_cols)

        # Map vào col1..col10 (schema mẫu)
        MAX_COLS = 10
        if len(data_cols) > MAX_COLS:
            logging.warning("Có %d cột dữ liệu > %d, chỉ lấy 10 cột đầu.", len(data_cols), MAX_COLS)
            data_cols = data_cols[:MAX_COLS]

        out_df = pd.DataFrame()
        out_df["source_sheet"] = combined["source_sheet"]
        out_df["load_date"] = combined["load_date"]
        for i in range(1, MAX_COLS + 1):
            out_df[f"col{i}"] = combined[data_cols[i-1]] if i-1 < len(data_cols) else None

        tmp_csv = f"/tmp/combined_{ds}.csv"
        out_df.to_csv(tmp_csv, index=False)
        logging.info("Prepared rows=%s, mapped_cols=%s", len(out_df), data_cols)
        return tmp_csv

    # 3) Load vào MySQL (idempotent theo load_date)
    @task()
    def load_to_mysql(csv_path: str, **context) -> int:
        ds = context["ds"]
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        df = pd.read_csv(csv_path, dtype=str)
        rows = len(df)
        with engine.begin() as conn:
            conn.execute(text(f"DELETE FROM {TARGET_TABLE} WHERE load_date = :d"), {"d": ds})
            df.to_sql(TARGET_TABLE, con=conn, if_exists="append", index=False)
        return rows

    # 4) Data Quality check
    @task()
    def dq_check(loaded_rows: int, **context):
        ds = context["ds"]
        if loaded_rows <= 0:
            raise ValueError(f"DQ fail: 0 row loaded for load_date={ds}")

    csv_path = extract_transform()
    loaded = load_to_mysql(csv_path)
    dq = dq_check(loaded)

    ensure_table >> csv_path >> loaded >> dq

excel_to_mysql_pipeline()
