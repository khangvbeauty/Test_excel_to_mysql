# ETL Excel → MySQL với Airflow
## Tổng quan

Bài test này minh họa một ETL pipeline sử dụng Apache Airflow + Python + MySQL.
Dữ liệu đầu vào: File Excel (`input.xlsx`) gồm 4 sheet (Q1, Q2, Q3, Q4). 
Xử lý: Đọc tất cả sheet, chuẩn hóa và gộp lại thành một dataset.
Tải dữ liệu: Ghi vào bảng MySQL `excel_combined`.
Tự động hóa: DAG trong Airflow chạy hằng ngày lúc 07:00 sáng.

## Khởi động dịch vụ với Docker Compose

```bash
docker compose up airflow-init
docker compose up -d
```

## Truy cập dịch vụ

* Airflow UI: [http://localhost:8081](http://localhost:8081)
* MySQL (CLI hoặc Workbench):

  * Host: `127.0.0.1`
  * Port: `3307`
  * User: `airflow`
  * Password: `airflow`
  * Database: `de_test`

 ## Chạy pipeline ETL

1. Vào Airflow UI → DAGs.
2. Bật DAG `excel_to_mysql_daily_7am`.
3. Có thể Trigger DAG thủ công hoặc chờ 07:00 sáng.
4. Vào Graph View kiểm tra → tất cả task màu xanh ✅.


## Kiểm tra kết quả trong MySQL

```sql
USE de_test;

-- Tổng số dòng load hôm nay
SELECT COUNT(*) FROM excel_combined WHERE load_date = CURDATE();

-- Số bản ghi theo từng sheet
SELECT source_sheet, COUNT(*) 
FROM excel_combined
WHERE load_date = CURDATE()
GROUP BY source_sheet;

-- Xem 10 dòng đầu tiên
SELECT * FROM excel_combined LIMIT 10;


Kết quả mong đợi: 800 dòng (200 dòng mỗi sheet Q1–Q4)

```
Ví dụ log trong Airflow

```sql
INFO - Prepared rows=800, mapped_cols=[
  'customer_id','order_date','order_id',
  'product_id','quantity','region','status','unit_price'
]
```

 Luồng xử lý
````
Excel (4 sheet) → Airflow DAG → Python (pandas) → MySQL (excel\_combined)
````
