CREATE TABLE IF NOT EXISTS excel_combined (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  source_sheet VARCHAR(128),
  load_date DATE,
  col1 TEXT NULL,
  col2 TEXT NULL,
  col3 TEXT NULL,
  col4 TEXT NULL,
  col5 TEXT NULL,
  col6 TEXT NULL,
  col7 TEXT NULL,
  col8 TEXT NULL,
  col9 TEXT NULL,
  col10 TEXT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
