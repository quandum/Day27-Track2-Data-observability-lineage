# BÁO CÁO KẾT QUẢ - Sales Data Quality Lab

**Học viên:** Trần Mạnh Chánh Quân  
**Mã học viên:** 2A202600786  
**Ngày thực hiện:** 2026-07-03  
**Lab:** sales_data_quality_pipeline  

---

## 1. Mô tả dự án

Xây dựng một Apache Airflow DAG để tự động kiểm tra chất lượng dữ liệu đơn hàng bán hàng (sales orders). Pipeline thực hiện:

1. Đọc dữ liệu từ file CSV (`orders_passed.csv` hoặc `orders_failed.csv`)
2. Kiểm tra 3 quy tắc chất lượng dữ liệu:
   - `customer_id` không được rỗng
   - `amount` phải là số và lớn hơn 0
   - `status` phải là một trong: `completed`, `pending`, `cancelled`
3. Tự động tạo file `validation_summary.json` trong thư mục `output/`
4. Gửi thông báo kết quả qua Discord Webhook
5. Dừng pipeline và raise exception khi phát hiện dữ liệu không hợp lệ

---

## 2. Cấu trúc dự án

```
starter_project/
├── dags/
│   └── sales_data_quality_pipeline.py   ← DAG Airflow (cần hoàn thiện)
├── data/
│   ├── orders_failed.csv                ← Dataset có lỗi (10 dòng)
│   └── orders_passed.csv                ← Dataset sạch (10 dòng)
├── expected/
│   ├── validation_summary_failed.json   ← Kết quả mong đợi (failed)
│   └── validation_summary_passed.json   ← Kết quả mong đợi (passed)
├── output/                              ← Thư mục output (tự động tạo)
│   └── validation_summary.json          ← File JSON sinh ra tự động
├── scripts/
│   └── run_local_check.py               ← Script kiểm tra local
└── src/
    ├── __init__.py
    ├── config.py                        ← Cấu hình (đã hoàn chỉnh)
    └── validation.py                    ← Logic validation (đã hoàn chỉnh)
```

---

## 3. Các bước đã thực hiện

### Bước 1: Phân tích yêu cầu và cấu trúc
- [ ] Đọc và phân tích `README.md`, `lab_brief.py`, `pseudocode.py`, `implementation_steps.py`
- [ ] Kiểm tra cấu trúc thư mục `starter_project/`
- [ ] Xác nhận code trong `src/config.py` và `src/validation.py` đã hoàn chỉnh

### Bước 2: Hoàn thiện DAG Airflow
- [ ] Implement hàm `validate_orders_task()` trong `dags/sales_data_quality_pipeline.py`
- [ ] Kết nối với các hàm từ `src/validation.py`

### Bước 3: Chạy kiểm tra với dataset passing
- [ ] Chạy `run_local_check.py` với `orders_passed.csv`
- [ ] Xác nhận kết quả validation_status = "passed"
- [ ] So sánh output JSON với `expected/validation_summary_passed.json`

### Bước 4: Chạy kiểm tra với dataset failing
- [ ] Chạy `run_local_check.py` với `orders_failed.csv --allow-failure`
- [ ] Xác nhận kết quả validation_status = "failed"
- [ ] So sánh output JSON với `expected/validation_summary_failed.json`

### Bước 5: Kiểm tra Discord notification
- [ ] Kiểm tra logic gửi Discord message (với mock webhook hoặc webhook thật)

---

## 4. Kết quả

### 4.1. Dataset passing (`orders_passed.csv`)

| Chỉ số | Kết quả |
|--------|---------|
| row_count | __ |
| missing_customer_ids | __ |
| invalid_amounts | __ |
| invalid_statuses | __ |
| validation_status | __ |
| Discord notification | __ |

### 4.2. Dataset failing (`orders_failed.csv`)

| Chỉ số | Kết quả |
|--------|---------|
| row_count | __ |
| missing_customer_ids | __ |
| invalid_amounts | __ |
| invalid_statuses | __ |
| validation_status | __ |
| Discord notification | __ |
| Pipeline dừng đúng | __ |

---

## 5. Tự đánh giá theo Rubric

| # | Tiêu chí | Điểm tối đa | Tự đánh giá | Ghi chú |
|---|----------|-------------|-------------|---------|
| 1 | DAG Structure And Execution | 2 | __/2 | |
| 2 | CSV Ingestion And Dataset Handling | 2 | __/2 | |
| 3 | Validation Logic | 3 | __/3 | |
| 4 | Output JSON | 1 | __/1 | |
| 5 | Discord Alert | 1 | __/1 | |
| 6 | Failure Handling | 1 | __/1 | |
| **Tổng** | | **10** | **__/10** | |

---

## 6. Kết luận

_(Điền kết luận sau khi hoàn thành các bước thực hiện)_

---

**Người thực hiện:** Trần Mạnh Chánh Quân  
**Mã học viên:** 2A202600786
