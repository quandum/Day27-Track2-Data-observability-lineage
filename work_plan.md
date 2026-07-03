# KẾ HOẠCH THỰC HIỆN - Sales Data Quality Lab

**Học viên:** Trần Mạnh Chánh Quân  
**Mã học viên:** 2A202600786  
**Ngày:** 2026-07-03  

---

## Tổng quan dự án

Xây dựng một Apache Airflow DAG (Directed Acyclic Graph) thực hiện:
1. Đọc dữ liệu đơn hàng bán hàng từ file CSV
2. Kiểm tra chất lượng dữ liệu (validate) theo 3 quy tắc
3. Tự động tạo file `validation_summary.json`
4. Gửi thông báo qua Discord Webhook
5. Dừng pipeline khi phát hiện dữ liệu lỗi

---

## Phân tích hiện trạng (Current State Analysis)

### ✅ Đã hoàn thành (Already Done)

| Thành phần | File | Trạng thái |
|------------|------|------------|
| Cấu hình | `starter_project/src/config.py` | ✅ Hoàn chỉnh |
| Logic validation | `starter_project/src/validation.py` | ✅ Hoàn chỉnh - đầy đủ các hàm `read_rows`, `is_positive_number`, `build_summary`, `write_summary`, `send_discord_message`, `run_lab_check` |
| Script kiểm tra local | `starter_project/scripts/run_local_check.py` | ✅ Hoàn chỉnh |
| Dữ liệu lỗi | `starter_project/data/orders_failed.csv` | ✅ Có sẵn (10 dòng, chứa lỗi) |
| Dữ liệu sạch | `starter_project/data/orders_passed.csv` | ✅ Có sẵn (10 dòng, không lỗi) |
| JSON mẫu - failed | `starter_project/expected/validation_summary_failed.json` | ✅ Có sẵn |
| JSON mẫu - passed | `starter_project/expected/validation_summary_passed.json` | ✅ Có sẵn |
| Package init | `starter_project/src/__init__.py` | ✅ File trống |

### ❌ Cần hoàn thành (To Do)

| Thành phần | File | Mô tả công việc |
|------------|------|-----------------|
| DAG Airflow | `starter_project/dags/sales_data_quality_pipeline.py` | Hàm `validate_orders_task()` đang là stub với `raise NotImplementedError` |
| Thư mục output | `starter_project/output/` | Chưa tồn tại, sẽ được tạo tự động khi chạy |
| File báo cáo | `report.md` | Chưa tồn tại |

---

## Kế hoạch từng bước (Step-by-Step Plan)

### BƯỚC 1: Phân tích và xác nhận cấu trúc dự án

- [ ] Xem lại toàn bộ cấu trúc thư mục `starter_project/`
- [ ] Xác nhận các file CSV tồn tại trong `starter_project/data/`
- [ ] Đọc hiểu code hiện có trong `src/config.py` và `src/validation.py`

**File liên quan:**
- `starter_project/dags/sales_data_quality_pipeline.py`
- `starter_project/src/config.py`
- `starter_project/src/validation.py`

**Tiêu chí đánh giá (Rubric):** 1 - DAG Structure (2 điểm)

---

### BƯỚC 2: Hoàn thiện hàm `validate_orders_task()` trong DAG

Cần implement các bước sau trong hàm `validate_orders_task()`:

```python
def validate_orders_task() -> dict:
    # 1. Import config values (AIRFLOW_INPUT_FILE, DISCORD_WEBHOOK_URL)
    # 2. Gọi read_rows() để đọc CSV
    # 3. Gọi build_summary() để validate dữ liệu
    # 4. Gọi write_summary() để lưu JSON
    # 5. Gọi send_discord_message() để gửi thông báo
    # 6. Nếu validation_status == "failed" thì raise exception
    # 7. Return summary dict
```

**File cần sửa:** `starter_project/dags/sales_data_quality_pipeline.py`

**Tiêu chí đánh giá:**
- 1 - DAG Structure And Execution (2 điểm)
- 2 - CSV Ingestion And Dataset Handling (2 điểm)
- 3 - Validation Logic (3 điểm)
- 5 - Discord Alert (1 điểm)
- 6 - Failure Handling (1 điểm)

---

### BƯỚC 3: Kiểm tra logic validation (3 quy tắc)

Xác nhận code validation kiểm tra đúng 3 quy tắc:

| # | Quy tắc | Logic | File |
|---|---------|-------|------|
| 1 | `customer_id` không được rỗng | `if not customer_id` → `missing_customer_ids += 1` | `validation.py:build_summary()` |
| 2 | `amount` phải là số và > 0 | `is_positive_number()` → `invalid_amounts += 1` | `validation.py:build_summary()` |
| 3 | `status` phải là `completed`, `pending`, hoặc `cancelled` | `status not in VALID_STATUSES` → `invalid_statuses += 1` | `validation.py:build_summary()` |

**File liên quan:** `starter_project/src/validation.py`, `starter_project/src/config.py`

**Tiêu chí đánh giá:** 3 - Validation Logic (3 điểm)

---

### BƯỚC 4: Chạy kiểm tra với dataset passing (`orders_passed.csv`)

```bash
cd starter_project
python3 scripts/run_local_check.py data/orders_passed.csv --skip-discord
```

**Kết quả mong đợi:**
- Validation passed
- `output/validation_summary.json` được tạo với nội dung:
  ```json
  {
    "row_count": 10,
    "missing_customer_ids": 0,
    "invalid_amounts": 0,
    "invalid_statuses": 0,
    "validation_status": "passed"
  }
  ```

**Tiêu chí đánh giá:** 4 - Output JSON (1 điểm)

---

### BƯỚC 5: Chạy kiểm tra với dataset failing (`orders_failed.csv`)

```bash
cd starter_project
python3 scripts/run_local_check.py data/orders_failed.csv --allow-failure --skip-discord
```

**Kết quả mong đợi:**
- Validation failed
- `output/validation_summary.json` được tạo với nội dung:
  ```json
  {
    "row_count": 10,
    "missing_customer_ids": 2,
    "invalid_amounts": 2,
    "invalid_statuses": 2,
    "validation_status": "failed"
  }
  ```
- Pipeline dừng lại (raise exception)

**Phân tích dữ liệu lỗi trong `orders_failed.csv`:**
- Dòng 3 (10003): `customer_id` trống → missing_customer_ids
- Dòng 4 (10004): `amount = -10.00` → invalid_amounts
- Dòng 5 (10005): `status = unknown` → invalid_statuses
- Dòng 6 (10006): `amount = 0.00` → invalid_amounts
- Dòng 7 (10007): `customer_id = " "` (chỉ có khoảng trắng) → missing_customer_ids
- Dòng 8 (10008): `status = processing` → invalid_statuses

**Tiêu chí đánh giá:** 3 - Validation Logic, 6 - Failure Handling

---

### BƯỚC 6: So sánh output với file expected

So sánh file JSON sinh ra với file mẫu trong `starter_project/expected/`:

```bash
diff starter_project/output/validation_summary.json starter_project/expected/validation_summary_passed.json
diff starter_project/output/validation_summary.json starter_project/expected/validation_summary_failed.json
```

**Tiêu chí đánh giá:** 4 - Output JSON (1 điểm)

---

### BƯỚC 7: Kiểm tra Discord notification (nếu có webhook)

Nếu có Discord Webhook URL hợp lệ:
```bash
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
python3 scripts/run_local_check.py data/orders_passed.csv
python3 scripts/run_local_check.py data/orders_failed.csv --allow-failure
```

Nếu không có webhook thật, sử dụng mock webhook để kiểm tra logic gửi tin nhắn.

**Tiêu chí đánh giá:** 5 - Discord Alert (1 điểm)

---

### BƯỚC 8: Kiểm tra DAG trong Airflow (nếu có môi trường)

Nếu có Apache Airflow đang chạy:
1. Copy `starter_project/` vào thư mục Airflow
2. Xác nhận DAG được Airflow phát hiện (`airflow dags list`)
3. Trigger DAG thủ công với từng dataset
4. Kiểm tra logs và output

**Tiêu chí đánh giá:** 1 - DAG Structure And Execution (2 điểm)

---

### BƯỚC 9: Hoàn thiện báo cáo (`report.md`)

File `report.md` sẽ bao gồm:
- Thông tin học viên
- Mô tả dự án
- Các bước đã thực hiện
- Kết quả đạt được
- Bảng tự đánh giá theo rubric

---

## Bảng tự đánh giá theo Rubric

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

## Danh sách File cần chỉnh sửa / tạo mới

| File | Hành động | Mức độ ưu tiên |
|------|-----------|----------------|
| `starter_project/dags/sales_data_quality_pipeline.py` | SỬA - Hoàn thiện `validate_orders_task()` | 🔴 CAO |
| `report.md` | TẠO MỚI - Báo cáo kết quả | 🔴 CAO |
| `starter_project/output/` | TỰ ĐỘNG - Thư mục được tạo khi chạy | 🟡 TRUNG BÌNH |

---

## Ghi chú

1. **Tên học viên:** Trần Mạnh Chánh Quân
2. **Mã học viên:** 2A202600786
3. Code trong `src/validation.py` và `src/config.py` đã hoàn chỉnh, KHÔNG cần sửa.
4. Công việc chính là hoàn thiện hàm `validate_orders_task()` trong file DAG.
5. Sử dụng `--skip-discord` khi test local nếu không có webhook URL.
6. File CSV cần giữ nguyên để đảm bảo kết quả validation khớp với expected JSON.
