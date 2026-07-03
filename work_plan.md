# KẾ HOẠCH THỰC HIỆN CHI TIẾT — Sales Data Quality Lab

**Học viên:** Trần Mạnh Chánh Quân  
**Mã học viên:** 2A202600786  
**Ngày:** 2026-07-03  

> ⚠️ **Phạm vi làm việc:** Chỉ làm việc trong thư mục `starter_project/`.  
> Không sửa bất kỳ file nào trong `assignment/`.

---

## 1. Tổng quan dự án

Xây dựng Apache Airflow DAG `sales_data_quality_pipeline` thực hiện:
1. Đọc CSV đơn hàng từ `starter_project/data/`
2. Validate 3 quy tắc: `customer_id` không rỗng, `amount > 0`, `status` hợp lệ
3. Tự động sinh `output/validation_summary.json`
4. Gửi thông báo Discord Webhook
5. Raise exception + dừng pipeline khi validation fail

---

## 2. Phân tích chi tiết từng file trong `starter_project/`

### 2.1. Cấu trúc thư mục hiện tại

```
starter_project/
├── .env.example                          ← Mẫu biến môi trường
├── dags/
│   └── sales_data_quality_pipeline.py    ← ⚠️ FILE CẦN SỬA DUY NHẤT
├── data/
│   ├── orders_failed.csv                 ← 10 dòng, chứa lỗi có chủ đích
│   └── orders_passed.csv                 ← 10 dòng, dữ liệu sạch
├── expected/
│   ├── validation_summary_failed.json    ← JSON mẫu kết quả failed
│   └── validation_summary_passed.json    ← JSON mẫu kết quả passed
├── scripts/
│   └── run_local_check.py                ← Script CLI test local (đã hoàn chỉnh)
└── src/
    ├── __init__.py                       ← File trống (package marker)
    ├── config.py                         ← Cấu hình (đã hoàn chỉnh)
    └── validation.py                     ← Logic validation (đã hoàn chỉnh)
```

---

### 2.2. `src/config.py` — ĐÃ HOÀN CHỈNH ✅

**Vai trò:** File cấu hình trung tâm, định nghĩa tất cả hằng số và đường dẫn.

| Biến | Giá trị | Mục đích |
|------|--------|----------|
| `PROJECT_ROOT` | `Path(__file__).resolve().parents[1]` | Gốc `starter_project/` |
| `DATA_DIR` | `PROJECT_ROOT / "data"` | Thư mục chứa CSV |
| `OUTPUT_DIR` | `PROJECT_ROOT / "output"` | Thư mục output |
| `PASSED_DATASET` | `DATA_DIR / "orders_passed.csv"` | Đường dẫn dataset sạch |
| `FAILED_DATASET` | `DATA_DIR / "orders_failed.csv"` | Đường dẫn dataset lỗi |
| `SUMMARY_FILE` | `OUTPUT_DIR / "validation_summary.json"` | Đường dẫn file JSON output |
| `VALID_STATUSES` | `{"completed", "pending", "cancelled"}` | 3 trạng thái hợp lệ |
| `DISCORD_WEBHOOK_URL` | `os.getenv("DISCORD_WEBHOOK_URL", "")` | URL webhook từ biến môi trường |
| `AIRFLOW_INPUT_FILE` | `os.getenv("AIRFLOW_INPUT_FILE", str(PASSED_DATASET))` | File CSV đầu vào cho DAG |

**Kết luận:** KHÔNG cần sửa gì. File này đã export đủ mọi thứ cần thiết.

---

### 2.3. `src/validation.py` — ĐÃ HOÀN CHỈNH ✅

**Vai trò:** Chứa toàn bộ logic validation. File này import từ `src.config`.

| Hàm / Class | Input | Output | Mô tả |
|-------------|-------|--------|-------|
| `LabValidationError(RuntimeError)` | — | — | Exception class cho validation fail |
| `read_rows(csv_path)` | `str \| Path` | `list[dict[str,str]]` | Đọc CSV bằng `csv.DictReader`, trả về list các dict |
| `is_positive_number(raw_amount)` | `str` | `bool` | Kiểm tra chuỗi amount có phải số > 0 không |
| `build_summary(rows)` | `list[dict]` | `dict` | Duyệt từng dòng, đếm lỗi, trả về summary dict |
| `write_summary(summary, output_path)` | `dict, str\|Path` | `Path` | Ghi JSON ra file, tự tạo thư mục nếu chưa có |
| `send_discord_message(summary, webhook_url)` | `dict, str` | `None` | POST JSON payload tới Discord webhook |
| `run_lab_check(input_path, output_path, allow_failure, skip_discord)` | `Path, Path, bool, bool` | `dict` | **Hàm tổng (orchestrator)** — gọi tuần tự read_rows → build_summary → write_summary → send_discord_message → raise nếu fail |

**Quan sát quan trọng:**
- `run_lab_check()` đã làm **toàn bộ** các bước cần thiết.
- File này chỉ import `DISCORD_WEBHOOK_URL`, `OUTPUT_DIR`, `VALID_STATUSES` từ config.
- Các biến `AIRFLOW_INPUT_FILE` và `SUMMARY_FILE` (từ config.py) **không** được import trong file này — DAG cần tự import chúng.

**Kết luận:** KHÔNG cần sửa gì. Toàn bộ logic đã sẵn sàng.

---

### 2.4. `scripts/run_local_check.py` — ĐÃ HOÀN CHỈNH ✅

**Vai trò:** CLI wrapper cho phép test validation mà không cần Airflow.

```bash
# Cách dùng:
python3 scripts/run_local_check.py data/orders_passed.csv --skip-discord
python3 scripts/run_local_check.py data/orders_failed.csv --allow-failure --skip-discord
```

- Import `run_lab_check` và `LabValidationError` từ `src.validation`
- Parse CLI args: `input_file`, `--output`, `--allow-failure`, `--skip-discord`
- Gọi `run_lab_check()` và in kết quả

**Kết luận:** KHÔNG cần sửa. Dùng script này để test local.

---

### 2.5. `dags/sales_data_quality_pipeline.py` — ⚠️ CẦN SỬA

**Trạng thái hiện tại:**

Phần DAG definition và PythonOperator **đã đúng và hoàn chỉnh**:
```python
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # → starter_project/
sys.path.append(str(PROJECT_ROOT))                   # Thêm vào PYTHONPATH

if DAG is not None:
    with DAG(
        dag_id="sales_data_quality_pipeline",        # ✅ Đúng tên
        start_date=datetime(2024, 1, 1),
        schedule=None,                                # ✅ Manual trigger
        catchup=False,
        tags=["lab", "data-quality", "discord"],
    ) as dag:
        validate_orders = PythonOperator(
            task_id="validate_orders",                # ✅ Đúng task_id
            python_callable=validate_orders_task,
        )
```

**Phần DUY NHẤT cần hoàn thiện:** Hàm `validate_orders_task()` hiện là:
```python
def validate_orders_task() -> dict:
    raise NotImplementedError
```

**Yêu cầu của hàm:** Làm theo đúng TODO comment trong code:
1. Import config values (`AIRFLOW_INPUT_FILE`, `SUMMARY_FILE`, `DISCORD_WEBHOOK_URL`)
2. Read input CSV → gọi `read_rows()`
3. Validate rows → gọi `build_summary()`
4. Write JSON summary → gọi `write_summary()`
5. Send Discord alert → gọi `send_discord_message()`
6. Raise error nếu validation failed → `LabValidationError`

---

### 2.6. `data/orders_failed.csv` — Phân tích dữ liệu lỗi

| Dòng | order_id | customer_id | amount | status | Lỗi |
|------|----------|-------------|--------|--------|-----|
| 1 | 10001 | CUST-1001 | 149.99 | completed | ✅ |
| 2 | 10002 | CUST-1002 | 79.50 | pending | ✅ |
| 3 | 10003 | *(rỗng)* | 42.00 | completed | ❌ missing customer_id |
| 4 | 10004 | CUST-1004 | **-10.00** | completed | ❌ amount ≤ 0 |
| 5 | 10005 | CUST-1005 | 60.00 | **unknown** | ❌ invalid status |
| 6 | 10006 | CUST-1006 | **0.00** | pending | ❌ amount ≤ 0 |
| 7 | 10007 | *(khoảng trắng)* | 199.95 | cancelled | ❌ missing customer_id |
| 8 | 10008 | CUST-1008 | 350.40 | **processing** | ❌ invalid status |
| 9 | 10009 | CUST-1009 | 89.00 | completed | ✅ |
| 10 | 10010 | CUST-1010 | 15.75 | pending | ✅ |

→ Tổng: **2 missing_customer_ids + 2 invalid_amounts + 2 invalid_statuses = 6 lỗi**

---

### 2.7. `data/orders_passed.csv` — Dữ liệu sạch

10 dòng, tất cả đều có `customer_id` hợp lệ, `amount > 0`, `status` ∈ `{completed, pending, cancelled}`.

---

### 2.8. `expected/` — File JSON mẫu để đối chiếu

**`validation_summary_passed.json`:**
```json
{"row_count": 10, "missing_customer_ids": 0, "invalid_amounts": 0, "invalid_statuses": 0, "validation_status": "passed"}
```

**`validation_summary_failed.json`:**
```json
{"row_count": 10, "missing_customer_ids": 2, "invalid_amounts": 2, "invalid_statuses": 2, "validation_status": "failed"}
```

---

## 3. Hai phương án implement `validate_orders_task()` 

Cả hai đều đúng. Chọn một.

### Phương án A: Gọi từng hàm riêng lẻ (theo sát pseudocode) ⭐ Khuyến nghị

```python
def validate_orders_task() -> dict:
    from src.config import AIRFLOW_INPUT_FILE, SUMMARY_FILE, DISCORD_WEBHOOK_URL
    from src.validation import (
        read_rows, build_summary, write_summary,
        send_discord_message, LabValidationError,
    )

    rows = read_rows(AIRFLOW_INPUT_FILE)
    summary = build_summary(rows)
    write_summary(summary, SUMMARY_FILE)
    send_discord_message(summary, DISCORD_WEBHOOK_URL)

    if summary["validation_status"] == "failed":
        raise LabValidationError(
            f"Validation failed. Summary saved to {SUMMARY_FILE}"
        )

    return summary
```

**Ưu điểm:** Thể hiện rõ từng bước, khớp với TODO comment và pseudocode, dễ debug, dễ chấm điểm từng phần.

### Phương án B: Gọi `run_lab_check()` (ngắn gọn)

```python
def validate_orders_task() -> dict:
    from src.config import AIRFLOW_INPUT_FILE, SUMMARY_FILE, DISCORD_WEBHOOK_URL
    from src.validation import run_lab_check

    return run_lab_check(
        input_path=AIRFLOW_INPUT_FILE,
        output_path=SUMMARY_FILE,
        allow_failure=False,
        skip_discord=(not DISCORD_WEBHOOK_URL),
    )
```

**Ưu điểm:** Code ngắn, tận dụng hàm orchestrator có sẵn.

> ⚠️ Lưu ý: Dù chọn phương án nào, **phần còn lại của file DAG (imports, DAG definition, PythonOperator) KHÔNG được sửa** — chúng đã đúng.

---

## 4. Kế hoạch thực hiện từng bước

### BƯỚC 1 — Đọc hiểu code hiện có 

- [ ] Đọc kỹ `src/config.py`: nắm mọi biến được export
- [ ] Đọc kỹ `src/validation.py`: hiểu signature và hành vi từng hàm
- [ ] Xác nhận `dags/sales_data_quality_pipeline.py` đã có PROJECT_ROOT và sys.path setup đúng

---

### BƯỚC 2 — Implement `validate_orders_task()` (FILE DUY NHẤT CẦN SỬA)

**File:** `starter_project/dags/sales_data_quality_pipeline.py`  
**Vị trí:** Thay thế `raise NotImplementedError` bằng code thực thi (Phương án A hoặc B)

Checklist sau khi implement:
- [ ] Import đúng các hàm từ `src.validation`
- [ ] Import đúng các biến từ `src.config`
- [ ] Gọi `read_rows(AIRFLOW_INPUT_FILE)` → đọc CSV
- [ ] Gọi `build_summary(rows)` → validate + tạo summary dict
- [ ] Gọi `write_summary(summary, SUMMARY_FILE)` → ghi JSON
- [ ] Gọi `send_discord_message(summary, DISCORD_WEBHOOK_URL)` → gửi Discord
- [ ] `if validation_status == "failed": raise LabValidationError(...)` → dừng pipeline
- [ ] `return summary` → trả về dict

**Rubric liên quan:** #1 DAG Structure (2đ), #2 CSV Ingestion (2đ), #3 Validation Logic (3đ), #5 Discord (1đ), #6 Failure Handling (1đ)

---

### BƯỚC 3 — Test local với dataset passing

```bash
cd /mnt/c/Users/quand/OneDrive/Documents/VinUni-AI2k_2/Day27-Track2-lab/starter_project
python3 scripts/run_local_check.py data/orders_passed.csv --skip-discord
```

**Kết quả mong đợi:**
- In ra dict: `{'row_count': 10, 'missing_customer_ids': 0, 'invalid_amounts': 0, 'invalid_statuses': 0, 'validation_status': 'passed'}`
- Return code = 0 (thành công)
- File `output/validation_summary.json` được tạo

- [ ] So sánh output JSON với `expected/validation_summary_passed.json`

**Rubric:** #4 Output JSON (1đ)

---

### BƯỚC 4 — Test local với dataset failing

```bash
cd starter_project
python3 scripts/run_local_check.py data/orders_failed.csv --allow-failure --skip-discord
```

**Kết quả mong đợi:**
- In ra dòng: `Validation failed. Summary saved to .../output/validation_summary.json`
- Return code = 1 (lỗi)
- File `output/validation_summary.json` vẫn được tạo trước khi raise exception

- [ ] So sánh output JSON với `expected/validation_summary_failed.json`
- [ ] Xác nhận các counters: `missing_customer_ids=2`, `invalid_amounts=2`, `invalid_statuses=2`

**Rubric:** #3 Validation Logic (3đ), #6 Failure Handling (1đ)

---

### BƯỚC 5 — Test Discord notification

**Nếu có webhook thật:**
```bash
export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
python3 scripts/run_local_check.py data/orders_passed.csv
python3 scripts/run_local_check.py data/orders_failed.csv --allow-failure
```

**Nếu không có webhook thật:**
- [ ] Chạy không có flag `--skip-discord` với `DISCORD_WEBHOOK_URL=""` → code sẽ `return` sớm (không gửi gì) → không lỗi
- [ ] Test với mock webhook URL → xác nhận HTTP POST được gửi (sẽ fail do URL không tồn tại, nhưng chứng minh code hoạt động)

**Rubric:** #5 Discord Alert (1đ)

---

### BƯỚC 6 — Đối chiếu output với expected

```bash
cd starter_project

# Sau khi chạy với orders_passed.csv:
diff output/validation_summary.json expected/validation_summary_passed.json

# Sau khi chạy với orders_failed.csv:
diff output/validation_summary.json expected/validation_summary_failed.json
```

- [ ] Không có khác biệt (diff trả về rỗng) → output khớp expected

**Rubric:** #4 Output JSON (1đ)

---

### BƯỚC 7 — Xác nhận DAG chạy được trong Airflow (nếu có môi trường)

```bash
# Kiểm tra DAG được phát hiện
airflow dags list | grep sales_data_quality_pipeline

# Trigger DAG với dataset passing
airflow dags trigger -c '{"env": "AIRFLOW_INPUT_FILE=./data/orders_passed.csv"}' sales_data_quality_pipeline

# Trigger DAG với dataset failing
airflow dags trigger -c '{"env": "AIRFLOW_INPUT_FILE=./data/orders_failed.csv"}' sales_data_quality_pipeline
```

- [ ] DAG xuất hiện trong `airflow dags list`
- [ ] DAG có thể trigger thủ công
- [ ] Task `validate_orders` thành công với `orders_passed.csv`
- [ ] Task `validate_orders` fail với `orders_failed.csv`
- [ ] `output/validation_summary.json` được tạo trong cả 2 trường hợp

**Rubric:** #1 DAG Structure And Execution (2đ)

---

### BƯỚC 8 — Cập nhật báo cáo `report.md`

Sau khi hoàn thành tất cả bước trên:
- [ ] Điền kết quả thực tế vào bảng trong `report.md`
- [ ] Tự đánh giá theo rubric (điền điểm vào bảng)
- [ ] Viết kết luận

---

## 5. Bảng ánh xạ: Việc cần làm → File → Rubric

| # | Việc cần làm | File duy nhất | Rubric |
|---|-------------|---------------|--------|
| 1 | Implement `validate_orders_task()` | `dags/sales_data_quality_pipeline.py` | #1, #2, #3, #5, #6 |
| 2 | Test với `orders_passed.csv` | *(chạy script)* | #3, #4 |
| 3 | Test với `orders_failed.csv` | *(chạy script)* | #3, #6 |
| 4 | So sánh output JSON | *(diff)* | #4 |
| 5 | Test Discord notification | *(chạy script)* | #5 |
| 6 | Test trong Airflow | *(nếu có)* | #1 |
| 7 | Cập nhật `report.md` | `report.md` | — |

---

## 6. Tự đánh giá theo Rubric

| # | Tiêu chí | Điểm tối đa | Tự đánh giá | Ghi chú |
|---|----------|-------------|-------------|---------|
| 1 | DAG Structure And Execution | 2 | __/2 | DAG đã có sẵn, chỉ cần implement hàm |
| 2 | CSV Ingestion And Dataset Handling | 2 | __/2 | Dùng `csv.DictReader` từ `validation.py` |
| 3 | Validation Logic | 3 | __/3 | 3 quy tắc đã implement trong `build_summary()` |
| 4 | Output JSON | 1 | __/1 | `write_summary()` tự tạo thư mục và ghi JSON |
| 5 | Discord Alert | 1 | __/1 | `send_discord_message()` POST webhook |
| 6 | Failure Handling | 1 | __/1 | Raise `LabValidationError` khi fail |
| **Tổng** | | **10** | **__/10** | |

---

## 7. Ghi chú quan trọng

1. **Chỉ sửa 1 file duy nhất:** `starter_project/dags/sales_data_quality_pipeline.py` — thay `raise NotImplementedError` bằng code thực thi.
2. **Không đụng đến `assignment/`** — đó là tài liệu tham khảo, không phải code cần sửa.
3. **Không cần sửa `src/config.py` hay `src/validation.py`** — chúng đã hoàn chỉnh.
4. `write_summary()` tự động tạo thư mục `output/` nếu chưa tồn tại → không cần tạo thủ công.
5. Khi không có `DISCORD_WEBHOOK_URL`, `send_discord_message()` sẽ `return` sớm, không gây lỗi.
6. File CSV phải giữ nguyên để kết quả validation khớp với expected JSON.
7. Có thể test toàn bộ logic validation qua `scripts/run_local_check.py` mà không cần Airflow.
