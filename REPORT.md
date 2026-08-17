# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Hùng Mạnh  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details open>
<summary>Output ba lần chạy (make verify)</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 27.6s
  run 2/3 … 23.2s
  run 3/3 … 28.0s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 137,186 (36.4×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (100% test pass, hoàn thành xuất sắc cả 2 bài mở rộng A & B).

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Khi chạy lại pipeline hoặc khi vận hành bấm *Clear Task* trên Airflow khiến DAG chạy bù, số hàng trong `gold_training_set` tăng gấp đôi/gấp ba sau mỗi lần chạy (từ 12.480 lên 24.960, 37.440...). Bảng đích xuất hiện nhiều bản ghi trùng lặp cho cùng một `ticket_id`. |
| **Nguyên nhân** | Model `gold_training_set` cấu hình `materialized='incremental'` nhưng không khai báo `unique_key` và `incremental_strategy`, khiến dbt mặc định sinh câu lệnh `INSERT INTO` (chiến lược `append`). Khi chạy lại cùng một partition ngày hoặc backfill, dbt chèn thêm các dòng mới thay vì ghi đè. Ngoài ra, nguồn CDC chứa các bản ghi cập nhật (`op = 'u'`) diễn ra ở các ngày khác nhau, khiến một ticket lọt qua điều kiện lọc theo ngày nhiều lần; do thiếu cơ chế `merge` theo khoá thực thể `ticket_id`, các bản ghi cập nhật này bị nhân bản thành các dòng riêng biệt. |
| **Cách khắc phục** | 1. `dbt/models/gold/gold_training_set.sql`: Thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào khối `config()`.<br>2. `dags/ai_training_pipeline.py`: Đặt `catchup=False` (ngăn Airflow tự động backfill dồn dập toàn bộ lịch sử khi clear task) và `max_active_runs=1` (ngăn các DAG run chạy đồng thời tranh chấp ghi vào cùng một bảng incremental). |
| **Bằng chứng** | trước: 12.480 hàng (lượt 1) → 24.960 hàng (lượt 2) → 37.440 hàng (lượt 3) · sau: 12.480 hàng cố định qua mọi lượt chạy · checksum 3 lượt: `8dd7c98653` / `8dd7c98653` / `8dd7c98653` (ổn định tuyệt đối, 1 hàng / 1 ticket không lặp). |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` chỉ đạt 8.645 hàng (thiếu 455 hàng so với kỳ vọng 9.100 hàng = 14 ngày × 650 customer). Các hàng bị thiếu tập trung ở các ngày cũ trong quá khứ (từ 2026-08-03 đến 2026-08-13). |
| **P99 độ trễ đo được** | **2,68 ngày** *(bắt buộc)* |
| **Lookback đã chọn** | **3 ngày** (`interval 3 day`) — vì P99 độ trễ thực tế đo được từ `bronze_events` là ~2,68 ngày (~64,3 giờ), đảm bảo bao phủ 99% các sự kiện đến muộn (late-arriving events) mà không tốn chi phí tính toán quét lại toàn bộ lịch sử 14 ngày ở mỗi lần chạy. |
| **Nguyên nhân** | Model `gold_feature_daily` sử dụng điều kiện lọc incremental theo thời gian sự kiện: `where event_date > (select max(event_date) from {{ this }})`. Khi dữ liệu sự kiện phát sinh trong quá khứ bị chuyển về kho muộn (do thiết bị client offline, nghẽn mạng, delay queue), thời điểm `_ingested_at` rơi vào các ngày sau (ví dụ event ngày 08-08 nhưng 08-11 mới tới kho). Tại thời điểm chạy ngày 08-11, `max(event_date)` trong bảng đích đã là 08-10, khiến toàn bộ các event muộn có `event_date <= max(event_date)` bị điều kiện `>` loại bỏ hoàn toàn khỏi bước tổng hợp. |
| **Cách khắc phục** | 1. `dbt/models/gold/gold_feature_daily.sql`: Nới điều kiện lọc incremental bằng lookback window lùi 3 ngày: `where event_date >= (select max(event_date) from {{ this }}) - interval 3 day`.<br>2. Cấu hình `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` trong `config()` để khi tính lại dữ liệu 3 ngày gần nhất, các cặp `(event_date, customer_id)` được cập nhật/ghi đè chuẩn xác thay vì bị cộng dồn nhân bản. |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455 hàng) · sau: 9.100 hàng (đủ 100% 14 ngày × 650 khách hàng) · checksum 3 lượt: `3db448685c` / `3db448685c` / `3db448685c`. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` đo khoảng trễ cực đoan của một vài outlier cá biệt (có thể trễ hàng tuần hoặc hàng tháng do thiết bị bị treo rất lâu). Nếu chọn lookback theo `max`, mọi run hàng ngày đều phải quét và tổng hợp lại một khoảng thời gian quá lớn trong quá khứ -> chi phí compute (I/O đọc đĩa, CPU, runtime của pipeline) tăng vọt và lặp đi lặp lại ở mọi lượt chạy sau này.
> 
> Trong khi đó, chọn `P99` là điểm cân bằng tối ưu giữa tính đầy đủ của dữ liệu (99% dữ liệu kịp cập nhật trong SLA của pipeline hàng ngày) và chi phí vận hành (lookback window chỉ cần 3 ngày, chi phí tính thêm không đáng kể). Đối với 1% ngoại lai trễ hơn P99, giải pháp chuẩn trong kiến trúc Data Pipeline là sử dụng một batch reconciliation job chạy định kỳ (hàng tuần/hàng tháng) để đối soát và bù dữ liệu, thay vì gánh nặng vào pipeline streaming/daily hàng ngày.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Kể từ ngày 2026-08-10, cột `priority` trong bảng `silver_tickets` xuất hiện hàng nghìn giá trị `NULL` (hơn 4.000 hàng), đồng thời lẫn các giá trị sai contract như `0`, `5`, `-1`. Khi bật `contract: enforced: true` trên `silver_tickets`, lệnh `dbt run` bị fail do kiểu dữ liệu không khớp. |
| **Nguyên nhân** | Phía upstream (hệ thống nguồn backend) thực hiện thay đổi schema (schema evolution) từ ngày 2026-08-10: chuyển kiểu `priority` từ số nguyên (`1..4`) sang nhãn chuỗi (`urgent`, `high`, `medium`, `low`). Biểu thức `try_cast(priority_raw as integer)` cũ trong macro không ép kiểu được các chuỗi này thành số nên trả về `NULL`. Đồng thời, upstream cũng lẫn các lỗi dữ liệu thực sự (bản ghi rác chứa `0`, `5`, `-1`, `unknown`, `P1`, `''`, `null`). |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | 1. **Nhóm số hợp lệ (`'1'`, `'2'`, `'3'`, `'4'`):** Đúng contract ban đầu → Cast sang kiểu `integer`.<br>2. **Nhóm nhãn chuỗi (`'urgent'`, `'high'`, `'medium'`, `'low'`):** Schema evolution (ngữ nghĩa không đổi) → Map chuẩn hoá sang số: `urgent → 1`, `high → 2`, `medium → 3`, `low → 4`.<br>3. **Nhóm giá trị sai/lỗi (`'P1'`, `'unknown'`, `'0'`, `'5'`, `'-1'`, `''`, `null`...):** Dữ liệu lỗi thật → Macro trả về `NULL`, đưa vào bảng cách ly `quarantine_tickets`. |
| **Cách khắc phục** | 1. `dbt/macros/normalize_priority.sql`: Viết khối `CASE` xử lý map nhóm 1 & 2 thành 1..4, nhóm 3 trả về `NULL`.<br>2. `dbt/models/silver/silver_tickets.sql`: Lọc loại bỏ bản ghi có `priority_clean IS NULL` TRƯỚC khi xếp hạng `row_number()`, đảm bảo chỉ loại bỏ *bản ghi CDC lỗi* mà không loại bỏ *ticket* (ticket vẫn giữ trạng thái hợp lệ từ lần cập nhật trước).<br>3. `dbt/models/silver/quarantine_tickets.sql`: Thay `where false` bằng `where {{ normalize_priority('priority_raw') }} is null` để thu gom đủ 312 bản ghi CDC lỗi.<br>4. `dbt/models/silver/schema.yml`: Đặt `enforced: true` để kích hoạt Data Contract, bổ sung test `not_null` và `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (đúng 100% số bản ghi CDC lỗi kỳ vọng, checksum `ebb89036fb`) · `silver_tickets` đủ 12.480 ticket · `dbt test` 11/11 pass (bổ sung 2 test mới so với bản gốc 9 test). |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **1. Nên lưu trữ thô ở Bronze và kiểm tra/chặn ở Silver:** 
> Tầng Bronze là *Single Source of Truth* lưu trữ raw immutable audit log của toàn bộ sự kiện từ upstream. Nếu drop/từ chối ngay từ Bronze khi vừa ingest, ta sẽ làm mất vĩnh viễn dữ liệu gốc, không thể trace vết sự cố, không thể replay pipeline và không thể khôi phục lại khi upstream sửa lỗi. Tầng Silver là nơi thực thi Data Contract và Business Logic để làm sạch dữ liệu phục vụ các tầng Gold/ML; các bản ghi lỗi được cách ly sang `quarantine_tickets` giúp đội Data Ops giám sát chất lượng dữ liệu mà không làm hỏng dữ liệu tầng hạ nguồn.
>
> **2. Vì sao không để pipeline dừng:**
> Trong 14.300 bản ghi CDC, chỉ có 312 bản ghi bị lỗi (chiếm ~2%). Nếu để test fail và dừng toàn bộ pipeline (Hard Fail), 98% dữ liệu hợp lệ (hơn 12.000 ticket) của các bộ phận kinh doanh và các mô hình AI phục vụ khách hàng sẽ bị tắc nghẽn, vi phạm SLA vận hành. Mô hình *Quarantine / Dead Letter Queue* cho phép pipeline tiếp tục phục vụ 98% dữ liệu sạch liên tục, đồng thời cô lập 2% dữ liệu lỗi để xử lý độc lập.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | **Cả 2 bài: Bài A và Bài B** (+10 điểm thưởng) |
| **Nguyên nhân** | **Bài A (Dashboard chậm):** Dữ liệu bị phân mảnh thành 5.000 file Parquet nhỏ không partition, tên file không chứa thông tin ngày/khách hàng. Điều kiện lọc `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` bọc cột trong function khiến query engine không thể áp dụng partition pruning và min/max stats của row group.<br><br>**Bài B (Consumer Crash):** Consumer vận hành theo cơ chế `at-most-once` (commit offset trước, ghi đĩa sau). Khi tiến trình chết tại `maybe_crash()`, offset đã tăng nhưng dữ liệu chưa được ghi xuống DuckDB. Khi khởi động lại, consumer đọc từ offset mới và bỏ qua lô đó dẫn đến mất dữ liệu. |
| **Cách khắc phục** | **Bài A:**<br>1. `tools/compact.py`: Dùng `COPY ... TO 'data/gold_events_v2'` với `PARTITION_BY (event_date)` (14 thư mục theo ngày), `ORDER BY customer_name, event_time` và `ROW_GROUP_SIZE 2000`.<br>2. `queries/dashboard.sql`: Trỏ sang `data/gold_events_v2/**/*.parquet` với `hive_partitioning = true`, chuyển điều kiện lọc thành sargable: `where customer_name = 'ACME' and event_date = '2026-08-09'`.<br><br>**Bài B:**<br>1. `ingest/consumer.py`: Tạo bảng `bronze_events_stream` có `event_id varchar primary key`.<br>2. Trong `write_batch()`: Dùng cú pháp `INSERT INTO ... ON CONFLICT (event_id) DO UPDATE SET ...` để đảm bảo tính idempotent.<br>3. Trong `consume()`: Đảo thứ tự thao tác thành ghi dữ liệu xuống kho trước (`write_batch`), sau đó mới commit offset (`consumer.commit()`), chuyển ngữ nghĩa sang `at-least-once`. |
| **Bằng chứng** | **Bài A:** `rows scanned` giảm từ 5.000.000 xuống **137.186** (giảm **36,4 lần**, mục tiêu ≥ 10×) · `files` giảm từ 5.000 xuống **14 file** · `result hash` giữ nguyên `4379e4c5d9f3` · thời gian thực thi giảm từ 110.512 ms xuống **12,9 ms**.<br><br>**Bài B:** `make crash-test` đạt ✓ (A = 20.000 hàng, B crash ở lô 7 offset commit 3.000, C chạy nốt đạt đúng 20.000 hàng, `C == A`, không mất bản ghi, không trùng bản ghi). |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra các model incremental: đã có `unique_key` và `incremental_strategy` rõ ràng (`merge`/`delete+insert`) chưa, hay đang để dbt tự `append` ngầm định. Kiểm tra tính *idempotency* của pipeline khi chạy lại cùng một batch/partition. |
| 2 | Kiểm tra phân bố độ trễ dữ liệu giữa thời điểm phát sinh sự kiện (`event_time`) và thời điểm nạp vào kho (`_ingested_at`) để lượng hoá P95/P99. Đảm bảo các bộ lọc incremental có lookback window đủ rộng để hứng late-arriving data thay vì chỉ lọc `> max(event_time)`. |
| 3 | Kiểm tra Data Contract tại ranh giới Bronze-Silver: schema validation, contract enforcement, và cơ chế xử lý dữ liệu ngoại lệ (Dead Letter Queue / Quarantine table) thay vì để pipeline crash toàn bộ hoặc ép kiểu ngầm làm sinh `NULL`. |
