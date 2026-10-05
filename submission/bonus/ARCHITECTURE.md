# Architecture Brief: Nền Tảng Lakehouse CDC Xử Lý Dữ Liệu Ride-Hailing Thời Gian Thực Tuân Thủ Nghị Định 13/2023/NĐ-CP

**Tác giả:** Phan Đức Duy (MSSV: 2A202602397)  
**Mã môn học:** K4-Track02-Day18 Lakehouse Architecture  
**Topic:** Topic C — CDC từ ride-hailing Việt Nam → Lakehouse với yêu cầu bảo vệ dữ liệu cá nhân  

---

## 1. Problem Statement

Hệ thống vận hành ứng dụng gọi xe tại Việt Nam phục vụ **100 triệu chuyến đi/năm**, với lưu lượng giờ cao điểm đạt **30.000 writes/giây** trên cơ sở dữ liệu giao dịch Oracle DB. Dữ liệu CDC (Change Data Capture) bao gồm tọa độ GPS thời gian thực, số điện thoại, định danh tài xế/hành khách (CCCD), phương thức thanh toán và nhật ký trạng thái chuyến. 

Hệ thống đối mặt với 3 thách thức cốt lõi:
1. **Tuân thủ pháp lý nghiêm ngặt:** Nghị định 13/2023/NĐ-CP yêu cầu mã hóa/ẩn danh hóa dữ liệu cá nhân (PII), có khả năng thu hồi quyền và xóa dữ liệu vĩnh viễn (Right to be Forgotten) trong vòng 72 giờ, đồng thời ghi vết kiểm toán (audit log) mọi truy cập PII.
2. **Sự kiện đến muộn và mất thứ tự (Late-arriving & Out-of-order data):** Mạng 4G/5G chập chờn tại các vùng ven và tầng hầm khiến sự kiện GPS/hoàn thành chuyến đến trễ tới vài giờ.
3. **SLA khắt khe với ngân sách tối ưu:** Dashboard điều hành cập nhật dữ liệu với độ trễ (freshness) < 60 giây từ nguồn commit; truy vấn ad-hoc p95 < 1 giây; tổng chi phí hạ tầng (compute + storage) không vượt quá **$4.000/tháng**.

---

## 2. Kiến Trúc Tổng Thể (Architecture Diagram)

Kiến trúc triển khai theo mô hình **Medallion Architecture**, tích hợp các cơ chế cốt lõi của Lakehouse Day 18:

```
[ Oracle DB OLTP ] (Peak: 30K w/s)
        │
        ▼ (Log-based CDC)
[ Debezium + Apache Kafka ] (Topics: rides, users, drivers, gps)
        │
        ▼ (Micro-batch 10s via Delta Streaming Engine)
┌────────────────────────────────────────────────────────────────────────┐
│ 1. BRONZE LAYER (Landing & Zero-Trust Tokenization)                    │
│  - Stateless KMS Tokenizer (HMAC-SHA256 Salted)                       │
│  - Raw payload lưu dạng Parquet append-only                            │
│  - PII (CCCD, SĐT) được thay bằng UUID token; Khóa giải mã lưu tại Vault│
│  - Delta format: _delta_log transaction recording                      │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼ (MERGE WHEN MATCHED AND src.ts > tgt.ts)
┌────────────────────────────────────────────────────────────────────────┐
│ 2. SILVER LAYER (Cleansed, SCD Type 2 & Deletion Vectors)              │
│  - Khử trùng trùng lặp, xử lý out-of-order bằng event_timestamp        │
│  - Delta Change Data Feed (CDF) kích hoạt cho downstream audit        │
│  - Deletion Vectors (DVs) hỗ trợ Soft/Hard Delete tuân thủ NĐ 13      │
│  - Z-Order / Liquid Clustering theo (geohash_prefix, ride_date)        │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼ (Aggregations & Materialized Views)
┌────────────────────────────────────────────────────────────────────────┐
│ 3. GOLD LAYER (High-Performance Dimensional Serving)                   │
│  - Daily & Hourly Aggregates (Doanh thu, p50/p95 latency, ETA drift)   │
│  - DuckDB / Trino Cache phục vụ BI Dashboard (Freshness < 60s)         │
│  - Ad-hoc Query p95 < 1s thông qua Min/Max File Stats & Bloom Filters  │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │
                                   ▼
┌────────────────────────────────────────────────────────────────────────┐
│ CONTROL PLANE & GOVERNANCE (Catalog + Audit Plane)                     │
│  - Unity Catalog / Polaris REST Catalog: Quản lý metadata & ACLs       │
│  - Dynamic Column Masking: Nhân viên nghiệp vụ chỉ thấy masked PII    │
│  - Audit Table: Ghi log 100% truy vấn giải mã token                   │
└────────────────────────────────────────────────────────────────────────┘
```

### 4 Khái niệm Day 18 được áp dụng cụ thể:
1. **Delta Transaction Log & Deletion Vectors:** Cho phép cập nhật và xóa PII theo NĐ 13 mà không phải ghi lại toàn bộ file Parquet dung lượng lớn, giảm I/O write amplification từ 100× xuống 1×.
2. **Delta Change Data Feed (CDF):** Biến đổi mọi thao tác cập nhật/xóa thành luồng sự kiện có cấu trúc để audit log tự động thu thập và đồng bộ sang index phụ.
3. **Z-Order Clustering theo Không-Thời Gian `(geohash, ride_date)`:** Gom cụm vật lý các chuyến đi gần nhau về địa lý và thời gian, kích hoạt file-skipping giúp truy vấn phạm vi quét ít hơn 90% số file.
4. **Time Travel & RESTORE:** Cung cấp khả năng kiểm toán trạng thái dữ liệu tại bất kỳ thời điểm nào trong 30 ngày và rollback tức thì khi có sự cố logic hoặc pipeline lỗi.

---

## 3. Quyết Định Kiến Trúc & Alternatives Đã Loại

### Quyết định 1: Lựa chọn Định dạng Bảng (Table Format)
- **Lựa chọn:** **Delta Lake 3.x**.
- **Lý do loại Apache Iceberg:** Mặc dù Iceberg có REST Catalog trung lập xuất sắc, nhưng cơ chế merge-on-read xóa hàng (equality deletes) của Iceberg đòi hỏi chi phí tính toán lớn khi scan và compactor thường xuyên bị quá tải ở tải 30K ops/s. Delta Lake có Deletion Vectors (DVs) ổn định hơn trên các engine thực thi micro-batch phổ biến và tích hợp sẵn Change Data Feed (CDF) chuẩn hóa cao.
- **Lý do loại Apache Hudi:** Hudi tối ưu cho streaming CDC nhưng metadata phức tạp, tooling hệ sinh thái Python (delta-rs, DuckDB) hỗ trợ Hudi kém hơn rất nhiều so với Delta Lake.

### Quyết định 2: Chiến lược Xử lý Dữ liệu Đến Muộn (Late-Arriving CDC)
- **Lựa chọn:** **Delta MERGE với vị từ thời gian đơn điệu (`WHEN MATCHED AND src.event_ts > tgt.event_ts THEN UPDATE`)**.
- **Lý do loại Append-only Log + Query-time Deduplication (Window Functions):** Mô hình append-only đẩy toàn bộ gánh nặng tính toán dedup (dùng `ROW_NUMBER() OVER (...)`) vào thời điểm truy vấn, khiến p95 query latency vượt quá 10 giây trên tập dữ liệu hàng trăm triệu dòng, vi phạm SLA < 1s của dashboard.
- **Lý do loại Staging Buffer + Batch Micro-reprocessing:** Giữ dữ liệu trong staging buffer 1 giờ để đợi dữ liệu đến muộn sẽ vi phạm nghiêm trọng SLA dữ liệu tươi < 60 giây cho ban điều phối xe.

### Quyết định 3: Kiến trúc Bảo vệ Dữ liệu Cá Nhân (PII Security & Compliance)
- **Lựa chọn:** **HMAC-SHA256 Tokenization tại Bronze Landing Gate kết hợp Hashicorp Vault KMS**.
- **Lý do loại Mã hóa Cột cấp DB (AES-256 Column Encryption):** Mã hóa toàn bộ chuỗi khiến cột PII mất tính chất cấu trúc, không thể thực hiện join hoặc aggregate theo user_id nếu không giải mã, làm tăng tải CPU của compute cluster lên 300%.
- **Lý do loại Lưu PII Plaintext ở Bronze và chỉ Masking ở Silver:** Vi phạm nguyên tắc bảo vệ dữ liệu ngay từ thiết kế (Privacy by Design) của NĐ 13. Nếu storage bucket Bronze bị rò rỉ hoặc cấp quyền nhầm, toàn bộ số điện thoại và CCCD của hàng triệu khách hàng sẽ lộ ra ngoài.

### Quyết định 4: Chiến lược Phân Vùng và Gom Cụm File (Clustering Strategy)
- **Lựa chọn:** **Phân vùng ngày `date(event_ts)` kết hợp Z-Order Clustering trên `(geohash_prefix, driver_id)`**.
- **Lý do loại Hive-style Multi-level Partitioning (`/year/month/day/hour/city`):** Gây ra thảm họa file nhỏ (small-file problem) với hàng chục ngàn thư mục và file < 1MB, làm nghẽn metadata log và tăng chi phí S3 ListBucket API.
- **Lý do loại Flat Parquet không Clustering:** Truy vấn tìm kiếm hành trình của 1 xe trong 1 quận bắt buộc phải quét toàn bộ partition ngày (~50 GB), gây lãng phí I/O và không đạt p95 < 1s.

### Quyết định 5: Engine Truy Vấn và Tính Toán Phục Vụ (Query Engine)
- **Lựa chọn:** **DuckDB cho local caching/embedded aggregate + Spark Structured Streaming cho Ingestion**.
- **Lý do loại Trino Cluster quy mô lớn chạy 24/7:** Chi phí duy trì Trino cluster tối thiểu 5 worker node lên tới $1.800/tháng, vượt quá ngân sách hạ tầng cho giai đoạn đầu.
- **Lý do loại Direct Query trực tiếp từ S3 bằng Athena:** Chi phí Athena quét $5/TB dữ liệu sẽ tăng vọt nếu dashboard auto-refresh 60 giây một lần ($5 × 0.05 TB × 1440 lần/ngày = $360/ngày = $10.800/tháng).

---

## 4. Ước Tính Chi Phí Thực Tế (Back-of-Envelope Math)

### Quy mô dữ liệu:
- **Số chuyến đi:** 100.000.000 chuyến/năm ≈ 274.000 chuyến/ngày.
- **Event CDC phát sinh:** Trung bình 25 sự kiện/chuyến (booking, accept, pickup, route GPS 5s/lần, dropoff, payment, rating) → **6.850.000 CDC events/ngày**.
- **Giờ cao điểm:** 30.000 writes/giây (khoảng 2 tiếng/ngày).
- **Kích thước trung bình mỗi raw event:** 400 bytes raw JSON → 2.74 GB raw/ngày.
- **Sau nén Snappy/Parquet + Tokenization:** ~0.8 GB Parquet/ngày.
- **Dữ liệu Silver + Gold (1 năm):** 365 × 1.2 GB = ~438 GB dữ liệu nén/năm.

### Bảng tính chi phí hàng tháng (Cloud Region Singapore / VN):
| Hạng mục hạ tầng | Cấu hình & Khối lượng | Đơn giá | Thành tiền / tháng |
|---|---|---|---|
| **S3 Storage (Hot Tier)** | 5 TB (dữ liệu Bronze + Silver + Gold 1 năm + Delta log) | $0.023 / GB-tháng | **$115 / tháng** |
| **S3 Storage (Glacier)** | 10 TB (Cold audit log > 1 năm) | $0.004 / GB-tháng | **$40 / tháng** |
| **Kafka / Redpanda Cluster** | 3 broker (m6g.large, 2 vCPU, 8GB RAM, 500GB SSD) | $0.10 / giờ × 3 | **$216 / tháng** |
| **Streaming Compute (Spark/delta-rs)** | 2 worker r6g.xlarge (auto-scale lên 4 lúc peak) | $0.25 / giờ trung bình | **$360 / tháng** |
| **Hashicorp Vault KMS / KMS API** | 50M requests tokenization/tháng | $0.03 / 10K requests | **$150 / tháng** |
| **Query Engine & Dashboard Server** | 1 instance c6g.2xlarge chạy DuckDB/FastAPI cache | $0.34 / giờ | **$245 / tháng** |
| **Data Egress & Misc** | Egress nội vùng + S3 API calls (PUT/GET) | Trọn gói ước tính | **$120 / tháng** |
| **TỔNG CỘNG** | | | **~$1.246 / tháng** |

> **Nhận xét:** Tổng chi phí thực tế chỉ **~$1.246/tháng**, thấp hơn rất nhiều so với trần ngân sách cho phép ($4.000/tháng), dành dư địa 68% ngân sách cho việc dự phòng đột biến lưu lượng (Spike headroom).

---

## 5. Kịch Bản Sự Cố (Failure Modes & Recovery)

### Sự cố 1: Out-of-order CDC gây sai lệch trạng thái chuyến xe lúc 3:00 AM
- **Hiện tượng:** Tài xế trả khách dưới tầng hầm chung cư mất sóng 4G; sự kiện `COMPLETED` phát sinh lúc 02:45 nhưng đến 03:15 mới gửi đến Kafka, sau khi sự kiện hủy chuyến nhầm `CANCELLED` đã ghi vào Silver.
- **Cơ chế phát hiện:** Silver validation rule quét các bản ghi có trạng thái kết thúc chuyến xung đột hoặc `event_timestamp` chênh lệch > 15 phút so với `ingest_timestamp`.
- **Cơ chế khắc phục (Day 18 concept):** Nhờ vị từ `WHEN MATCHED AND src.event_ts > tgt.event_ts` trong câu lệnh MERGE, sự kiện đến muộn nhưng có timestamp thật lớn hơn sẽ ghi đè chính xác trạng thái thực tế. Trong trường hợp cần rollback logic, sử dụng **Delta Time Travel** `RESTORE TABLE silver.rides TO VERSION AS OF <safe_version>` và replay lại batch sự kiện từ Kafka topic với offset đã chốt.

### Sự cố 2: Yêu cầu Xóa Dữ liệu Khách hàng theo Nghị định 13 (Right to be Forgotten)
- **Hiện tượng:** Khách hàng yêu cầu xóa toàn bộ lịch sử di chuyển và thông tin cá nhân. Nếu chỉ xóa ở Silver/Gold, bản ghi raw ở Bronze và các vector index dẫn xuất vẫn còn tồn tại.
- **Cơ chế phát hiện:** Hệ thống tiếp nhận ticket DPO (Data Protection Officer) tự động kiểm tra số lượng bản ghi còn tồn tại của `user_token`.
- **Cơ chế khắc phục (Day 18 concept):**
  1. Xóa khóa token của khách hàng trong KMS/Vault (Crypto-shredding: lập tức vô hiệu hóa khả năng đọc dữ liệu PII ở mọi tầng).
  2. Thực thi `DELETE FROM bronze.rides_raw WHERE user_token = 'xyz'` và `DELETE FROM silver.rides WHERE user_token = 'xyz'`.
  3. Nhờ **Deletion Vectors**, lệnh DELETE hoàn thành trong vài mili-giây mà không cần rewrite Parquet files.
  4. Lắng nghe **Delta Change Data Feed (CDF)** để broadcast sự kiện `delete` sang downstream vector/search engine nhằm thu hồi vector embedding ngay lập tức.
  5. Chạy job dọn dẹp vật lý `VACUUM RETAIN 0 HOURS` (sau khi đã chốt phiên bản an toàn).

### Sự cố 3: Thảm họa File Nhỏ (Small-File Sprawl) do Micro-batch Ingestion
- **Hiện tượng:** Ingestion chạy micro-batch 10 giây trong 24 giờ tạo ra 8.640 file Parquet nhỏ (~100 KB) mỗi ngày. Hiệu năng scan của dashboard giảm 80%, thời gian query tăng từ 0.5s lên 12s.
- **Cơ chế phát hiện:** Prometheus metric giám sát chỉ số `table_file_count / table_size_mb`. Cảnh báo kích hoạt khi số file trung bình > 10 file/MB.
- **Cơ chế khắc phục (Day 18 concept):**
  - Kích hoạt cơ chế **Auto-Compaction** (`delta.autoOptimize.optimizeWrite = true`).
  - Lên lịch chạy job bảo trì định kỳ ban đêm (Off-peak maintenance job lúc 02:00 sáng): `OPTIMIZE silver.rides ZORDER BY (geohash, ride_date)`. Giảm số lượng file từ 8.640 xuống ~20 file lớn (128 MB), khôi phục truy vấn p95 về 0.4s.

---

## 6. Kế Hoạch Triển Khai MVP Trong 1 Tuần (One-Week MVP Slice)

### Mục tiêu MVP:
Xây dựng một lát cắt dọc (vertical slice) hoàn chỉnh chứng minh tính khả thi của: Ingestion CDC → Tokenization PII → Delta MERGE xử lý out-of-order → Dashboard truy vấn ad-hoc p95 < 1s.

| Ngày | Hạng mục thực hiện | Tiêu chí nghiệm thu (Acceptance Criteria) |
|---|---|---|
| **Ngày 1** | Dựng Kafka giả lập & Tokenizer service bằng Python + HMAC-SHA256 | Tokenizer mã hóa 50.000 records/giây; đầu ra che giấu 100% SĐT và CCCD. |
| **Ngày 2** | Xây dựng pipeline Bronze landing ghi dữ liệu vào Delta Table | Dữ liệu cập nhật liên tục mỗi 10 giây; `_delta_log` ghi nhận commit đều đặn. |
| **Ngày 3** | Cấu hình Silver pipeline với logic MERGE xử lý late-arriving events | Bơm 10.000 sự kiện cố tình đảo lộn thứ tự thời gian; kết quả cuối cùng phản ánh đúng trạng thái của timestamp mới nhất. |
| **Ngày 4** | Kích hoạt Deletion Vectors và Change Data Feed (CDF) | Thực hiện xóa 1 khách hàng; đo thời gian thực thi DELETE < 1 giây; CDF phát sinh event delete. |
| **Ngày 5** | Triển khai Z-Order trên `geohash` và kết nối DuckDB query engine | Benchmark truy vấn 1 triệu dòng: thời gian quét < 500 ms; file pruning ratio đạt ≥ 8×. |
| **Ngày 6-7** | Test tải mô phỏng 30K ops/s, kiểm tra audit log và hoàn thiện tài liệu | Hệ thống ổn định không crash; audit table ghi nhận đủ 100% các request giải mã. |

### Cách thức kiểm tra cơ chế khó nhất (Hardest Mechanism Verification):
Cơ chế khó nhất là **Đảm bảo tính nhất quán của trạng thái chuyến xe khi mạng chập chờn gây ra late-arriving CDC updates với khối lượng lớn**.
- **Cách test:** Tạo kịch bản thử nghiệm tải với 100.000 sự kiện, trong đó 20% sự kiện được cố tình làm trễ từ 1 đến 60 phút và tráo đổi thứ tự gửi vào Kafka (gửi `COMPLETED` trước `STARTED`).
- **Xác minh:** Chạy kiểm thử tự động so sánh trạng thái cuối cùng trong bảng Delta Silver với bảng chân lý (ground truth) của Oracle DB. Độ lệch cho phép = **0.00%**.
