# Thông Tin Bài Nộp K4-Track02-Day18 Lakehouse Lab

- **Họ và tên:** Phan Đức Duy
- **Mã số sinh viên (MSSV):** 2A202602397
- **Mã bài lab:** K4-Track02-Day18
- **Tên repository:** `K4-Track02-Day18-PhanDucDuy-2A202602397-Lakehouse-Lab`
- **Tài khoản GitHub:** DuykoNgu
- **GitHub Fork URL:** `https://github.com/DuykoNgu/K4-Track02-Day18-PhanDucDuy-2A202602397-Lakehouse-Lab`
- **Upstream Repository:** `https://github.com/VinUni-AI20k/K4-Track02-Day18-Lakehouse-Lab`

## Môi trường thực thi
- **Đường chạy (Execution Path):** Lightweight path (Python native APIs: `deltalake` 1.6.6 + `pyiceberg` 0.12.0 + DuckDB 1.5.6 + Polars 1.44.2 + PyArrow 25.0.1; không JVM, không Docker, không cần API key). Toàn bộ 8 notebook (NB01–NB08) đều chạy trên đường lightweight.
- **Phiên bản Python:** Python 3.12.10
- **Hệ điều hành:** macOS Darwin 24.x (Apple Silicon arm64)

## Kết quả kiểm tra
- **Smoke test (`scripts/verify_lite.py`):** 9/9 checks PASS (Delta write/read/time-travel, CDF, Iceberg catalog & hidden partitioning scan pruning, DuckDB core vector search, DuckDB-Delta zero-copy Arrow bridge).
- **Pytest suite (`make test` / `pytest -q`):** 24/24 tests PASS.
- **Headless suite (`make run-all` / `scripts/run_all.py`):** 8/8 notebooks PASS (16.9s total).
- **Notebooks đã thực thi có output:** Đầy đủ 8 notebook trong `submission/notebooks/`.
- **Ảnh minh chứng:** Đầy đủ 8 ảnh trong `submission/screenshots/` tương ứng từng notebook.
- **Reflection:** Đã hoàn thành tại `submission/REFLECTION.md`.
- **Bonus Challenge:** Đã hoàn thành tại `submission/bonus/ARCHITECTURE.md` (Topic C: CDC từ ride-hailing Việt Nam → Lakehouse với yêu cầu bảo vệ dữ liệu cá nhân theo Nghị định 13/2023/NĐ-CP).
