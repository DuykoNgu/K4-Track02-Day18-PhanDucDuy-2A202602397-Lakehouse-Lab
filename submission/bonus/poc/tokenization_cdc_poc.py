"""Proof-of-Concept (PoC) cho Bonus Architecture Topic C.

Minh họa 3 cơ chế kỹ thuật cốt lõi:
1. Zero-trust PII Tokenization (HMAC-SHA256 Salted) tại Bronze landing.
2. Xử lý CDC Out-of-order & Late-arriving data bằng Delta MERGE (`src.event_ts > tgt.event_ts`).
3. Tuân thủ Quyền xóa dữ liệu (Nghị định 13) qua Delta Deletion & Change Data Feed (CDF).
"""
import hmac
import hashlib
import time
import datetime as dtm
from pathlib import Path
import polars as pl
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

import shutil

POC_DIR = Path("./_lakehouse/bonus_poc")
TABLE_SILVER = str(POC_DIR / "rides_silver")

SALT = b"secret_kms_salt_vietnam_ride_hailing_2026"

def tokenize_pii(val: str) -> str:
    """Mã hóa một chiều PII có muối (salted HMAC) tạo token định danh an toàn."""
    return hmac.new(SALT, val.encode("utf-8"), hashlib.sha256).hexdigest()[:16]

def run_poc():
    print("=== BONUS PoC: VIETNAM RIDE-HAILING CDC & PII LAKEHOUSE ===")
    shutil.rmtree(TABLE_SILVER, ignore_errors=True)
    POC_DIR.mkdir(parents=True, exist_ok=True)
    
    # 1. Giả lập luồng sự kiện CDC với PII nhạy cảm (SĐT, CCCD)
    raw_events = [
        {"ride_id": "R001", "phone": "0912345678", "cccd": "001202001234", "status": "REQUESTED", "event_ts": 100},
        {"ride_id": "R002", "phone": "0987654321", "cccd": "001202005678", "status": "REQUESTED", "event_ts": 105},
    ]
    
    # Tokenize PII tại Bronze gateway
    tokenized = []
    for e in raw_events:
        tokenized.append({
            "ride_id": e["ride_id"],
            "user_token": tokenize_pii(e["phone"]),
            "driver_token": tokenize_pii(e["cccd"]),
            "status": e["status"],
            "event_ts": e["event_ts"],
            "ingest_ts": int(time.time()),
        })
    df_init = pl.DataFrame(tokenized)
    
    # Ghi khởi tạo Silver Delta Table với Change Data Feed
    write_deltalake(
        TABLE_SILVER,
        df_init.to_arrow(),
        mode="overwrite",
        configuration={"delta.enableChangeDataFeed": "true"}
    )
    print(f"[OK] Khoi tao Silver table voi {len(tokenized)} ban ghi da tokenized PII.")
    print(f"     Mau token: user_token={tokenized[0]['user_token']} (SĐT goc da an toan)")

    # 2. Xử lý CDC Out-of-order & Late data
    # Giả lập sự kiện R001 đến muộn: sự kiện COMPLETED (ts=120) và ACCEPTED (ts=110)
    # Cố tình gửi sự kiện COMPLETED trước, ACCEPTED đến sau!
    cdc_batch_1 = pl.DataFrame([{
        "ride_id": "R001", "user_token": tokenize_pii("0912345678"),
        "driver_token": tokenize_pii("001202001234"), "status": "COMPLETED", "event_ts": 120, "ingest_ts": int(time.time())
    }])
    
    # Merge batch 1 (COMPLETED)
    dt = DeltaTable(TABLE_SILVER)
    (dt.merge(source=cdc_batch_1.to_arrow(), predicate="t.ride_id = s.ride_id", source_alias="s", target_alias="t")
     .when_matched_update_all(predicate="s.event_ts > t.event_ts")
     .when_not_matched_insert_all()
     .execute())
    
    # Giờ sự kiện ACCEPTED (ts=110) đến muộn do mất sóng 4G
    cdc_batch_2_late = pl.DataFrame([{
        "ride_id": "R001", "user_token": tokenize_pii("0912345678"),
        "driver_token": tokenize_pii("001202001234"), "status": "ACCEPTED", "event_ts": 110, "ingest_ts": int(time.time())
    }])
    
    dt = DeltaTable(TABLE_SILVER)
    (dt.merge(source=cdc_batch_2_late.to_arrow(), predicate="t.ride_id = s.ride_id", source_alias="s", target_alias="t")
     .when_matched_update_all(predicate="s.event_ts > t.event_ts")
     .when_not_matched_insert_all()
     .execute())
    
    # Kiểm tra: status của R001 bắt buộc phải giữ là COMPLETED (không bị ACCEPTED ghi đè)
    cur = DeltaTable(TABLE_SILVER).to_pyarrow_table()
    cur_status = cur.filter(pa.compute.equal(cur.column("ride_id"), "R001")).column("status").to_pylist()[0]
    print(f"[OK] Kiem tra Out-of-order CDC: Trạng thái hiện tại của R001 = '{cur_status}'")
    assert cur_status == "COMPLETED", f"Lỗi: Sự kiện cũ đã ghi đè sự kiện mới! (got {cur_status})"
    print("     -> Vi tu 's.event_ts > t.event_ts' da chan thanh cong su kien den muon!")

    # 3. Tuân thủ Quyền được lãng quên (Right to be Forgotten - NĐ 13/2023)
    target_token = tokenize_pii("0912345678")
    dt = DeltaTable(TABLE_SILVER)
    dt.delete(f"user_token = '{target_token}'")
    
    # Kiểm tra dữ liệu trong bảng hiện tại
    dt_after_del = DeltaTable(TABLE_SILVER)
    remaining_count = dt_after_del.to_pyarrow_table().filter(
        pa.compute.equal(dt_after_del.to_pyarrow_table().column("user_token"), target_token)
    ).num_rows
    print(f"[OK] Thuc thi DELETE cho khach hang co token '{target_token}': Con lai {remaining_count} ban ghi trong table.")
    assert remaining_count == 0, "Lỗi: Bản ghi chưa được xóa!"

    # Kiểm tra Delta Change Data Feed (CDF) đã bắt được sự kiện xóa để downstream evict cache/vector
    cdf_changes = dt_after_del.load_cdf(starting_version=0).read_all()
    change_types = cdf_changes.column("_change_type").to_pylist()
    num_deletes = change_types.count("delete")
    print(f"[OK] Delta Change Data Feed phat sinh {num_deletes} su kien delete de audit & evict downstream.")
    assert num_deletes > 0, "Lỗi: CDF không ghi nhận sự kiện delete!"

    print("\n>>> TẤT CẢ CƠ CHẾ KIẾN TRÚC TRONG PoC ĐÃ HOẠT ĐỘNG HOÀN HẢO! <<<")

if __name__ == "__main__":
    run_poc()
