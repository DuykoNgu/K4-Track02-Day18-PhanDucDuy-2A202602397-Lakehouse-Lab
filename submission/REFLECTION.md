# Reflection: Lakehouse Anti-Patterns

Trong hệ thống RAG và LLM Observability, anti-pattern nguy hiểm nhất là **Stale External Vector Index (Vòng đời Vector Index tách rời Lakehouse)**.

Khi dữ liệu nguồn thay đổi hoặc có yêu cầu xóa dữ liệu cá nhân (GDPR, Nghị định 13), lệnh `DELETE` trên bảng Lakehouse diễn ra tức thì nhờ transaction log. Tuy nhiên, nếu index vector bên ngoài (Milvus, Qdrant) đồng bộ qua batch một chiều — vốn chỉ chú trọng Upsert và bỏ qua Delete — thì các vector của tài liệu đã xóa vẫn tồn tại vĩnh viễn và bị truy xuất vào context LLM, gây rò rỉ dữ liệu nghiêm trọng.

**Giải pháp:** Bật Change Data Feed (CDF) trên Lakehouse để biến mọi thao tác Delete/Update thành event có cấu trúc, buộc worker phải evict vector khỏi index ngoài; hoặc lưu trữ vector ngay trong bảng Lakehouse để quản lý vòng đời nguyên khối thống nhất.

*Khai báo AI:* Dùng AI hỗ trợ tự động hóa chạy notebook và rà soát tiêu chí rubric.
