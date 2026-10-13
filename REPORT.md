# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Lâm Quang Anh Quân` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602467` |
| GitHub | `awun0105` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07304` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Bốn khác biệt lớn giữa telemetry nguồn A và nguồn B được ghi nhận tại lớp Bronze:
1. **Định dạng timestamp**: Nguồn A dùng chuỗi ISO 8601 UTC (`ts_utc`, ví dụ `2025-10-07T08:00:00.014Z`), trong khi nguồn B dùng Unix epoch tính bằng mili-giây (`timestamp_ms`, ví dụ `1759933801509`).
2. **Đơn vị và tên cột vận tốc**: Nguồn A dùng `speed_mps` với đơn vị mét/giây (m/s), nguồn B dùng `spd_kmh` với đơn vị kilômét/giờ (km/h).
3. **Quy ước đặt tên cột và tỷ lệ điều khiển**: Nguồn A dùng `heading_deg`, `lat`, `lon`, `alt_m`, `acc_x`, `acc_y`, bàn đạp `gas`/`brake` dạng tỷ lệ [0..1]; nguồn B dùng `hdg`, `latitude`, `longitude`, `altitude`, `ax`, `ay`, bàn đạp `throttle_pct`/`brake_pct` theo phần trăm [0..100%], góc lái `steering` thay vì `steer`.
4. **Độ lệch đồng hồ thiết bị (clock offset)**: Nguồn A có độ lệch 0 ms (`telemetry_clock_offset_ms = 0`), còn nguồn B có đồng hồ cảm biến bị lệch 1500 ms so với camera video (`telemetry_clock_offset_ms = 1500`).

Pipeline giữ mỗi nguồn một bảng Bronze riêng và lưu mọi cột dưới dạng văn bản (string/text) thô vì:
- **Bảo toàn dữ liệu gốc (Fidelity & Audit Trail)**: Theo nguyên lý Medallion (Slide 14), tầng Bronze lưu trữ nguyên vẹn dữ liệu từ nguồn gốc mà không chỉnh sửa, cho phép phục hồi và tính toán lại khi logic tầng Silver thay đổi mà không mất mát thông tin.
- **Tránh vỡ quá trình Ingestion (Fault Tolerance & Schema Drift)**: Giữ kiểu chữ giúp ingestion luôn thành công dù dữ liệu có null, ký tự rác, hay định dạng bất thường. Việc chuẩn hóa schema, ép kiểu và xử lý dữ liệu bẩn được tách biệt và giao cho tầng Silver (dbt) đảm nhận.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline đưa hai nguồn về một schema thống nhất thông qua các mô hình dbt staging (`stg_telemetry_a` và `stg_telemetry_b`):
- **Đơn vị**: Nguồn B chuyển đổi vận tốc từ km/h sang m/s bằng cách nhân với `(1000.0 / 3600.0)`, đưa về cùng cột `speed_mps` như nguồn A. Bàn đạp `throttle_pct` và `brake_pct` từ thang 0..100% được chia cho 100.0 để về thang 0..1. Các cột góc lái và GPS cũng được chuẩn hóa tên cột thống nhất (`heading_deg`, `lat`, `lon`, `alt_m`, `acc_x`, `acc_y`, `gas`, `brake`, `steer`).
- **Kiểu thời gian**: Nguồn A chuyển chuỗi ISO 8601 sang timestamp UTC. Nguồn B chuyển epoch ms sang timestamp UTC bằng `epoch_ms(timestamp_ms)`.
- **Độ lệch đồng hồ (Clock Offset)**: Nguồn B có đồng hồ cảm biến bị lệch 1500 ms so với camera. Trong `stg_telemetry_b.sql`, pipeline lấy giá trị `telemetry_clock_offset_ms = 1500` từ `sources.json` và trừ đi: `(epoch_ms(timestamp_ms) - interval '1500 milliseconds')`, giúp timestamp cảm biến khớp hoàn toàn với timeline của video.
- **Bằng chứng**: Ở bảng kết quả bước 2, thời điểm bắt đầu của telemetry B sạch là `2025-10-08 14:30:00.009000+00:00`, khớp chính xác với mốc thời gian bắt đầu của video `drive_b` (`2025-10-08 14:30:00`), chênh lệch chỉ 9 ms (nhỏ hơn 0.5s theo quy chuẩn kiểm tra).

Bảng quarantine thực tế của hệ thống:
```
drive_id  reject_reason       so_dong
--------  ------------------  -------
drive_a   duplicate           20     
drive_a   speed_missing       30     
drive_a   speed_out_of_range  3      
drive_b   duplicate           20     
drive_b   speed_missing       30     
drive_b   speed_out_of_range  3      
```
Mỗi drive có đúng 53 dòng lỗi bị cách ly; tổng dòng sạch + cách ly đúng bằng số dòng nhận về (Drive A: 2968 + 53 = 3021; Drive B: 3181 + 53 = 3234).

Dòng lỗi được đưa vào bảng `silver.silver_telemetry_rejects` thay vì bị xoá bỏ im lặng vì:
1. **Audit & Giám sát chất lượng phần cứng**: Giúp kỹ sư phân tích nguyên nhân lỗi (cảm biến nào chập chờn, bị mất mẫu hay đo sai vận tốc) và đo lường tỷ lệ lỗi theo thời gian (SLA).
2. **Khả năng phục hồi và tái xử lý (Replay/Reprocessing)**: Nếu ngưỡng làm sạch trong tương lai được điều chỉnh hợp lý hơn hoặc có thuật toán bù suy diễn mới, các dòng này vẫn còn để xử lý lại mà không bị mất dữ liệu vĩnh viễn.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

- **Phân biệt `warn` và `error`**:
  - `max_gap_ms` đặt là `warn` vì việc mất tín hiệu cảm biến tạm thời (sensor dropouts > 1000 ms) là hiện tượng thực tế thường gặp khi xe đi qua vùng khuất sóng hoặc giật bus CAN. Đây là khiếm khuyết được dung thứ (tolerated defect), các khung hình và đoạn telemetry bình thường khác vẫn có giá trị huấn luyện, nên chỉ cần cảnh báo để theo dõi mà không làm sập pipeline.
  - `in_range` (vận tốc [0..70 m/s] tương đương tối đa 252 km/h) đặt là `error` vì vận tốc vượt ngưỡng vật lý là dữ liệu hoàn toàn hỏng (fatal defect/sensor corruption). Nếu để lọt dữ liệu vận tốc sai lệch này vào tập huấn luyện mô hình lái xe tự động, xe có thể học sai mô hình điều khiển dẫn đến tai nạn chết người. Vì vậy, pipeline buộc phải dừng ngay lập tức khi phát hiện.

- **Kết quả thí nghiệm `break-gate` (`break_gate.json`)**:
  - Khi nới lỏng luật lọc từ 70 m/s lên 1000 m/s, các bản ghi vận tốc vô lý lọt qua bước staging vào `silver_telemetry`.
  - Cổng chất lượng dbt test lập tức phát hiện: test `in_range_silver_telemetry_speed_mps__70__0` bị FAIL và bắt được chính xác **6 dòng** (3 dòng từ drive_a và 3 dòng từ drive_b).
  - Tóm tắt kết quả test: khi nới luật đạt `PASS=5 WARN=1 ERROR=1 SKIP=0`; sau khi khôi phục luật chuẩn trở về `PASS=6 WARN=1 ERROR=0 SKIP=0`.

- **Vai trò của "Cổng chất lượng" (Quality Gate)**:
  - Cổng chất lượng đóng vai trò như một cơ chế phòng vệ chiều sâu (defense-in-depth). Kể cả khi tầng biến đổi upstream có sai sót, code bị nới lỏng nhầm, hoặc xuất hiện edge case mới lọt qua bộ lọc, quality gate độc lập sẽ chặn đứng dữ liệu bẩn ngay tại ranh giới tầng Silver, ngăn không cho dữ liệu độc hại lan truyền xuống tầng Gold và mô hình AI.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
