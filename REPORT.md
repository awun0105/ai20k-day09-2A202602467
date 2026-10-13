# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `Lâm Quang Anh Quân` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602467` |
| GitHub | `awun0105` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07304` |

## Kết quả cuối

- `dataset_version` của bạn: `59e1ed53d281`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `764 train / 141 val / 114 test` (tổng 1019 khung hình)
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

Dựa vào file bằng chứng `idempotency.json` thu được từ thí nghiệm trên partition `trip-07304`:
- **Số dòng trước và sau khi chạy lại**: Bảng `silver_frames` ban đầu có 1246 dòng với dấu vân tay (fingerprint) là `6c7480dfde7e`. Sau hai lần chạy lại partition `trip-07304`, tổng số dòng vẫn chính xác là **1246 dòng** và dấu vân tay vẫn là `6c7480dfde7e` (giá trị `unchanged: true`). Điều này chứng minh tính **idempotent**: chạy lại nhiều lần không sinh ra dữ liệu trùng lặp.
- **Biến thể ghi kiểu `append`**: Partition `trip-07304` có 34 khung hình (`rows_of_trip: 34`). Khi chạy lại 2 lần với phương thức `append`, số dòng của trip bị đội lên thành **68 dòng** (nhân đôi 2x), làm ô nhiễm dữ liệu nghiêm trọng.
- **Vì sao Delta `overwrite` theo partition an toàn hơn xoá rồi ghi lại (DELETE + INSERT)**:
  - Delta Lake sử dụng cơ chế transaction log (ACID commitments). Khi thực hiện `overwrite` với điều kiện partition (`replaceWhere="trip_id = 'trip-07304'"`), Delta ghi các file Parquet mới vào storage và thực hiện một commit nguyên tử duy nhất (atomic commit) trong transaction log.
  - Nếu hệ thống bị sập giữa chừng trong lúc trích xuất hoặc ghi file, transaction chưa commit sẽ bị huỷ bỏ (abort), các file rác bị bỏ qua, và snapshot cũ vẫn còn nguyên vẹn cho downstream reader đọc.
  - Ngược lại, nếu dùng mô hình xóa trước ghi sau (DELETE rồi INSERT), nếu tiến trình chết giữa chừng sau khi xóa, partition sẽ bị mất trắng dữ liệu (data loss), gây downtime và lỗi dây chuyền cho toàn bộ hệ thống.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

- **Khung đen vs khung tối**:
  - **Khung hình đen (`is_black`)**: Xảy ra khi camera gặp lỗi phần cứng, mất tín hiệu cảm biến hoặc nắp đậy/vật cản che hoàn toàn ống kính (độ sáng trung bình ~ 0). Khung đen hoàn toàn không mang thông tin thị giác, gây hại cho gradient huấn luyện của mô hình AI, do đó pipeline **loại bỏ hoàn toàn** khỏi tập Gold.
  - **Khung hình tối (`is_dark` / `toi_giu_lai`)**: Xuất hiện tự nhiên khi xe di chuyển qua đường hầm, hầm chui, bóng râm cây cối hoặc điều kiện thiếu sáng. Khung hình này vẫn chứa các đặc trưng thị giác quan trọng về làn đường, đèn chiếu và vật cản trong điều kiện thực tế. Pipeline **gắn cờ nhận diện nhưng vẫn giữ lại** (drive A có 4 khung, drive B có 61 khung tối được giữ lại).
  - **Nhận định**: Tôi hoàn toàn đồng ý với cách phân loại này. Xe tự lái bắt buộc phải học cách vận hành an toàn trong điều kiện thiếu sáng; loại bỏ khung tối sẽ tạo ra lỗ hổng tri thức (blind spot) cho mô hình AI khi lái xe ban đêm hoặc trong hầm.

Bảng số khung trùng theo ngưỡng dHash từ `threshold.json`:

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

- **Lựa chọn ngưỡng và phân tích**:
  - Tôi chọn **ngưỡng 2** (như thiết kế của pipeline).
  - **Lý do**:
    - Ngưỡng 0 quá khắt khe: chỉ nhận diện trùng khi 64 bit của dHash giống nhau tuyệt đối. Trong thực tế, nhiễu cảm biến camera nhẹ hoặc rung lắc nhỏ khiến 2 khung hình xe đứng yên vẫn lệch 1-2 bit, dẫn đến bỏ sót nhiều khung trùng (chỉ bắt được 61 ở A và 8 ở B).
    - Ngưỡng 6 quá lỏng: coi sự sai khác tới 6 bit là trùng, loại bỏ tới hơn một nửa dataset (363/602 khung ở A, 244/644 ở B), làm mất đi rất nhiều khung hình có giá trị khi xe đang di chuyển thực sự.
    - Ngưỡng 2 là điểm cân bằng tối ưu (sweet spot): lọc sạch hiện tượng đứng hình (như lỗi camera đóng băng 6s ở drive A) và các đoạn xe dừng chờ, trong khi không làm mất dữ liệu khi xe di chuyển.
  - **Sự phụ thuộc vào tốc độ xe**:
    - Khi xe chạy chậm hoặc dừng chờ (vận tốc thấp/bằng 0): khoảng cách di chuyển giữa hai khung hình lấy mẫu (0.5s ở 2 fps) là rất nhỏ, cảnh vật xung quanh hầu như không thay đổi, dẫn đến khoảng cách dHash rất nhỏ (0 - 2 bit). Do đó drive A (tốc độ chậm) có nhiều khung trùng hơn hẳn drive B.
    - Khi xe chạy tốc độ cao: quãng đường xe đi được sau 0.5s lớn hơn nhiều, quang cảnh và vạch đường trôi nhanh, khoảng cách dHash giữa các khung hình liên tiếp tăng cao (> 3-4 bit).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Dựa vào file bằng chứng `leakage.json` (tổng cộng 1019 khung hình sạch thuộc 26 trip):
- **Chia ngẫu nhiên theo khung hình (per-frame split 70/15/15)**: Toàn bộ **26/26 trip** bị chia cắt trên nhiều split (`per_frame_split_trips_straddling: 26`), và cả **26/26 trip** đều có khung hình xuất hiện đồng thời ở cả tập `train` và tập `test` (`per_frame_split_trips_in_both_train_and_test: 26`).
- **Chia theo trip (per-trip split của pipeline)**: Có đúng **0 trip** bị chia cắt (`per_trip_split_trips_straddling: 0`). Mỗi trip chỉ thuộc về duy nhất một split (18 trip cho train, 4 trip cho val, 4 trip cho test).
- **Vì sao điểm kiểm thử đẹp một cách giả tạo (Data Leakage)**:
  - Video xe tự lái được ghi liên tục với tần số cao (lấy mẫu 2 fps), nghĩa là hai khung hình kề nhau cách nhau chỉ 0.5 giây. Chúng gần như giống hệt nhau về phối cảnh không gian, điều kiện thời tiết, góc chiếu sáng, vạch kẻ đường và hành vi của các phương tiện xung quanh.
  - Khi chia ngẫu nhiên theo khung hình, khung hình $t$ nằm trong tập `train` còn khung hình $t + 0.5s$ nằm trong tập `test`. Mô hình AI sẽ không học các quy luật tổng quát (generalization) mà chỉ ghi nhớ (memorize) bối cảnh cụ thể của chuyến đi. Khi đánh giá, mô hình thực chất "đã thấy trước đề thi", khiến các chỉ số kiểm thử (loss, accuracy, mAP) cao ngất ngưởng một cách giả tạo. Khi triển khai trên thực tế với cung đường hoàn toàn mới (unseen trips), mô hình sẽ suy giảm độ chính xác nghiêm trọng và tiềm ẩn nguy cơ tai nạn.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

- **Vì sao là băm nội dung (Content Hash)**:
  - `dataset_version` là chuỗi băm 12 ký tự hex được tính toán trực tiếp từ nội dung dữ liệu (danh sách ID khung hình, phân chia split, nhãn telemetry và metadata manifest).
  - Điều này đảm bảo tính **tất định (deterministic)** và **tái lập tuyệt đối (reproducibility)**: Bất kỳ ai, chạy ở bất kỳ thời điểm nào hay trên máy chủ nào, nếu dữ liệu đầu vào và logic không đổi thì mã băm sinh ra luôn giống hệt nhau.
  - Nếu dùng ngày giờ (timestamp) hoặc số đếm tăng dần (counter): mỗi lần chạy lại pipeline (kể cả khi dữ liệu không hề thay đổi) sẽ sinh ra một phiên bản mới, làm rác kho lưu trữ mô hình (Model Registry) và kích hoạt việc huấn luyện lại mô hình tốn kém không cần thiết.
- **Kết quả hai lần chạy liên tiếp**: Cho ra đúng một `dataset_version` duy nhất không đổi (`59e1ed53d281`).
- **Vì sao `dataset_version` khác bạn bè dù cùng dữ liệu**:
  - Pipeline sử dụng `LAB_STUDENT_ID` (MSSV `2A202602467`) làm giá trị "muối" (salt) để băm MD5 khi sắp xếp và phân bổ các trip vào các tập train/val/test.
  - Do MSSV của mỗi sinh viên là duy nhất, thứ tự phân bổ các trip vào các split là khác nhau giữa các sinh viên. Dẫn tới danh sách khung hình gán cho train, val, test khác nhau, và hash nội dung của dataset cuối cùng (`dataset_version`) là hoàn toàn riêng biệt cho từng người.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

- **Dataset này được tạo từ những dữ liệu nào (trích xuất từ `manifest.json`)**:
  - Các bảng Delta upstream và commit version tương ứng:
    - `bronze.raw_telemetry_a`: version 0
    - `bronze.raw_telemetry_b`: version 0
    - `bronze.trips_raw`: version 0
    - `bronze.video_files`: version 1
    - `silver.frames`: version 27
    - `silver.telemetry`: version 0
    - `silver.frames_flagged`: version 0
    - `silver.frame_telemetry`: version 0
  - Hai file video gốc được định danh bất biến qua mã SHA256:
    - `drive_a`: `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`
    - `drive_b`: `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`
  - Các quy tắc lọc chất lượng (`rules.drop`): loại bỏ các khung hình `is_black`, `is_blurry`, `is_near_dup`, và `not sync_ok`.
  - Chiến lược phân chia split: `per trip, stratified by drive, md5 order`.

- **Thông tin giúp tái tạo chính xác dataset này sau 3 tháng**:
  1. Hai tệp video gốc có đúng mã băm SHA256 như trên.
  2. Số commit version của từng bảng Delta nguồn (đặc biệt là commit v27 của `silver.frames` và v0 của các bảng telemetry/trips) thông qua tính năng Time Travel của Delta Lake.
  3. Giá trị MSSV `LAB_STUDENT_ID = 2A202602467` làm salt phân chia split và các tham số lọc chất lượng (ngưỡng dHash = 2, max_gap = 1000ms, sync_tol = 500ms).
  4. Mã `manifest_sha256 = 120a3da8d92ff0f3183a7622a47f61407e9e2f0fcd5d0c7ddd4994a5282e5d3f` dùng để kiểm chứng toàn vẹn (checksum) tập dữ liệu sau khi tái tạo.

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
