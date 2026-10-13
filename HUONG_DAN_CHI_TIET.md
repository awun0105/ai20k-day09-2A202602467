# HƯỚNG DẪN CHI TIẾT LAB DAY 9: BẢN CHẤT GỐC (FIRST PRINCIPLES) VÀ CÁCH VẬN HÀNH

> **Dành cho**: Sinh viên / Kỹ sư dữ liệu muốn hiểu thấu đáo bản chất của hệ thống data pipeline cho xe tự lái (Autonomous Vehicle), không học vẹt theo công thức.

---

## PHẦN 1: BẢN CHẤT GỐC (FIRST PRINCIPLES THINKING) CỦA BÀI TOÁN

Thay vì ghi nhớ các công cụ như Airflow, Delta Lake, dbt hay DuckDB như những "hộp đen", chúng ta hãy bóc tách bài toán về những **chân lý vật lý và toán học không thể chối cãi (Ground Truths)**:

### 1. Bản chất của xe tự lái và dữ liệu cảm biến
* **Thực tế vật lý**: Một chiếc xe chạy ngoài đường mang theo camera (chụp ảnh 10–30 lần mỗi giây) và hàng loạt cảm biến vật lý (đo vận tốc qua vòng quay bánh xe, GPS đo tọa độ, con quay hồi chuyển đo gia tốc và góc lái).
* **Vấn đề cốt tử**:
  1. **Hai dòng dữ liệu lệch pha**: Camera ghi lại hình ảnh quang học, còn máy tính trên xe ghi nhận số liệu điện áp/vận tốc. Đồng hồ của máy tính xe và đồng hồ của camera có thể lệch nhau vài giây (Clock Offset). Nếu ta lấy ảnh xe đang ở ngã tư ghép với vận tốc xe lúc đang dừng đèn đỏ cách đó 1.5 giây, AI sẽ học sai hoàn toàn: *"Thấy ngã tư đông đúc thì đạp ga 70 km/h!"* -> Gây tai nạn chết người.
  2. **Dữ liệu thế giới thực luôn có "rác"**: Camera bị bùn đất che đen xì, xe chui vào hầm tối, cảm biến rung lắc mất kết nối (dropout), hoặc gửi về số đo âm, số đo trên trời (vận tốc 1000 km/h).
  3. **Video liên tục có tính "thừa mứa" (High Correlation)**: 1 giây quay 10 khung hình. Nếu xe dừng đèn đỏ 60 giây, ta có 600 tấm ảnh giống hệt nhau. Đưa cả 600 tấm này vào huấn luyện sẽ làm mô hình bị "nghiện" cảnh đèn đỏ đó và quên mất cách lái xe trên đường vắng.

---

## PHẦN 2: KIẾN TRÚC MEDALLION QUA CÁC VÍ DỤ ĐỜI THƯỜNG

Kiến trúc Medallion (Bronze -> Silver -> Gold) không phải là thuật ngữ trừu tượng, nó giống hệt quy trình sản xuất nước khoáng đóng chai:

```
[Nước suối tự nhiên]  ──>  [Bể lắng thô (Bronze)]  ──>  [Lọc khuẩn/Khử độc (Silver)]  ──>  [Nước tinh khiết đóng chai (Gold)]
(Camera & Telemetry)        (Lưu nguyên bản thô)         (Chuẩn hóa, cách ly lỗi)         (Dataset huấn luyện AI)
```

### Bước 1: Bronze — "Bể lắng thô" (Ghi nguyên bản, chỉ thêm)
* **Nguyên lý gốc**: Không bao giờ được phép vứt bỏ hay sửa đổi dữ liệu thô ngay khi mới nhận. Nếu bạn ép kiểu dữ liệu ngay từ đầu (ví dụ ép cột tốc độ thành số thực `float`), khi nguồn gửi về một chữ cái rác `"N/A"` hoặc ô trống, hệ thống sẽ sập (crash) hoặc xóa mất dòng đó.
* **Quy tắc**:
  - Lưu mọi thứ dưới dạng chuỗi (`text/string`).
  - Nguồn nào lưu bảng nấy (`raw_telemetry_a`, `raw_telemetry_b`), vì mỗi bên dùng đơn vị và tên cột khác nhau (A đo bằng `m/s`, B đo bằng `km/h`).
  - Nhận diện file bằng dấu vân tay nội dung (SHA256). Kể cả bạn nạp 10 lần, hệ thống chỉ lưu 1 lần (Idempotent).

### Bước 2: Silver Telemetry — "Lọc khuẩn & Chuẩn hóa đơn vị"
* **Nguyên lý gốc**: Để so sánh và tính toán, tất cả phải cùng một hệ quy chiếu:
  - Vận tốc: Đổi tất cả từ `km/h` sang chuẩn đo lường quốc tế `m/s`.
  - Đồng hồ: Lấy thời gian của nguồn B trừ đi độ lệch đồng hồ (1500 ms) để kéo timeline của cảm biến B khớp hoàn hảo với video.
* **Tại sao không xóa dòng lỗi mà đưa vào Quarantine (Khu cách ly)?**
  - Giống như xét nghiệm y tế: Người bệnh được đưa vào phòng cách ly để bác sĩ nghiên cứu và chữa trị, không ai "thủ tiêu" bệnh nhân. Dòng lỗi đưa vào `silver_telemetry_rejects` giúp kỹ sư biết cảm biến xe nào đang hỏng phần cứng, và có thể tái xử lý (reprocess) sau này.
* **Cổng chất lượng (Quality Gate): Sự khác nhau giữa `warn` và `error`**:
  - `warn` (Cảnh báo): Mất tín hiệu vài giây (`max_gap_ms > 1000ms`). Ngoài đời đi qua hầm mất sóng là chuyện bình thường, chỉ cần nhắc nhở chứ không làm sập cả nhà máy.
  - `error` (Dừng khẩn cấp): Tốc độ xe ghi nhận 1000 km/h (`in_range 0..70 m/s`). Đây là dữ liệu nhiễm độc nghiêm trọng, bắt buộc phải dừng toàn bộ dây chuyền để không làm ô nhiễm tập huấn luyện của AI.

### Bước 3: Silver Frames — "Trích xuất khung hình & Bắt bệnh thị giác"
* **Nguyên lý gốc**: Video là chuỗi ảnh liên tiếp nén lại (H.264). Ta lấy mẫu 2 khung/giây (2 fps) để giảm tải tính toán.
* **Phân biệt Khung Đen (`is_black`) và Khung Tối (`is_dark`)**:
  - Khung Đen (Black): Camera hỏng hoặc bị bịt kín. Độ sáng = 0. Không có thông tin thị giác -> **Bỏ**.
  - Khung Tối (Dark): Xe chui vào hầm, đi trời tối hoặc bóng cây râm. Vẫn còn hình ảnh làn đường và đèn xe -> **Gắn cờ nhưng Giữ Lại**. Nếu bỏ khung tối, xe tự lái sẽ "bị mù" vào ban đêm!
* **Khung trùng lặp (dHash - Difference Hash)**:
  - dHash biến bức ảnh thành một chuỗi 64 bit vân tay.
  - Nếu xe dừng đèn đỏ hoặc camera bị đơ hình (đoạn 6s ở drive A), các khung hình kề nhau có độ lệch dHash rất nhỏ (0–2 bit). Pipeline chọn **ngưỡng 2** để loại bỏ các ảnh trùng lặp này, tránh làm AI học vẹt.
* **Tính Idempotent của Delta Lake**:
  - Ghi theo partition (`replaceWhere="trip_id = '...'"`): Ghi file mới và commit nguyên tử trong Transaction Log. Nếu nửa chừng mất điện, bản cũ vẫn nguyên vẹn 100%, không bao giờ có chuyện mất dữ liệu hay bị nhân đôi dòng như cách ghi `append`.

### Bước 4: Gold — "Tập dữ liệu đóng gói cho Nhà khoa học AI"
* **Nguyên tắc chống rò rỉ (Data Leakage - Chân lý quan trọng nhất)**:
  - Nếu chia ngẫu nhiên theo từng khung hình (Per-frame split): Khung hình lúc 10h00:00.0 nằm ở tập `train`, khung hình lúc 10h00:00.5 nằm ở tập `test`. Hai khung hình này giống hệt nhau về quang cảnh! Mô hình lúc kiểm thử thực chất đã "nhìn thấy đề thi", điểm số đạt 99% một cách giả tạo, nhưng đem ra đường thật thì đâm xe.
  - Do đó, **bắt buộc phải chia theo Chuyến đi (Per-trip)**: Toàn bộ khung hình của một chuyến xe chỉ nằm trong train HOẶC val HOẶC test.
* **Mã phiên bản (Dataset Version)**:
  - Không dùng ngày giờ (vì mỗi lần bấm chạy giờ lại đổi).
  - Dùng **mã băm nội dung (Content Hash - 12 ký tự hex)**: Nội dung dữ liệu không đổi thì mã phiên bản không bao giờ đổi.
  - Mã sinh viên (MSSV) được dùng làm "muối" (salt) để sắp xếp các chuyến đi, tạo ra một dataset độc bản duy nhất cho bạn.

### Bước 5: Phục vụ & Vận hành (Warehouse, Airflow, Metabase)
* **Airflow**: Nhạc trưởng điều phối. Khi 1 trip bị sập (ví dụ `trip-07304`), Airflow chỉ đánh dấu đỏ task đó và tạm dừng các bước sau. Khi sự cố được khắc phục, ta chỉ cần bấm **Clear** đúng task đó để chạy lại từ điểm lỗi mà không cần chạy lại toàn bộ từ đầu.
* **Warehouse (Postgres)**: Nạp các bảng tổng hợp (Marts) phục vụ báo cáo.
* **Metabase**: Giao diện trực quan hóa dữ liệu qua biểu đồ cho kỹ sư theo dõi sức khỏe của hệ thống thu thập dữ liệu.

---

## PHẦN 3: LÀM SAO ĐỂ CHỤP ĐƯỢC ẢNH MÀN HÌNH CHÍNH XÁC?

Nhiều bạn thắc mắc: *"AI hoạt động trong terminal (dòng lệnh), không có màn hình hiển thị (GUI), làm sao có thể chụp được ảnh giao diện web như người thật?"*

Dưới đây là kỹ thuật chuyên sâu được áp dụng:

### 1. Trình duyệt không đầu (Headless Browser)
* Bình thường khi bạn mở Chrome, hệ điều hành sẽ vẽ cửa sổ trình duyệt lên màn hình thông qua giao diện đồ họa (X11 / Wayland).
* Tuy nhiên, Google Chrome có chế độ **Headless (Không đầu)**: Trình duyệt chạy hoàn toàn ngầm trong bộ nhớ CPU/RAM, tự dựng cây DOM, tải CSS, thực thi JavaScript, vẽ giao diện lên một bộ đệm ảnh ảo (virtual framebuffer) mà không cần mở cửa sổ đồ họa.

### 2. Giao thức điều khiển Chrome DevTools Protocol (CDP) & Puppeteer
* Chúng ta sử dụng thư viện tự động hóa **Puppeteer** (Node.js) để kết nối trực tiếp với Chrome engine:
  ```javascript
  const browser = await puppeteer.launch({
    executablePath: '/usr/bin/google-chrome',
    headless: 'new',
    args: ['--no-sandbox', '--disable-gpu'],
    defaultViewport: { width: 1600, height: 900 } // Đặt độ phân giải màn hình ảo
  });
  ```

### 3. Vượt qua lớp đăng nhập bằng Cookie / API Token
Các giao diện Airflow, MinIO, Metabase đều có lớp bảo vệ bắt đăng nhập. Nếu chụp thẳng URL, bạn chỉ chụp được trang Login (ảnh vài KB). Cách xử lý thực tế:
1. **Với Airflow**:
   - Gửi yêu cầu lấy `csrf_token`, sau đó gửi lệnh POST chứa username/password (`admin`/`admin`) vào form đăng nhập. Trình duyệt lưu `session cookie` và chuyển thẳng tới trang Graph: `http://localhost:8080/dags/day09_video_pipeline/grid?tab=graph`.
2. **Với MinIO**:
   - Dùng Puppeteer tự động điền `#accessKey` (`minioadmin`), `#secretKey` (`minioadmin`), kích hoạt sự kiện click nút đăng nhập, chờ tải xong giao diện cây thư mục `lake/` rồi mới chụp.
3. **Với Metabase**:
   - Gọi trực tiếp REST API `POST /api/session` để lấy token đăng nhập (`f6499ca1-...`).
   - Nạp token này vào cookie `metabase.SESSION` của trình duyệt ảo.
   - Điều hướng trực tiếp vào trang Dashboard số 2 (`/dashboard/2`), đợi 6 giây cho toàn bộ các biểu đồ và bảng SQL vẽ xong xuôi trên Canvas, sau đó gọi hàm:
     ```javascript
     await page.screenshot({ path: 'report/screenshots/metabase_dashboard.png' });
     ```

Nhờ quy trình này, toàn bộ ảnh chụp trong thư mục `report/screenshots/` đều là **ảnh thật 100% với độ phân giải cao (1600x900)**, hiển thị đầy đủ biểu đồ, dữ liệu và trạng thái hoạt động của hệ thống.
