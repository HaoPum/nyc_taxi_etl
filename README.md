# 🚖 End-to-End Data Lakehouse: NYC Taxi Analytics Platform

!Architecture Diagram
!Status
!Docker

## 📖 Tổng quan (Overview)

Dự án này là một hệ thống **Data Lakehouse** hoàn chỉnh, được xây dựng để xử lý, lưu trữ và phân tích dữ liệu chuyến đi Taxi tại New York (NYC Taxi Dataset) kết hợp với dữ liệu thời tiết.

Mục tiêu chính của dự án là xây dựng một pipeline dữ liệu hiện đại (**Modern Data Stack**), giải quyết các bài toán thực tế trong Data Engineering:
1.  **Xử lý dữ liệu lớn (Big Data Processing):** Sử dụng Apache Spark.
2.  **Lưu trữ tin cậy (Reliable Storage):** Sử dụng Apache Iceberg để đảm bảo tính ACID cho Data Lake.
3.  **Điều phối luồng công việc (Orchestration):** Quản lý các dependency phức tạp bằng Apache Airflow.
4.  **Streaming & CDC:** Tích hợp dữ liệu thời gian thực từ Database transactional thông qua Kafka và Debezium.
5.  **Phân tích & Trực quan hóa:** Sử dụng Trino để truy vấn và Superset để làm Dashboard.

---

## 🏗 Kiến trúc hệ thống (Architecture)

Hệ thống được triển khai hoàn toàn trên Docker Containers, mô phỏng một môi trường Production thu nhỏ:

| Thành phần | Công nghệ sử dụng | Vai trò |
| :--- | :--- | :--- |
| **Ingestion** | Python, Kafka, Debezium | Thu thập dữ liệu từ API và Transactional DB (Postgres). |
| **Processing** | **Apache Spark** (PySpark) | Xử lý dữ liệu phân tán, transform và load vào Lakehouse. |
| **Orchestration** | **Apache Airflow** | Lên lịch, quản lý dependency giữa các pipelines (Sensors). |
| **Storage Format** | **Apache Iceberg** | Định dạng bảng mở, hỗ trợ Time Travel, Schema Evolution. |
| **Object Storage** | **MinIO** (S3 Compatible) | Lưu trữ vật lý các file data (Parquet/Iceberg). |
| **Catalog** | Hive Metastore | Quản lý metadata cho các bảng Iceberg. |
| **Query Engine** | **Trino** | Truy vấn SQL tốc độ cao trực tiếp trên Data Lake. |
| **Visualization** | **Apache Superset** | BI Dashboard kết nối với Trino. |

---

## 🔄 Các luồng dữ liệu (Data Pipelines)

Dự án bao gồm 3 pipeline chính được điều phối bởi Airflow:

### 1. Batch Pipeline: NYC Taxi Data (`nyc_taxi_iceberg_etl`)
*   **Nguồn:** Dữ liệu NYC Taxi (Parquet) từ internet.
*   **Logic:**
    *   Tải dữ liệu Incremental theo tháng.
    *   Kiểm tra chất lượng dữ liệu (Data Quality Check) trước khi xử lý.
    *   Sử dụng Spark để ghi dữ liệu vào bảng Iceberg (`nyc_taxi_trips`) trên MinIO.
    *   Cập nhật bảng Control để quản lý trạng thái tải (Watermark).

### 2. Complementary Pipeline: Weather Data (`nyc_weather_etl`)
*   **Nguồn:** OpenWeatherMap API (hoặc Mock data).
*   **Logic:**
    *   Thu thập dữ liệu thời tiết hàng giờ.
    *   Tạo dữ liệu tham chiếu (Reference Data) cho các Zone của Taxi.
    *   Lưu trữ vào Iceberg để phục vụ việc phân tích tương quan (Ví dụ: Trời mưa thì lượng taxi thay đổi thế nào?).

### 3. Comprehensive Analytics (`comprehensive_analytics_pipeline`)
*   **Logic:** Đây là DAG tổng hợp.
    *   Sử dụng **Airflow Sensors** (`ExternalTaskSensor`) để đợi dữ liệu Taxi và Weather hoàn tất.
    *   Chạy Spark Job để join các bảng, tính toán các chỉ số kinh doanh (Business Metrics).
    *   Chuẩn bị Feature Store cho Machine Learning.

---

## 🛠 Cài đặt & Chạy dự án (Installation)

Dự án yêu cầu Docker và Docker Compose.

### 1. Clone Repository
```bash
git clone https://github.com/your-username/nyc-taxi-lakehouse.git
cd nyc-taxi-lakehouse
```

### 2. Khởi chạy hạ tầng
```bash
docker-compose up -d --build
```
*Lưu ý: Quá trình này có thể mất vài phút để tải images và khởi tạo các services (Spark, Kafka, Airflow, Trino, v.v.).*

### 3. Thiết lập kết nối (Connections)
Chạy script để tự động tạo các kết nối trong Airflow:
```bash
docker-compose exec airflow-webserver python /opt/airflow/dags/Create_connections.py
```

### 4. Truy cập giao diện
*   **Airflow UI:** `http://localhost:8080` (User/Pass: `airflow`/`airflow`)
*   **MinIO Console:** `http://localhost:9001` (User/Pass: `admin`/`password`)
*   **Spark Master:** `http://localhost:8080` (Port container map ra ngoài có thể khác, check docker-compose)
*   **Superset:** `http://localhost:8089`
*   **Trino:** `http://localhost:8084`

---

## 💡 Điểm nổi bật về kỹ thuật (Technical Highlights)

### Tại sao lại là Apache Iceberg?
Trong dự án này, tôi chọn Iceberg thay vì lưu file Parquet truyền thống vì:
*   **Schema Evolution:** Dữ liệu Taxi thay đổi cấu trúc theo thời gian, Iceberg xử lý việc này mà không cần viết lại toàn bộ dữ liệu.
*   **ACID Transactions:** Đảm bảo dữ liệu không bị lỗi khi nhiều pipeline cùng ghi/đọc.
*   **Partitioning ẩn:** Tối ưu hóa truy vấn mà người dùng không cần biết cấu trúc thư mục vật lý.

### Xử lý Dependency trong Airflow
Thay vì lập lịch cố định dễ gây lỗi, tôi sử dụng `ExternalTaskSensor` trong DAG `comprehensive_analytics_pipeline`. Pipeline phân tích sẽ **chỉ chạy** khi và chỉ khi pipeline ETL dữ liệu gốc đã thành công, đảm bảo tính toàn vẹn dữ liệu.

### Infrastructure as Code
Toàn bộ môi trường từ Database, Message Queue (Kafka), Compute Engine (Spark) đến BI tool đều được định nghĩa trong `docker-compose.yml`, giúp việc triển khai nhất quán trên mọi môi trường.

---

## 📊 Kết quả phân tích (Analytics Preview)

Dữ liệu sau khi xử lý cho phép trả lời các câu hỏi:
*   Khu vực nào có nhu cầu taxi cao nhất vào giờ cao điểm?
*   Thời tiết (Mưa/Tuyết) ảnh hưởng thế nào đến giá cước và tiền Tip?
*   Xu hướng di chuyển thay đổi thế nào theo mùa?

---

## 🚀 Hướng phát triển (Future Improvements)
*   Tích hợp **dbt (data build tool)** để quản lý các transformation SQL trong Trino/Spark tốt hơn.
*   Triển khai **Great Expectations** để kiểm soát chất lượng dữ liệu chặt chẽ hơn.
*   Xây dựng model Machine Learning dự đoán nhu cầu xe (Demand Forecasting).

---

## 👤 Tác giả

**[Tên của bạn]**
*   Data Engineer
*   Email: [Email của bạn]
*   LinkedIn: [Link Profile của bạn]

---
*Dự án này được xây dựng nhằm mục đích học tập và nghiên cứu các công nghệ Big Data hiện đại.*
