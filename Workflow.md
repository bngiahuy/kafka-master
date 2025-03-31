
# Hệ thống Kafka Master-Worker - Luồng Hoạt Động
- [Hệ thống Kafka Master-Worker - Luồng Hoạt Động](#hệ-thống-kafka-master-worker---luồng-hoạt-động)
  - [Mô hình tổng quan](#mô-hình-tổng-quan)
  - [Luồng hoạt động chi tiết](#luồng-hoạt-động-chi-tiết)
    - [1. Khởi động và Leader Election](#1-khởi-động-và-leader-election)
      - [1.1 Khởi động Master](#11-khởi-động-master)
      - [1.2 Bầu Leader](#12-bầu-leader)
      - [1.3 Phát hiện và xử lý Leader Fail](#13-phát-hiện-và-xử-lý-leader-fail)
    - [2. Đăng ký và quản lý Worker](#2-đăng-ký-và-quản-lý-worker)
      - [2.1 Worker đăng ký](#21-worker-đăng-ký)
      - [2.2 Master xử lý đăng ký Worker](#22-master-xử-lý-đăng-ký-worker)
      - [2.3 Xử lý cập nhật Worker không đồng bộ](#23-xử-lý-cập-nhật-worker-không-đồng-bộ)
    - [3. Xử lý File và phân công Batch](#3-xử-lý-file-và-phân-công-batch)
      - [3.1 Quét và chia file](#31-quét-và-chia-file)
      - [3.2 Phân công batch cho Worker](#32-phân-công-batch-cho-worker)
      - [3.3 Xử lý khi không có Worker sẵn sàng](#33-xử-lý-khi-không-có-worker-sẵn-sàng)
    - [4. Theo dõi tiến trình và xử lý hoàn thành](#4-theo-dõi-tiến-trình-và-xử-lý-hoàn-thành)
      - [4.1 Worker báo cáo tiến trình](#41-worker-báo-cáo-tiến-trình)
      - [4.2 Master cập nhật tiến trình](#42-master-cập-nhật-tiến-trình)
      - [4.3 Xử lý batch hoàn thành](#43-xử-lý-batch-hoàn-thành)
      - [4.4 Hoàn thành file](#44-hoàn-thành-file)
    - [5. Cơ chế xử lý lỗi](#5-cơ-chế-xử-lý-lỗi)
      - [5.1 Worker timeout](#51-worker-timeout)
      - [5.2 Lỗi gửi batch đến Worker](#52-lỗi-gửi-batch-đến-worker)
      - [5.3 Xử lý rebalance Kafka](#53-xử-lý-rebalance-kafka)
      - [5.4 Master Failover](#54-master-failover)
    - [6. Hệ thống ghi log](#6-hệ-thống-ghi-log)
      - [6.1 Log hệ thống](#61-log-hệ-thống)
      - [6.2 Log theo dõi batch](#62-log-theo-dõi-batch)
      - [6.3 Cấu trúc log batch](#63-cấu-trúc-log-batch)
    - [7. Redis - Kho lưu trữ trạng thái hệ thống](#7-redis---kho-lưu-trữ-trạng-thái-hệ-thống)
      - [7.1 Quản lý Master](#71-quản-lý-master)
      - [7.2 Quản lý Worker](#72-quản-lý-worker)
      - [7.3 Quản lý cập nhật Worker](#73-quản-lý-cập-nhật-worker)
      - [7.4 Quản lý Batch và File](#74-quản-lý-batch-và-file)
    - [8. API và Monitoring](#8-api-và-monitoring)
      - [8.1 API Endpoints chính](#81-api-endpoints-chính)
      - [8.2 Debug và theo dõi hệ thống](#82-debug-và-theo-dõi-hệ-thống)
    - [9. Luồng Shutdown và Xử lý tín hiệu](#9-luồng-shutdown-và-xử-lý-tín-hiệu)
      - [9.1 Xử lý tín hiệu dừng](#91-xử-lý-tín-hiệu-dừng)
      - [9.2 Leader từ bỏ vị trí](#92-leader-từ-bỏ-vị-trí)
  - [Kết luận](#kết-luận)

## Mô hình tổng quan

Hệ thống sử dụng mô hình master-worker để phân phối và xử lý dữ liệu với Kafka làm message broker và Redis làm kho lưu trữ trạng thái.

Markdown Live Preview
Reset
Copy

```


                    ┌────────────┐
                    │   Master   │◄────┐
                    └─────┬──────┘     │
                          │            │
                          ▼            │
     ┌───────────────────┬────────────┬─────────────────┐
     │                   │            │                 │
     ▼                   ▼            ▼                 ▼
┌──────────┐     ┌──────────────┐  ┌──────┐     ┌────────────┐
│  Redis   │     │    Kafka     │  │ API  │     │ Filesystem │
└──────────┘     └──────────────┘  └──────┘     └────────────┘
     ▲                   ▲
     │                   │
     └───────┬───────────┘
             │
┌────────────┴────────────┬─────────────────────┐
│                         │                     │
▼                         ▼                     ▼
┌──────────┐        ┌──────────┐         ┌──────────┐
│ Worker 1 │        │ Worker 2 │   ...   │ Worker N │
└──────────┘        └──────────┘         └──────────┘
```

## Luồng hoạt động chi tiết

### 1. Khởi động và Leader Election

#### 1.1 Khởi động Master
- Khi một master khởi động, nó gọi `startLeaderElection()` để tham gia vào quá trình bầu leader
- Master đăng ký vào Redis sorted set `masters:list` với timestamp làm score
- Mỗi master gửi heartbeat định kỳ đến key `master:heartbeat:{masterId}` với TTL

#### 1.2 Bầu Leader
- Quá trình bầu leader được kiểm soát qua key `master:election:running`
- Master đầu tiên trong danh sách sẽ trở thành leader và được lưu trong `master:active:leader`
- Chỉ một master thực hiện bầu cử bằng cách nhận được lock `leader:election:lock`
- Các master khác ở trạng thái standby và chỉ hoạt động khi leader fail

#### 1.3 Phát hiện và xử lý Leader Fail
- Mỗi master định kỳ kiểm tra heartbeat của leader
- Nếu phát hiện leader không còn hoạt động (heartbeat hết hạn), khởi động cuộc bầu cử mới
- Master tiếp theo trong danh sách sẽ trở thành leader mới

### 2. Đăng ký và quản lý Worker

#### 2.1 Worker đăng ký
- Worker gửi thông điệp đến topic `KAFKA_TOPIC_NAME_WORKER_FREE` với:
  - `workerId`: ID duy nhất của worker
  - `status`: 'new' khi đăng ký lần đầu
  - `partitions`: Danh sách các partition mà worker subscribe

#### 2.2 Master xử lý đăng ký Worker
- Master nhận thông điệp từ topic `KAFKA_TOPIC_NAME_WORKER_FREE`
- Nếu không thể lấy lock worker, lưu thông tin vào hàng đợi `worker:pending:update:{workerId}`
- Nếu lấy được lock, cập nhật thông tin worker:
  - Lưu trạng thái worker vào `worker:status` (1 = sẵn sàng)
  - Lưu thông tin partition vào `worker:partition`

#### 2.3 Xử lý cập nhật Worker không đồng bộ
- Service `workerUpdater` chạy định kỳ kiểm tra key `worker:needs:update:*`
- Khi phát hiện worker cần cập nhật, cố gắng lấy lock
- Nếu thành công, áp dụng cập nhật rồi xóa thông tin chờ

### 3. Xử lý File và phân công Batch

#### 3.1 Quét và chia file
- Master leader chạy `startBatchAssigner()` để quét thư mục input
- Đọc danh sách file mới (chưa có trong set `processed:files`)
- Đọc từng file, chia thành các chunks theo kích thước `numBatches`
- Lưu các chunks vào Redis list `master:fileChunks`

#### 3.2 Phân công batch cho Worker
- Lấy danh sách workers sẵn sàng (`worker:status` = 1)
- Chọn worker, lấy lock và đánh dấu là bận (`worker:status` = 0)
- Lấy chunk từ đầu queue `master:fileChunks`
- Lưu thông tin batch vào `worker:batchInfo`
- Gửi batch thông qua Kafka topic `KAFKA_TOPIC_NAME_MASTER`
- Giải phóng lock worker

#### 3.3 Xử lý khi không có Worker sẵn sàng
- Master chờ và kiểm tra định kỳ
- Nếu master shutdown trong khi chờ, kết thúc vòng lặp và disconnect Kafka producer

### 4. Theo dõi tiến trình và xử lý hoàn thành

#### 4.1 Worker báo cáo tiến trình
- Worker gửi tiến trình thông qua topic `KAFKA_TOPIC_NAME_WORKER` với:
  - `workerId`: ID của worker
  - `batchId`: ID của batch đang xử lý
  - `processing`: Số lượng đã xử lý
  - `total`: Tổng số lượng cần xử lý

#### 4.2 Master cập nhật tiến trình
- Nhận thông điệp tiến trình từ `KAFKA_TOPIC_NAME_WORKER`
- Cập nhật `lastSeen:{workerId}` để đánh dấu worker còn hoạt động
- Lưu tiến trình vào `worker:processing`
- Ghi log tiến trình thông qua `logMessage()`

#### 4.3 Xử lý batch hoàn thành
- Khi `processing` = `total`, master ghi nhận batch hoàn thành
- Ghi log thành công vào `finish-batches.log` qua `logBatchSuccess()`
- Xóa thông tin batch khỏi `worker:batchInfo`
- Đánh dấu worker sẵn sàng để nhận công việc mới

#### 4.4 Hoàn thành file
- Khi tất cả chunks của file đã xử lý, thêm file vào set `processed:files`
- Ghi log thông báo file đã xử lý xong

### 5. Cơ chế xử lý lỗi

#### 5.1 Worker timeout
- Service `checkWorkerStatus` chạy định kỳ kiểm tra tất cả workers
- Phát hiện worker timeout dựa trên:
  - Thời gian kể từ khi gán batch (`worker:batchInfo.assignedAt`) 
  - Thời gian kể từ lần cuối cùng worker truy cập (`lastSeen:{workerId}`)
- Nếu phát hiện timeout:
  - Ghi log lỗi vào `fail-batches.log` qua `logBatchFailure()`
  - Đưa chunk trở lại queue Redis để xử lý lại
  - Xóa thông tin worker khỏi hệ thống

#### 5.2 Lỗi gửi batch đến Worker
- Khi không thể gửi batch đến worker:
  - Đưa chunk trở lại đầu queue `master:fileChunks`
  - Ghi log lỗi
  - Giải phóng lock worker

#### 5.3 Xử lý rebalance Kafka
- Khi Kafka rebalance partition, worker có thể gửi thông tin partition mới
- Cơ chế pending update đảm bảo thông tin sẽ được cập nhật ngay khi có thể
- Tránh xung đột lock giữa các luồng xử lý

#### 5.4 Master Failover
- Nếu master leader dừng hoạt động, master tiếp theo được bầu làm leader
- Leader mới tiếp tục xử lý các chunks trong queue `master:fileChunks`
- Dữ liệu tiến trình được duy trì trong Redis, giúp việc failover không bị mất dữ liệu

### 6. Hệ thống ghi log

#### 6.1 Log hệ thống
- `logMessage()`: Ghi log vào file hệ thống, thông tin chung
- `console.log()`: Hiển thị thông tin debug trên console

#### 6.2 Log theo dõi batch
- `logBatchSuccess()`: Ghi log các batch hoàn thành vào `finish-batches.log`
- `logBatchFailure()`: Ghi log các batch lỗi vào `fail-batches.log`

#### 6.3 Cấu trúc log batch
- Log thành công:
  ```json
  {"workerId":"123","batchId":"file_chunk_1","processedCount":500,"totalCount":500}
  ```
- Log thất bại:
  ```json
  {"workerId":"123","batchId":"file_chunk_2"} - Reason: Worker timeout
  ```

### 7. Redis - Kho lưu trữ trạng thái hệ thống

#### 7.1 Quản lý Master
- `masters:list`: Sorted set các master đang hoạt động
- `master:heartbeat:{masterId}`: Thời gian heartbeat mới nhất
- `master:active:leader`: ID của master đang là leader
- `master:election:running`: Đánh dấu đang diễn ra bầu cử

#### 7.2 Quản lý Worker
- `worker:status`: Hash lưu trạng thái worker (1=sẵn sàng, 0=đang bận)
- `worker:partition`: Hash lưu partition của worker
- `worker:batchInfo`: Hash lưu thông tin batch đang xử lý
- `worker:processing`: Hash lưu tiến trình xử lý
- `lastSeen:{workerId}`: Thời gian cuối worker gửi cập nhật

#### 7.3 Quản lý cập nhật Worker
- `worker:pending:update:{workerId}`: Hash lưu cập nhật chờ xử lý
- `worker:needs:update:{workerId}`: Flag đánh dấu worker cần cập nhật

#### 7.4 Quản lý Batch và File
- `master:fileChunks`: List các chunk đang chờ xử lý
- `processed:files`: Set các file đã xử lý
- `numBatches`: Kích thước chunk mặc định

### 8. API và Monitoring

#### 8.1 API Endpoints chính
- `/api/getWorkersStatus`: Xem trạng thái worker
- `/api/getNumBatches`: Lấy cấu hình kích thước batch
- `/api/updateNumBatches`: Cập nhật kích thước batch
- `/api/getPartitions`: Lấy thông tin partition Kafka

#### 8.2 Debug và theo dõi hệ thống
- Sử dụng Redis CLI để kiểm tra trạng thái:
  ```bash
  redis-cli HGETALL worker:status
  redis-cli LLEN master:fileChunks
  redis-cli GET master:active:leader
  ```
- Kiểm tra log file:
  ```bash
  tail -f finish-batches.log
  tail -f fail-batches.log
  ```

### 9. Luồng Shutdown và Xử lý tín hiệu

#### 9.1 Xử lý tín hiệu dừng
- Bắt các tín hiệu `SIGTERM`, `SIGINT`
- Dừng các service đang chạy (workerUpdater, checkWorkerStatus)
- Disconnect khỏi Kafka
- Cập nhật trạng thái vào Redis trước khi thoát

#### 9.2 Leader từ bỏ vị trí
- Khi leader shutdown, nó xóa thông tin khỏi `master:active:leader`
- Điều này cho phép quá trình bầu leader mới diễn ra nhanh chóng
- Các master khác phát hiện và tiến hành bầu leader mới

## Kết luận

Hệ thống Kafka Master-Worker được thiết kế với khả năng chịu lỗi cao:
- Master failover: Đảm bảo luôn có một master xử lý công việc
- Worker monitoring: Phát hiện và khắc phục các worker lỗi
- Batch retry: Đảm bảo tất cả dữ liệu đều được xử lý
- Hệ thống log: Theo dõi chi tiết trạng thái và lỗi
- Redis làm trung tâm lưu trữ trạng thái: Giúp hệ thống phục hồi nhanh chóng sau lỗi

Tất cả các cơ chế này kết hợp để tạo nên một hệ thống phân tán mạnh mẽ, đáng tin cậy và dễ mở rộng.
