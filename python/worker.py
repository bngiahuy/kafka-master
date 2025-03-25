import os
import json
import aiohttp
import asyncio
import logging
import uuid
from datetime import datetime
import pytz
from confluent_kafka import Consumer, Producer, KafkaError
tz = pytz.timezone('Asia/Ho_Chi_Minh')

# Cấu hình Kafka
KAFKA_BOOTSTRAP_SERVERS = '10.6.18.5:9092'
KAFKA_MASTER_SEND_TOPIC = 'master-signal'
KAFKA_WORKER_SEND_TOPIC = 'worker-signal'
KAFKA_WORKER_CONNECTION_TOPIC = 'worker-connection'
KAFKA_GROUP_ID = 'ip-scanner-group'
KAFKA_AUTO_OFFSET_RESET = 'latest'

# Cấu hình xử lý
MAX_CONCURRENT_REQUESTS = 10  # Điều chỉnh dựa trên giới hạn API

# Thêm hằng số cho file lưu trạng thái
WORKER_ID_FILE = "worker_id.txt"

# Cấu hình logger
def setup_logger():
    logger = logging.getLogger("ip_scanner")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        logger.handlers.clear()
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

# Hợp nhất dữ liệu từ hai API
def merge_ip_data(dataIPInfo, dataShodan, worker_id):
    timestampt = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    if dataShodan:
        if dataIPInfo:
            merged = {
                "ip": dataShodan.get("ip", "N/A"),
                "location": {
                    "city": dataIPInfo.get("city", "Unknown"),
                    "region": dataIPInfo.get("region", "Unknown"),
                    "country": dataIPInfo.get("country", "Unknown"),
                    "loc": dataIPInfo.get("loc", "N/A"),
                    "postal": dataIPInfo.get("postal", "N/A"),
                    "timezone": dataIPInfo.get("timezone", "N/A")
                },
                "organization": dataIPInfo.get("org", "Unknown"),
                "hostnames": dataShodan.get("hostnames", []),
                "port_list": dataShodan.get("ports", []),
                "technologies": {
                    "cpes": dataShodan.get("cpes", []),
                    "tags": dataShodan.get("tags", [])
                },
                "cve_list": dataShodan.get("vulns", []),
                "scan_method": "tool",
                "last_scan": timestampt,
                "vm_id": worker_id
            }
        else:
            merged = {
                "ip": dataShodan.get("ip", "N/A"),
                "hostnames": dataShodan.get("hostnames", []),
                "port_list": dataShodan.get("ports", []),
                "technologies": {
                    "cpes": dataShodan.get("cpes", []),
                    "tags": dataShodan.get("tags", [])
                },
                "cve_list": dataShodan.get("vulns", []),
                "scan_method": "tool",
                "last_scan": timestampt,
                "vm_id": worker_id
            }
        return merged
    return None

# Gọi API không đồng bộ
async def fetch_ip_info(ip, session, logger, semaphore):
    async with semaphore:
        try:
            # Gọi Shodan API
            async with session.get(
                f"https://internetdb.shodan.io/{ip}", 
                timeout=aiohttp.ClientTimeout(total=10)
            ) as responseShodan:
                if responseShodan.status == 404:
                    # logger.info(f"IP {ip}: Shodan API returned 404 (No data found)")
                    return False, None, None, 404
                elif responseShodan.status != 200:
                    # logger.error(f"IP {ip}: Shodan API failed with status {responseShodan.status}")
                    return False, None, None, responseShodan.status
                
                dataShodan = await responseShodan.json()
                
                # Gọi IPInfo API
                try:
                    async with session.get(
                        f"https://ipinfo.io/{ip}/json", 
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as responseIPInfo:
                        if responseIPInfo.status == 200:
                            dataIPInfo = await responseIPInfo.json()
                            return True, dataIPInfo, dataShodan, 200
                        else:
                            # logger.warning(f"IP {ip}: IPInfo API failed with status {responseIPInfo.status}")
                            return True, {}, dataShodan, responseIPInfo.status
                except Exception as e:
                    # logger.error(f"IP {ip}: IPInfo API error: {str(e)}")
                    return True, {}, dataShodan, 0
                
        except Exception as e:
            logger.error(f"IP {ip}: Shodan API error: {str(e)}")
            return False, None, None, 0

# Xử lý một IP và thu thập kết quả
async def process_ip(ip, session, logger, semaphore, worker_id):
    success, dataIPInfo, dataShodan, status_code = await fetch_ip_info(ip, session, logger, semaphore)
    
    # Chuẩn bị kết quả
    result = {
        "ip": ip,
        "success": success,
        "error": None if success else f"API failed with status {status_code}",
        "result": None
    }
    
    if success:
        merged_data = merge_ip_data(dataIPInfo, dataShodan, worker_id)
        if merged_data:
            result["result"] = merged_data
        else:
            result["success"] = False
            result["error"] = "Failed to merge data"
    
    return result

# Lấy hoặc tạo worker ID
def get_worker_id():
    if os.path.exists(WORKER_ID_FILE):
        with open(WORKER_ID_FILE, 'r') as f:
            worker_id = f.read().strip()
            if worker_id:
                return worker_id
    
    # Tạo ID mới nếu không tìm thấy
    worker_id = str(uuid.uuid4())
    with open(WORKER_ID_FILE, 'w') as f:
        f.write(worker_id)
    
    return worker_id

# Thêm hàm mới để gửi thông báo hoàn thành
async def send_done_message(producer, worker_id, logger):
    try:
        # Tạo message hoàn thành
        done_message = {
            "status": "done",
            "id": worker_id
        }
        
        # Log chi tiết về message sẽ gửi
        logger.info(f"Preparing to send done message: {done_message}")
        
        # Gửi lên Kafka
        producer.produce(
            KAFKA_WORKER_CONNECTION_TOPIC,
            key=worker_id.encode('utf-8'),
            value=json.dumps(done_message).encode('utf-8'),
            callback=lambda err, msg: delivery_callback(err, msg, logger)
        )
        producer.flush()
        
        logger.info(f"Sent done message for worker ID: {worker_id}")
        return True
    except Exception as e:
        logger.error(f"Error sending done message: {e}")
        return False

# Cập nhật hàm send_progress_update để thay đổi định dạng tin nhắn
async def send_progress_update(producer, worker_id, batch_id, processed, total, logger):
    try:
        # Tạo message cập nhật tiến độ
        progress_message = {
            "id": worker_id,
            "batchId": batch_id,
            "processing": processed,
            "total": total,
        }
        
        # Gửi lên Kafka
        producer.produce(
            KAFKA_WORKER_SEND_TOPIC,
            key=worker_id.encode('utf-8'),
            value=json.dumps(progress_message).encode('utf-8'),
            callback=lambda err, msg: delivery_callback(err, msg, logger)
        )
        producer.poll(0)
        
        # if processed == total or processed % 50 == 0:
        #     logger.info(f"Batch {batch_id}: Progress {processed}/{total} ({processed/total*100:.2f}%)")
        
        return True
    except Exception as e:
        logger.error(f"Error sending progress update: {e}")
        return False

# Cập nhật hàm delivery_callback để thêm logging chi tiết
def delivery_callback(err, msg, logger):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    # else:
    #     topic = msg.topic()
    #     partition = msg.partition()
    #     offset = msg.offset()
    #     key = msg.key().decode('utf-8') if msg.key() else None
    #     logger.info(f'Message delivered to {topic} [partition={partition}] at offset {offset} with key={key}')

# Tạo thư mục logs và scanned nếu chưa tồn tại
def ensure_directories():
    # Tạo thư mục logs
    logs_folder = "logs"
    os.makedirs(logs_folder, exist_ok=True)
    
    # Tạo thư mục scanned
    scanned_folder = "scanned"
    os.makedirs(scanned_folder, exist_ok=True)
    
    # Tạo file codulieu.txt và khongcodulieu.txt nếu chưa tồn tại
    codulieu_file = os.path.join(logs_folder, "codulieu.txt")
    khongcodulieu_file = os.path.join(logs_folder, "khongcodulieu.txt")
    
    if not os.path.exists(codulieu_file):
        open(codulieu_file, 'w').close()
    
    if not os.path.exists(khongcodulieu_file):
        open(khongcodulieu_file, 'w').close()
    
    return logs_folder, scanned_folder, codulieu_file, khongcodulieu_file

# Xử lý một batch IP
async def process_ip_batch(ip_list, batch_id, worker_id, logger):
    logs_folder, scanned_folder, codulieu_file, khongcodulieu_file = ensure_directories()
    
    # Tạo file JSON cho batch này
    batch_file = os.path.join(scanned_folder, f"{batch_id}.json")
    with open(batch_file, 'w') as f:
        f.write('[\n')  # Bắt đầu mảng JSON
    
    # Tạo producer để gửi cập nhật tiến độ
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 1000,
        'batch.size': 16384,
        'linger.ms': 5,
        'acks': 'all',
        'retries': 5,
        'retry.backoff.ms': 500
    }
    producer = Producer(producer_conf)
    
    # Giới hạn số lượng yêu cầu đồng thời
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    try:
        # Xử lý tất cả IP trong batch
        processed_count = 0
        successful_count = 0
        total_count = len(ip_list)
        has_written_result = False  # Flag để theo dõi xem đã ghi kết quả nào chưa
        
        async with aiohttp.ClientSession() as session:
            tasks = [process_ip(ip, session, logger, semaphore, worker_id) for ip in ip_list]
            
            for future in asyncio.as_completed(tasks):
                result = await future
                processed_count += 1
                
                # Lưu kết quả vào file tương ứng
                if result["success"]:
                    successful_count += 1
                    # Lưu vào file codulieu.txt
                    with open(codulieu_file, 'a') as f:
                        f.write(f"{result['ip']}\n")
                    
                    # Lưu vào file JSON của batch
                    with open(batch_file, 'a') as f:
                        if has_written_result:  # Đã có kết quả trước đó
                            f.write(',\n')
                        json.dump(result["result"], f, separators=(',', ':'))
                        has_written_result = True
                else:
                    # Lưu vào file khongcodulieu.txt
                    with open(khongcodulieu_file, 'a') as f:
                        f.write(f"{result['ip']}\n")
                
                # Gửi cập nhật tiến độ
                await send_progress_update(producer, worker_id, batch_id, processed_count, total_count, logger)
        
        # Đóng file JSON
        with open(batch_file, 'a') as f:
            f.write('\n]')
        
        # Gửi thông báo worker đã hoàn thành và sẵn sàng nhận batch mới
        await send_done_message(producer, worker_id, logger)
        
        logger.info(f"Completed batch {batch_id}: {successful_count}/{total_count} IPs successful")
        return successful_count
    except Exception as e:
        logger.error(f"Error processing batch {batch_id}: {e}")
        # Đảm bảo file JSON được đóng đúng cách
        try:
            with open(batch_file, 'a') as f:
                f.write('\n]')
        except:
            pass
        return 0

# Cập nhật hàm main để sửa lỗi logger trong on_assign
async def main():
    # Thiết lập logger
    logger = setup_logger()
    logger.info(f"Starting worker at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Lấy hoặc tạo worker ID
    worker_id = get_worker_id()
    logger.info(f"Worker ID: {worker_id}")
    
    # Cấu hình Kafka Consumer
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': KAFKA_AUTO_OFFSET_RESET,
        'max.poll.interval.ms': 600000,
        'session.timeout.ms': 60000,
        'enable.auto.commit': False
    }
    
    # Tạo producer cho gửi thông báo
    producer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'acks': 'all',
        'retries': 3,
        'retry.backoff.ms': 500
    }
    producer = Producer(producer_conf)
    
    # Tạo consumer và subscribe
    consumer = Consumer(consumer_conf)
    
    # Định nghĩa hàm on_assign đồng bộ với logger từ closure
    def on_assign(consumer, partitions):
        nonlocal logger, worker_id, producer
        logger.info(f"Assignment callback triggered with {len(partitions)} partitions")
        
        # Lấy danh sách partition chi tiết
        partition_details = []
        for p in partitions:
            partition_details.append({
                "topic": p.topic,
                "partition": p.partition,
                "offset": p.offset
            })
        
        # Lấy danh sách partition
        assigned_partitions = [p.partition for p in partitions]
        logger.info(f"Partitions assigned: {assigned_partitions}")
        logger.info(f"Detailed partition info: {partition_details}")
        
        # Gửi thông tin partition ngay lập tức
        try:
            # Tạo message kết nối với thông tin partition
            connection_message = {
                "status": "new",
                "id": worker_id,
                "partitions": assigned_partitions,
                # "partition_details": partition_details
            }
            
            logger.info(f"Sending connection message: {connection_message}")
            
            # Gửi lên Kafka sử dụng topic mới
            producer.produce(
                KAFKA_WORKER_CONNECTION_TOPIC,  # Sử dụng topic mới
                key=worker_id.encode('utf-8'),
                value=json.dumps(connection_message).encode('utf-8')
            )
            producer.flush()
            
            logger.info(f"Sent connection message with worker ID: {worker_id}, partitions: {assigned_partitions}")
        except Exception as e:
            logger.error(f"Error sending connection message: {e}")
    
    # Subscribe với callback on_assign đồng bộ
    consumer.subscribe([KAFKA_MASTER_SEND_TOPIC], on_assign=on_assign)
    
    try:
        while True:
            # Poll cho message mới
            msg = consumer.poll(1000)  # Poll mỗi 1 giây
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                else:
                    logger.error(f"Error while consuming: {msg.error()}")
                continue
            
            # Xử lý message
            try:
                value = json.loads(msg.value().decode('utf-8'))
                message_worker_id = value.get('id')
                
                # Chỉ xử lý message dành cho worker này
                if message_worker_id == worker_id:
                    batch_id = value.get('batchId')
                    ip_list = value.get('data', [])
                    
                    if not ip_list:
                        logger.warning(f"Received empty IP list for batch_id: {batch_id}")
                        consumer.commit(msg)
                        continue
                    
                    logger.info(f"Received batch_id: {batch_id} with {len(ip_list)} IPs")
                    
                    # Xử lý batch IP
                    await process_ip_batch(ip_list, batch_id, worker_id, logger)
                else:
                    logger.debug(f"Received message for worker {message_worker_id}, ignoring (my ID: {worker_id})")
                
                # Commit offset sau khi xử lý xong
                consumer.commit(msg)
                
            except json.JSONDecodeError:
                logger.error(f"Failed to decode message: {msg.value()}")
                consumer.commit(msg)  # Commit để không xử lý lại message lỗi
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # Không commit để có thể xử lý lại nếu cần
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
    finally:
        # Đóng consumer
        consumer.close()
        producer.flush()
        logger.info(f"Worker closed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    asyncio.run(main())
