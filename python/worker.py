import os
import json
import aiohttp
import asyncio
import logging
import uuid
from datetime import datetime
import pytz
from confluent_kafka import Consumer, Producer, KafkaError
import socket
import re
import demjson3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_MASTER_SEND_TOPIC = os.getenv('KAFKA_MASTER_SEND_TOPIC', 'master-signal')
KAFKA_WORKER_SEND_TOPIC = os.getenv('KAFKA_WORKER_SEND_TOPIC', 'worker-signal')
KAFKA_WORKER_CONNECTION_TOPIC = os.getenv('KAFKA_WORKER_CONNECTION_TOPIC', 'worker-connection')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'ip-scanner-group')
KAFKA_AUTO_OFFSET_RESET = os.getenv('KAFKA_AUTO_OFFSET_RESET', 'latest')

# Processing Configuration
MAX_CONCURRENT_REQUESTS = int(os.getenv('MAX_CONCURRENT_REQUESTS', 10))

# Path Configuration
CLIENT_DATA_PATH = os.getenv('CLIENT_DATA_PATH', '/mnt/samba')
HAS_DATA_PATH = f"{CLIENT_DATA_PATH}/has_data"
NO_DATA_PATH = f"{CLIENT_DATA_PATH}/no_data"
OUTPUT_PATH = f"{CLIENT_DATA_PATH}/output"

# Shodan Configuration
COOKIE = os.getenv('SHODAN_COOKIE', '')

tz = pytz.timezone('Asia/Ho_Chi_Minh')

# Thêm biến global để lưu mapping port-service
PORT_SERVICE_MAPPING = {}

# Thêm hàm để load service mapping
def load_service_mapping():
    global PORT_SERVICE_MAPPING
    try:
        with open('service.json', 'r') as f:
            services = json.load(f)
            PORT_SERVICE_MAPPING = {str(service['port']): service['service'] for service in services}
    except Exception as e:
        logger = logging.getLogger("ip_scanner")
        logger.error(f"Error loading service.json: {e}")
        PORT_SERVICE_MAPPING = {}

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
def merge_ip_data(data, worker_id, hostname):
    if not data:
        return None
        
    # Đảm bảo service mapping đã được load
    if not PORT_SERVICE_MAPPING:
        load_service_mapping()
        
    # Lấy thời gian hiện tại ở múi giờ GMT+7
    current_time = datetime.now(tz).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # Xử lý thông tin ports
    processed_ports = []
    if 'data' in data:
        for port_data in data['data']:
            port = str(port_data.get('port', ''))
            port_info = {
                "cpe": port_data.get('cpe', [None])[0] if port_data.get('cpe') else None,
                "cve": [],
                "port": port,
                "product": port_data.get('product', ''),
                "protocol": port_data.get('transport', ''),
                "service": PORT_SERVICE_MAPPING.get(port, ''),
                "tls": {
                    "issuer": port_data.get('ssl', {}).get('cert', {}).get('issuer', 'Unknown'),
                    "version": port_data.get('ssl', {}).get('cert', {}).get('version', 'Unknown'),
                },
                "version": port_data.get('version', '')
            }
            
            # Xử lý CVE cho port
            if 'vulns' in port_data:
                for cve_id in port_data['vulns'].keys():
                    port_info['cve'].append({
                        "checked": False,
                        "id": cve_id,
                        "validated": False
                    })
            
            processed_ports.append(port_info)
    
    # Tạo đối tượng kết quả
    result = {
        "asn": data.get('asn', 0),
        "city": data.get('city', ''),
        "country": data.get('country_name', ''),
        "device": "Unknown",
        "hostnames": data.get('hostnames', []),
        "ip": data.get('ip_str', ''),
        "latitude": data.get('latitude', 0),
        "longitude": data.get('longitude', 0),
        "org": data.get('org', ''),
        "os": data.get('os', ''),
        "port_list": [str(p) for p in data.get('ports', [])],
        "cve_list": [str(p) for p in data.get('vulns', [])],
        "scan_method": "tool",
        "vm_id": hostname,
        "last_scan": current_time,
        "ports": processed_ports
    }
    
    return result

# Gọi API không đồng bộ
async def fetch_ip_info(ip, session, logger, semaphore):
    async with semaphore:
        try:
            headers = {
                "Accept": "*/*",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
                "Cookie": COOKIE
            }
            
            # Gọi Shodan API
            async with session.get(
                f"https://www.shodan.io/host/{ip}/raw",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                if response.status != 200:
                    logger.error(f"IP {ip}: Shodan API failed with status {response.status}")
                    return False, None, response.status
                
                content = await response.text()
                
                # Tìm và parse dữ liệu JSON
                pattern = r'const DATA = ({[\s\S]*?});'
                match = re.search(pattern, content)
                
                if match:
                    # Lấy phần JSON string
                    data_str = match.group(1)
                    
                    try:
                        # Parse JSON với demjson3
                        data = demjson3.decode(data_str)
                        return True, data, 200
                    except Exception as e:
                        logger.error(f"Error parsing JSON for IP {ip}: {e}")
                        return False, None, 0
                else:
                    logger.error(f"No DATA variable found in HTML for IP {ip}")
                    return False, None, 0
                
        except Exception as e:
            logger.error(f"IP {ip}: Shodan API error: {str(e)}")
            return False, None, 0

# Hàm để lấy tên máy
def get_host_name():
    # Thử lấy từ biến môi trường HOSTNAME (cho Docker)
    docker_hostname = os.environ.get('HOSTNAME')
    if docker_hostname:
        return docker_hostname
    
    # Nếu không có, lấy tên máy host
    try:
        return socket.gethostname()
    except:
        return "unknown_host"

# Xử lý một IP và thu thập kết quả
async def process_ip(ip, session, logger, semaphore, worker_id, hostname):
    success, data, status_code = await fetch_ip_info(ip, session, logger, semaphore)
    
    # Chuẩn bị kết quả
    result = {
        "ip": ip,
        "success": success,
        "error": None if success else f"API failed with status {status_code}",
        "result": None,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    if success:
        merged_data = merge_ip_data(data, worker_id, hostname)
        if merged_data:
            result["result"] = merged_data
        else:
            result["success"] = False
            result["error"] = "Failed to merge data"
    
    return result

# Lấy hoặc tạo worker ID
def get_worker_id():
    if os.path.exists("worker_id.txt"):
        with open("worker_id.txt", 'r') as f:
            worker_id = f.read().strip()
            if worker_id:
                return worker_id
    
    # Tạo ID mới nếu không tìm thấy
    worker_id = str(uuid.uuid4())
    with open("worker_id.txt", 'w') as f:
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
def ensure_directories(hostname, worker_id):
    has_data_file = os.path.join(HAS_DATA_PATH, f"has_data_{hostname}_{worker_id}.txt")
    no_data_file = os.path.join(NO_DATA_PATH, f"no_data_{hostname}_{worker_id}.txt")
    
    if not os.path.exists(has_data_file):
        open(has_data_file, 'w').close()
    
    if not os.path.exists(no_data_file):
        open(no_data_file, 'w').close()
    
    return has_data_file, no_data_file

# Xử lý một batch IP
async def process_ip_batch(ip_list, batch_id, worker_id, logger, hostname):
    # Tạo tên file với hostname và worker_id
    has_data_file = f"{HAS_DATA_PATH}/has_data_{hostname}_{worker_id}.txt"
    no_data_file = f"{NO_DATA_PATH}/no_data_{hostname}_{worker_id}.txt"
    batch_file = f"{OUTPUT_PATH}/{batch_id}.json"
    
    # Tạo file JSON cho batch này
    with open(batch_file, 'w') as f:
        f.write('[\n')
    
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

    try:
        # Giới hạn số lượng yêu cầu đồng thời
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        
        # Xử lý tất cả IP trong batch
        processed_count = 0
        successful_count = 0
        total_count = len(ip_list)
        has_written_result = False
        
        async with aiohttp.ClientSession() as session:
            tasks = [process_ip(ip, session, logger, semaphore, worker_id, hostname) for ip in ip_list]
            
            for future in asyncio.as_completed(tasks):
                result = await future
                processed_count += 1
                
                if result["success"]:
                    successful_count += 1
                    # Lưu vào file codulieu.txt với timestamp và hostname
                    with open(has_data_file, 'a') as f:
                        f.write(f"[{datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}] {result['ip']} (scanned by {hostname})\n")
                    
                    # Lưu vào file JSON của batch
                    with open(batch_file, 'a') as f:
                        if has_written_result:  # Đã có kết quả trước đó
                            f.write(',\n')
                        json.dump(result["result"], f, separators=(',', ':'))
                        has_written_result = True
                else:
                    # Lưu vào file khongcodulieu.txt với timestamp và hostname
                    with open(no_data_file, 'a') as f:
                        f.write(f"[{datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')}] {result['ip']} (scanned by {hostname})\n")
                
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

    # Lấy tên máy
    hostname = get_host_name()

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
                    await process_ip_batch(ip_list, batch_id, worker_id, logger, hostname)
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
