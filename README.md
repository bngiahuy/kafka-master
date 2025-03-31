# Kafka Master
## Architecture Overview

* **Master Node**: Coordinates job distribution and monitors worker status
* **Worker Nodes**: Process assigned data batches and report completion
* **Kafka**: Message broker for communication between components
* **Redis**: Stores worker status, job assignments, and system state

## Project Structure

```
kafka-master/
├── src/
│   ├── configs/         # Configuration for Kafka, Redis and services
│   ├── consumers/       # Kafka message consumers
│   ├── controllers/     # API endpoint handlers
│   ├── producers/       # Kafka message producers
│   ├── routes/          # Express API routes
│   ├── services/        # Business logic services
│   ├── utils/           # Utility functions and helpers
│   └── index.js         # Application entry point
└── package.json         # Project dependencies
```

## System Flow

1. **Worker Registration**:
   * Workers connect to `KAFKA_TOPIC_NAME_WORKER_FREE` with their workerId and partition assignment
   * Master stores worker information in Redis

2. **Batch Processing**:
   * Master reads files from `input_ip_data/` directory
   * Splits files into manageable chunks based on configured size
   * Assigns chunks to available workers via `KAFKA_TOPIC_NAME_MASTER` topic

3. **Job Execution & Monitoring**:
   * Workers process assigned data and report progress to `KAFKA_TOPIC_NAME_WORKER`
   * Master tracks worker status and handles timeouts/failures
   * Completed batches are marked as processed

## Configuration

The system uses environment variables for configuration:

```
KAFKA_BROKER_ADDRESS= # The IP address of the Kafka broker, necessary for connecting to Kafka
KAFKA_BROKER_PORT= # The port on which the Kafka broker is listening, typically 9092
API_SERVER_PORT= # The port that the API server will use to listen for requests
MASTER_ID= # A unique identifier for the master node in the system
KAFKA_GROUP_ID= # The ID of the Kafka group, used to manage consumers within the same group
BATCH_SIZE= # The number of data items in each batch that a worker will process
WORKER_TIMEOUT= # The maximum time (in seconds) a worker can be unresponsive before being considered failed
SMB_HOST= # The IP address of the SMB server, used for connecting and sharing data
SMB_USER= # The username for authentication when connecting to the SMB server
SMB_PASSWORD= # The password for authentication when connecting to the SMB server
SMB_PORT= # The port that the SMB server is using
CLIENT_DATA_PATH= # The path to the client's data directory, where data to be processed is stored
KAFKA_MASTER_HEARTBEAT_TOPIC= # The name of the Kafka topic used for sending heartbeat signals from the master
KAFKA_TOPIC_NAME_WORKER= # The name of the Kafka topic that workers use to send signals
KAFKA_TOPIC_NAME_MASTER= # The name of the Kafka topic that the master uses to send signals
KAFKA_TOPIC_NAME_WORKER_FREE= # The name of the Kafka topic that workers use to announce their availability
```

## API Endpoints

* **GET /api/getWorkersStatus**: View current worker status
* **GET /api/getNumBatches**: Get current batch size setting
* **GET /api/updateNumBatches?numBatches=<integer>**: Update batch size configuration
* **GET /api/getPartitions**: Get Kafka partition information
* **GET /api/updatePartitions?value=<integer>**: Update Kafka partition number (supports only master-signal topic).

## Running the System
- Create `.env` file and its content.
- Use docker-compose to run the program.
```bash
# Using docker-compose
docker-compose build && docker-compose up -d
```
- You can use some API Endpoints above to monitor.

Sure, here is the full Redis Key Reference translated into English:

---

# Redis Keys Reference

Below is the list of Redis keys used in the system:

## Master Management & Leader Election

| Key | Type | Description |
|-----|------|-------------|
| `masters:list` | Sorted Set | List of registered masters, with score as the join timestamp |
| `master:heartbeat:{masterId}` | String | Latest heartbeat timestamp of the master |
| `master:active:leader` | String | ID of the current leader master |
| `master:election:running` | String | Flag indicating an ongoing election process |
| `master:fileChunks` | List | List of chunks waiting to be processed |

## Worker Management

| Key | Type | Description |
|-----|------|-------------|
| `worker:status` | Hash | Status of workers (1 = ready, 0 = busy) |
| `worker:partition` | Hash | Partition information assigned to each worker |
| `worker:batchInfo` | Hash | Batch information currently being processed by each worker |
| `worker:processing` | Hash | Current processing progress for each batch |
| `lastSeen:{workerId}` | String | Last seen timestamp of the worker |

## File & Batch Processing

| Key | Type | Description |
|-----|------|-------------|
| `processed:files` | Set | List of files that have been fully processed |
| `numBatches` | String | Default chunk size (number of items per batch) |

## Useful Redis Commands

```bash
# Check list of registered masters
redis-cli ZRANGE masters:list 0 -1

# Check current leader
redis-cli GET master:active:leader

# Check status of all workers
redis-cli HGETALL worker:status

# Check progress of all batches
redis-cli HGETALL worker:processing

# Check list of processed files
redis-cli SMEMBERS processed:files

# Check number of chunks waiting to be processed
redis-cli LLEN master:fileChunks

# View detailed info for a specific worker
redis-cli HGET worker:batchInfo "your-worker-id"

# Check when a worker was last active
redis-cli GET lastSeen:your-worker-id
```

## Redis Keys Related to Files and Chunks

### Completed files:
```bash
redis-cli SMEMBERS processed:files
```

### View chunks waiting to be processed:
```bash
redis-cli LRANGE master:fileChunks 0 -1
```

### View processing progress of all batches:
```bash
redis-cli HGETALL worker:processing
```

### Check which workers are handling which batches:
```bash
redis-cli HGETALL worker:batchInfo
```
