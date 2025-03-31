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