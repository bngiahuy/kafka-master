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
├── input_ip_data/       # Batch data files for processing
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
# API Server
API_SERVER_PORT=3001

# Kafka Configuration
KAFKA_BROKER_ADDRESS=localhost
KAFKA_BROKER_PORT=9092
KAFKA_TOPIC_NAME_WORKER=worker-signal      # Workers -> Master (results)
KAFKA_TOPIC_NAME_MASTER=master-signal      # Master -> Workers (assignments)
KAFKA_TOPIC_NAME_WORKER_FREE=worker-connection  # Workers -> Master (registration)
```

## API Endpoints

* **GET /api/getWorkersStatus**: View current worker status
* **GET /api/getNumBatches**: Get current batch size setting
* **GET /api/updateNumBatches?numBatches=N**: Update batch size configuration
* **GET /api/getPartitions**: Get Kafka partition information

## Running the System
- Create a folder named `input_ip_data` inside the project folder.
- Create `.env` file and its content.
- Use docker-compose to run the program.
```bash
# Using docker-compose
docker-compose up -d
```
- Wait a few seconds for kafka-workers registration successfully, then you can add .txt files into `input_ip_data` folder.
- You can use some API Endpoints above to monitor.