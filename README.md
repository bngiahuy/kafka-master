# HSE Kafka Master

A Node.js application that implements a Kafka producer/consumer pattern for distributed message processing.

## Project Structure

```
hse-kafka-master/
├── src/
│   ├── configs/         # Kafka configuration
│   ├── consumers/       # Kafka consumer implementations
│   ├── controllers/     # HTTP endpoint controllers
│   ├── producers/       # Kafka producer implementations
│   ├── routes/          # Express routes
│   ├── services/        # Business logic services
│   ├── utils/          # Utility functions
│   └── index.js        # Application entry point
├── prisma/
│   └── schema.prisma   # Database schema
└── package.json
```

## Prerequisites

- Node.js (version 20 or higher)
- npm or yarn
- Apache Kafka server running
- PostgreSQL database (in development)

## Environment Setup

Create a `.env` file in the root directory with the following variables:

```
# API Server
API_SERVER_PORT=3001

# Kafka Configuration
KAFKA_BROKER_ADDRESS=localhost
KAFKA_BROKER_PORT=9092
KAFKA_TOPIC_NAME_WORKER=work-signal
KAFKA_TOPIC_NAME_MASTER=master-signal

# Database
DATABASE_URL="postgresql://user:password@localhost:5432/dbname"
```

## Installation

Install project dependencies:

```sh
npm install
```

Initialize the database:

```sh
npx prisma generate
```

## Running the Application

Start the application in development mode:

```sh
npm run dev
```

The server will start on the configured port (default: 3001).

## API Endpoints

### Send Signal to Workers
- **URL**: `/api/sendToWorkers`
- **Method**: GET
- **Description**: Sends a signal message to all connected Kafka workers

## Application Flow

1. The application starts an Express server and initializes a Kafka consumer.
2. The consumer listens for messages on the `work-signal` topic from workers.
3. When the `/api/sendToWorkers` endpoint is called, the producer sends a message to the `master-signal` topic.
4. Messages received from workers are logged to the console.

## Project Dependencies

- `express` - Web framework for handling HTTP requests
- `kafkajs` - Kafka client for Node.js
- `dotenv` - Environment variable management
- `prisma` - Database ORM

## Scripts

- `npm run dev` - Start the application in development mode
- `npm start` - Start the application in production mode

For more information about the Kafka configuration and message handling, see the `kafkaConfig.js` and `kafkaConsumer.js` files.

## Commands to create topics
```
./opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic master-signal --partitions 10 --replication-factor 1

./opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic worker-signal --partitions 1 --replication-factor 1

./opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic worker-connection --partitions 1 --replicatifactor 1

./opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```