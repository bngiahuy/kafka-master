import kafka from '../configs/kafkaConfig.js';
import 'dotenv/config';
export const getPartitionsService = async () => {
	const admin = kafka.admin();
	await admin.connect().catch((err) => {
		console.error(
			'Error connecting to Kafka while get number of partitions. Error: ',
			err
		);
		return;
	});

	const topics = await admin.listTopics();
	const partitions = await Promise.all(
		topics.map(async (topic) => {
			const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
			return {
				topic,
				partitions: metadata.topics[0].partitions.length,
			};
		})
	);

	await admin.disconnect();
	return partitions;
};

export const updatePartitionsService = async (newPartitionSize) => {
	const admin = kafka.admin();
	await admin.connect().catch((err) => {
		console.error(
			'Error connecting to Kafka while updating partitions. Error: ',
			err
		);
		return;
	});

	await admin
		.createPartitions({
			timeout: 5000,
			topicPartitions: [
				{
					topic: process.env.KAFKA_TOPIC_NAME_MASTER,
					count: newPartitionSize,
				},
			],
		})
		.catch((err) => {
			console.error('Error updating partitions: ', err);
			return;
		});

	const topics = await admin.listTopics();
	const partitions = await Promise.all(
		topics.map(async (topic) => {
			const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
			return {
				topic,
				partitions: metadata.topics[0].partitions.length,
			};
		})
	);
	return partitions;
};
