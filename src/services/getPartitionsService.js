import kafka from '../configs/kafkaConfig';
const getPartitionsService = async () => {
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

export default getPartitionsService;
