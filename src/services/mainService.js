import KafkaManager from '../configs/kafkaConfig.js';
import State from '../state.js';

const kafkaManager = new KafkaManager();
const state = new State();

export const getPartitionsService = async () => {
	return kafkaManager.getPartitions();
};

export const updatePartitionsService = async (value) => {
	return kafkaManager.increaseMasterPartition(value);
};

export const getWorkersStatusService = async () => {
	return state.getAllWorkersStatus();
};
