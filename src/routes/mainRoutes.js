import { Router } from 'express';
import {
	getWorkersStatus,
	updateNumBatches,
	getNumBatches,
	getPartitions,
	updatePartitions,
} from '../controllers/mainController.js';

const mainRouter = Router();

mainRouter.use('/updateNumBatches', updateNumBatches);
mainRouter.use('/updatePartitions', updatePartitions);
mainRouter.use('/getPartitions', getPartitions);
mainRouter.use('/getNumBatches', getNumBatches);
mainRouter.use('/getWorkersStatus', getWorkersStatus);

export default mainRouter;
