import { Router } from 'express';
import {
	getWorkersStatus,
	assignNumBatches,
	getNumBatches,
} from '../controllers/mainController.js';

const mainRouter = Router();

mainRouter.use('/updateNumBatches', assignNumBatches);
mainRouter.use('/getNumBatches', getNumBatches);
mainRouter.use('/getWorkersStatus', getWorkersStatus);
export default mainRouter;
