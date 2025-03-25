import { Router } from 'express';
import { getWorkersStatus, sendSignal } from '../controllers/mainController.js';

const mainRouter = Router();

mainRouter.use('/sendToWorkers', sendSignal);
mainRouter.use('/getWorkersStatus', getWorkersStatus);
export default mainRouter;
