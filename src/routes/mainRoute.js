import { Router } from 'express';
import { sendSignal } from '../controllers/mainController.js';

const mainRouter = Router();

mainRouter.use('/sendToWorkers', sendSignal);

export default mainRouter;
