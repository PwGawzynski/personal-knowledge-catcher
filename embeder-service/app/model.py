import asyncio
import logging
from typing import List

import torch
from sentence_transformers import SentenceTransformer

from .config import BATCH_SIZE

logger = logging.getLogger(__name__)


class EmbedModel:
    def __init__(self, model_name: str = "BAAI/bge-m3", device: str = None):
        if device is None:
            device = "cuda" if torch.cuda.is_available() else "cpu"

        self.model = SentenceTransformer(model_name, device=device)
        self.dim = self.model.get_sentence_embedding_dimension()
        self.device = device
        self.model_name = model_name

    async def embed(self, texts: List[str]) -> List[List[float]]:
        try:
            logger.info(f"Starting model.encode for {len(texts)} texts on device {self.device}")
            loop = asyncio.get_running_loop()
            vectors = await loop.run_in_executor(
                None,
                lambda: self.model.encode(
                    texts,
                    batch_size=BATCH_SIZE,
                    convert_to_numpy=True,
                    normalize_embeddings=True,
                    device=self.device,
                ),
            )
            logger.info(f"Model.encode completed successfully")
            return vectors.tolist()
        except Exception as e:
            logger.error(f"Error in model.embed: {e}")
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                logger.info("GPU cache cleared after error")
            raise


def log_gpu_info() -> None:
    logger.info(f"Torch info GPU available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        try:
            logger.info(f"Torch info GPU name: {torch.cuda.get_device_name(0)}")
            logger.info(f"Torch info GPU capability: {torch.cuda.get_device_capability(0)}")
            logger.info(f"Torch info GPU properties: {torch.cuda.get_device_properties(0)}")
            logger.info(f"Torch info GPU total memory: {torch.cuda.get_device_properties(0).total_memory}")
        except Exception:
            # Best-effort
            pass


