import logging
from .config import LOGGING_LEVEL

def setup_logger(name: str) -> logging.Logger:

    logging.getLogger('neo4j').setLevel(logging.ERROR)

    # Formatters
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Handlers
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler('pipeline.log', mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)

    # Logger
    logger = logging.getLogger(name=name)
    logger.setLevel(LOGGING_LEVEL)
    logger.propagate = False
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
