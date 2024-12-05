import threading
import logging
from typing import Any, Dict, Tuple

# Configure the logger for KeyValueStore
logger = logging.getLogger("KeyValueStore")
logger.setLevel(logging.DEBUG)

# Create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Create formatter and add it to the handlers
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s"
)
ch.setFormatter(formatter)

# Add the handlers to the logger
if not logger.handlers:
    logger.addHandler(ch)


class KeyValueStore:
    def __init__(self):
        self.store: Dict[str, Tuple[Any, int]] = {}
        self.lock = threading.Lock()
        logger.info("Initialized KeyValueStore.")

    def read(self, key: str) -> Tuple[Any, int]:
        with self.lock:
            value = self.store.get(key)
            if value:
                logger.debug(f"Read key '{key}': value={value[0]}, version={value[1]}")
            else:
                logger.debug(f"Read key '{key}': not found.")
            return value

    def write(self, key: str, value: Any, version: int):
        with self.lock:
            self.store[key] = (value, version)
            logger.debug(f"Wrote key '{key}': value={value}, version={version}")
