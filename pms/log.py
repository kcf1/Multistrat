"""
PMS logging (loguru), same pattern as OMS.

Use: from pms.log import logger

Logs to stderr so Docker/service terminal shows output like OMS.
Default level: INFO. Set LOG_LEVEL=DEBUG to show debug logs.
"""

import os
import sys

from loguru import logger

_level = (os.environ.get("LOG_LEVEL") or "INFO").strip().upper()
logger.remove()
logger.add(sys.stderr, level=_level)

__all__ = ["logger"]
