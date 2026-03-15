import logging
import os
from pathlib import Path

logging.basicConfig(
    format="%(asctime)s %(levelname)s	%(message)s "
    "[%(process)d] %(module)s %(filename)s %(funcName)s",
    level=os.environ.get("LOGLEVEL", "INFO"),
)

_DATALAKE_LOCATION = Path(__file__).parent.parent.parent / "datalake"
DATALAKE_BRONZE_LOCATION = _DATALAKE_LOCATION / "bronze"
DATALAKE_SILVER_LOCATION = _DATALAKE_LOCATION / "silver"
DATALAKE_GOLD_LOCATION = _DATALAKE_LOCATION / "gold"
USER_REPORT = "user_report"
