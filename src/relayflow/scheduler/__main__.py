"""python -m relayflow.scheduler"""

from __future__ import annotations

import logging
import sys

from relayflow.config import get_settings
from relayflow.db import make_engine
from relayflow.scheduler.main import Scheduler, install_signal_handlers


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = get_settings()
    engine = make_engine(settings.database_url, pool_size=3)
    scheduler = Scheduler(settings, engine)
    install_signal_handlers(scheduler)
    scheduler.run()
    engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
