"""python -m relayflow.worker"""

from __future__ import annotations

import logging
import sys

from relayflow.config import get_settings
from relayflow.db import make_engine
from relayflow.worker.main import Worker, install_signal_handlers


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s"
    )
    settings = get_settings()
    engine = make_engine(settings.database_url, pool_size=settings.worker_concurrency + 2)
    worker = Worker(settings, engine)
    install_signal_handlers(worker)
    worker.run()
    engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
