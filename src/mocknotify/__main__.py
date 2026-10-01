"""python -m mocknotify"""

from __future__ import annotations

import os

import uvicorn

from mocknotify.app import create_app


def main() -> None:
    uvicorn.run(
        create_app(),
        host=os.environ.get("MOCKNOTIFY_HOST", "127.0.0.1"),
        port=int(os.environ.get("MOCKNOTIFY_PORT", "8100")),
        log_level="info",
    )


if __name__ == "__main__":
    main()
