#!/usr/bin/env python3

from pathlib import Path
import runpy


if __name__ == "__main__":
    runpy.run_path(
        Path(__file__).resolve().parent / "scripts" / "kafka_producer.py",
        run_name="__main__",
    )
