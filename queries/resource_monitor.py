from __future__ import annotations

import threading
import time
from pathlib import Path

import psutil


class ResourceMonitor:
    def __init__(self, interval: float = 0.2) -> None:
        self._interval = interval
        self._samples: list[tuple[float, float, float, float]] = []
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._start_time: float = 0.0
        self._last_bytes_recv: int = 0
        self._last_sample_time: float = 0.0

    def start(self) -> None:
        self._samples = []
        self._stop_event.clear()
        psutil.cpu_percent()  # prime the counter; first call always returns 0.0
        net = psutil.net_io_counters()
        self._last_bytes_recv = net.bytes_recv
        self._start_time = time.monotonic()
        self._last_sample_time = self._start_time
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
        self._record()

    def _record(self) -> None:
        now = time.monotonic()
        elapsed = now - self._start_time
        dt = now - self._last_sample_time

        net = psutil.net_io_counters()
        bytes_recv = net.bytes_recv
        download_mbps = (bytes_recv - self._last_bytes_recv) / dt / 1024**2 if dt > 0 else 0.0
        self._last_bytes_recv = bytes_recv
        self._last_sample_time = now

        self._samples.append((
            round(elapsed, 3),
            round(psutil.cpu_percent(), 1),
            round(download_mbps, 3),
            round(psutil.virtual_memory().percent, 1),
        ))

    def _run(self) -> None:
        while not self._stop_event.wait(self._interval):
            self._record()

    def write_csv(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            f.write("elapsed_s,cpu_percent,download_mbps,memory_percent\n")
            for sample in self._samples:
                f.write(",".join(str(v) for v in sample) + "\n")
