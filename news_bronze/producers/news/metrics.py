 
import time
import csv
from pathlib import Path
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
 
 
class NewsMetrics:
 
    def __init__(self):
        self.counters = defaultdict(int)
        self.start_time = time.time()
 
        project_root = Path(__file__).resolve().parents[2]
        self.metrics_dir = project_root / "shared_data" / "metrics"
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
 
    def increment(self, metric, amount=1):
        self.counters[metric] += amount
 
    @contextmanager
    def track(self, step_name, component="producer"):
        start_time = time.perf_counter()
        start_timestamp = datetime.now(timezone.utc).isoformat()
        status = "success"
 
        print(f"\n[TIMER START] {step_name}")
 
        try:
            yield
        except Exception:
            status = "failed"
            raise
        finally:
            end_time = time.perf_counter()
            end_timestamp = datetime.now(timezone.utc).isoformat()
            duration = end_time - start_time
 
            self.record_timing(
                step_name,
                component,
                start_timestamp,
                end_timestamp,
                duration,
                status
            )
 
            print(
                f"[TIMER END] {step_name} | "
                f"Duration: {duration:.3f}s | "
                f"Status: {status}"
            )
 
    def record_timing(
        self,
        step_name,
        component,
        start_timestamp,
        end_timestamp,
        duration,
        status
    ):
        filename = (
            f"phase1_{component}_timing.csv"
        )
 
        output_file = self.metrics_dir / filename
        file_exists = output_file.exists()
 
        with open(
            output_file,
            "a",
            newline="",
            encoding="utf-8"
        ) as file:
 
            writer = csv.writer(file)
 
            if not file_exists:
                writer.writerow([
                    "run_id",
                    "phase",
                    "component",
                    "step_name",
                    "start_time",
                    "end_time",
                    "duration_seconds",
                    "status"
                ])
 
            writer.writerow([
                datetime.now(timezone.utc).strftime(
                    "%Y%m%dT%H%M%S"
                ),
                "Phase 1",
                component,
                step_name,
                start_timestamp,
                end_timestamp,
                round(duration, 4),
                status
            ])
 
    def record_feed_fetch(self, source, duration, status):
        self.increment("feeds_fetched")
 
        if status == "success":
            self.increment("feeds_success")
        else:
            self.increment("feeds_failed")
 
        print(
            f"[FEED] {source} | "
            f"Status: {status} | "
            f"Duration: {duration:.2f}s"
        )
 
    def report(self):
        print("\n========== FINTEL NEWS METRICS ==========")
 
        for metric, value in sorted(self.counters.items()):
            print(f"{metric}: {value}")
 
        print(f"Uptime: {time.time() - self.start_time:.2f}s")
        print("=========================================\n")
 