import argparse
import json
import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Deque, Iterator, Optional, Tuple

TS_FMT = "%Y-%m-%d %H:%M:%S.%f"


def floor_to_minute(dt: datetime) -> datetime:
    """
    Simple flooring function.
    """
    return dt.replace(second=0, microsecond=0)


def effective_minute(dt: datetime) -> datetime:
    """
    Bucketing rule:
      - If dt is exactly on a minute boundary (..:..:00.000000), count it in the SAME minute.
      - Otherwise, count it starting from the NEXT minute. This is to account events only AFTER they have actually occured.

    Examples:
      18:11:08.509654 -> 18:12:00
      18:00:00.000000 -> 18:00:00
    """
    floored = floor_to_minute(dt)
    if dt == floored:
        return floored
    return floored + timedelta(minutes=1)


def parse_and_validate_event(
    line: str,
    line_no: int,
    *,
    strict_event_name: bool,
) -> Tuple[datetime, float]:
    """
    Parse a JSONL line and validate required fields.
    Returns: (timestamp_datetime, duration_float)
    Raises: ValueError with a helpful message if invalid.
    """
    try:
        obj = json.loads(line)
    except json.JSONDecodeError as e:
        raise ValueError(f"Line {line_no}: invalid JSON ({e})")

    if not isinstance(obj, dict):
        raise ValueError(f"Line {line_no}: JSON value must be an object")

    # Required fields for this challenge
    if "timestamp" not in obj:
        raise ValueError(f"Line {line_no}: missing 'timestamp'")
    if "duration" not in obj:
        raise ValueError(f"Line {line_no}: missing 'duration'")

    ts_raw = obj["timestamp"]
    if not isinstance(ts_raw, str):
        raise ValueError(f"Line {line_no}: 'timestamp' must be a string")

    try:
        ts = datetime.strptime(ts_raw, TS_FMT)
    except ValueError:
        raise ValueError(f"Line {line_no}: 'timestamp' does not match format {TS_FMT!r}")

    try:
        dur = float(obj["duration"])
    except (TypeError, ValueError):
        raise ValueError(f"Line {line_no}: 'duration' must be numeric")

    if dur < 0:
        raise ValueError(f"Line {line_no}: 'duration' must be >= 0")

    if strict_event_name:
        if obj.get("event_name") != "translation_delivered":
            raise ValueError(
                f"Line {line_no}: expected event_name 'translation_delivered', got {obj.get('event_name')!r}"
            )

    return ts, dur


def iter_minute_buckets(
    path: str,
    *,
    strict_event_name: bool,
    skip_invalid: bool,
    enforce_order: bool,
) -> Iterator[Tuple[datetime, float, int, datetime]]:
    """
    Stream JSONL events and yield per-minute buckets:
      (bucket_minute, sum_duration, count, start_output_minute)

    start_output_minute is the floored minute of the FIRST raw event timestamp. This helps us
    produce the initial "0" minute like the example output.

    This function aggregates different occurrences in a single minute under the same bucket. Example:
        If three events land in 18:16:00 with durations 10, 20, 30 -> (18:16:00, sum=60, count=3, start_output_minute)
    """
    with open(path, "r", encoding="utf-8") as f:
        start_out: Optional[datetime] = None

        cur_min: Optional[datetime] = None
        cur_sum = 0.0
        cur_cnt = 0

        prev_ts: Optional[datetime] = None

        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                ts, dur = parse_and_validate_event(
                    line,
                    line_no,
                    strict_event_name=strict_event_name,
                )
            except ValueError as e:
                if skip_invalid:
                    print(str(e), file=sys.stderr)
                    continue
                raise

            # Optional check: input should be ordered by timestamp (challenge assumption)
            if enforce_order and prev_ts is not None and ts < prev_ts:
                msg = f"Line {line_no}: timestamp out of order (got {ts}, prev {prev_ts})"
                if skip_invalid:
                    print(msg, file=sys.stderr)
                    continue
                raise ValueError(msg)
            prev_ts = ts

            if start_out is None:
                start_out = floor_to_minute(ts)

            m = effective_minute(ts)

            if cur_min is None:
                cur_min = m

            if m != cur_min:
                yield cur_min, cur_sum, cur_cnt, start_out
                cur_min, cur_sum, cur_cnt = m, 0.0, 0

            cur_sum += dur
            cur_cnt += 1

        if cur_min is not None and start_out is not None:
            yield cur_min, cur_sum, cur_cnt, start_out


@dataclass
class MovingAverageWindow:
    """
    Maintains a moving window of minute-buckets and keeps running totals.
    Window holds tuples: (minute_rounded, sum_duration_in_minute, count_in_minute)
    """
    window_size: int
    window: Deque[Tuple[datetime, float, int]] = field(default_factory=deque)
    w_sum: float = 0.0
    w_cnt: int = 0

    def expire_old(self, now_minute: datetime) -> None:
        """
        Empty old bucket
        """
        cutoff = now_minute - timedelta(minutes=self.window_size - 1)
        while self.window and self.window[0][0] < cutoff:
            _, s, c = self.window.popleft()
            self.w_sum -= s
            self.w_cnt -= c

    def add_bucket(self, minute: datetime, sum_duration: float, count: int) -> None:
        """
        Add new bucket
        """
        self.window.append((minute, sum_duration, count))
        self.w_sum += sum_duration
        self.w_cnt += count

    def average(self) -> float:
        """
        Calculates the moving avg for the current window
        """
        return (self.w_sum / self.w_cnt) if self.w_cnt else 0.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_file", required=True)
    ap.add_argument("--window_size", required=True, type=int)

    # Validation / robustness switches
    ap.add_argument(
        "--strict_event_name",
        action="store_true",
        help="Require event_name == 'translation_delivered' (default: off)",
        default=False
    )
    ap.add_argument(
        "--skip_invalid",
        action="store_true",
        help="Skip invalid lines instead of failing (errors go to stderr) (default: off)",
        default=False
    )
    ap.add_argument(
        "--enforce_order",
        action="store_true",
        help="Check that timestamps are non-decreasing (default: off)"
    )
    args = ap.parse_args()

    if args.window_size <= 0:
        raise SystemExit("--window_size must be >= 1")

    # Initialize buckets iterator and variables necessary
    buckets = iter_minute_buckets(
        args.input_file,
        strict_event_name=args.strict_event_name,
        skip_invalid=args.skip_invalid,
        enforce_order=args.enforce_order,
    )

    # Window state lives in a small class (no nonlocal needed)
    ma = MovingAverageWindow(window_size=args.window_size)

    next_bucket = next(buckets, None)
    # If for some reason there are no events, we end right away
    if next_bucket is None:
        return

    # Start the iterator with the first element, including the starting time (start_output_min)
    next_minute, next_sum, next_cnt, start_output_min = next_bucket
    current_out = start_output_min

    def emit(now_minute: datetime) -> None:
        # Calculates and prints the moving avg for the minute
        avg = ma.average()
        print(
            json.dumps(
                {
                    "date": now_minute.strftime("%Y-%m-%d %H:%M:%S"),
                    "average_delivery_time": avg,
                }
            )
        )

    last_bucket_minute = next_minute

    while True:
        # Loop logic = expire -> add -> emit
        ma.expire_old(current_out)

        if next_minute is not None and current_out == next_minute:
            ma.add_bucket(next_minute, next_sum, next_cnt)

            next_bucket = next(buckets, None)
            if next_bucket is not None:
                next_minute, next_sum, next_cnt, _ = next_bucket
            else:
                next_minute = None

        emit(current_out)

        if ma.window:
            last_bucket_minute = ma.window[-1][0]

        # If we have no new buckets left, we exit loop
        if next_minute is None and current_out >= last_bucket_minute:
            break

        current_out += timedelta(minutes=1)

        # Safety guard if input is unexpectedly out-of-order
        if next_minute is not None and current_out > next_minute:
            current_out = next_minute


if __name__ == "__main__":
    main()
