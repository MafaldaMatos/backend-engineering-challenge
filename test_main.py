
import json
import sys
from pathlib import Path
from typing import List, Optional

import pytest

import main as main


def run_cli(tmp_path: Path, lines: List[str], *, window_size: int = 10, extra_args: List[str] | None = None):
    """
    Helper to run main.main() with a temp input file and capture stdout/stderr.
    """
    input_file = tmp_path / "events.jsonl"
    input_file.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

    argv = ["prog", "--input_file", str(input_file), "--window_size", str(window_size)]
    if extra_args:
        argv.extend(extra_args)

    old_argv = sys.argv
    sys.argv = argv
    try:
        main.main()
    finally:
        sys.argv = old_argv


def parse_stdout(capsys):
    out = capsys.readouterr().out.strip()
    if not out:
        return []
    return [json.loads(line) for line in out.splitlines()]


def test_sample_input_matches_expected_output(tmp_path, capsys):
    # Sample from the challenge
    lines = [
        '{"timestamp":"2018-12-26 18:11:08.509654","translation_id":"5aa5b2f39f7254a75aa5","source_language":"en","target_language":"fr","client_name":"airliberty","event_name":"translation_delivered","nr_words":30,"duration":20}',
        '{"timestamp":"2018-12-26 18:15:19.903159","translation_id":"5aa5b2f39f7254a75aa4","source_language":"en","target_language":"fr","client_name":"airliberty","event_name":"translation_delivered","nr_words":30,"duration":31}',
        '{"timestamp":"2018-12-26 18:23:19.903159","translation_id":"5aa5b2f39f7254a75bb3","source_language":"en","target_language":"fr","client_name":"taxi-eats","event_name":"translation_delivered","nr_words":100,"duration":54}',
    ]

    run_cli(tmp_path, lines, window_size=10)

    got = parse_stdout(capsys)

    expected = [
        {"date": "2018-12-26 18:11:00", "average_delivery_time": 0},
        {"date": "2018-12-26 18:12:00", "average_delivery_time": 20},
        {"date": "2018-12-26 18:13:00", "average_delivery_time": 20},
        {"date": "2018-12-26 18:14:00", "average_delivery_time": 20},
        {"date": "2018-12-26 18:15:00", "average_delivery_time": 20},
        {"date": "2018-12-26 18:16:00", "average_delivery_time": 25.5},
        {"date": "2018-12-26 18:17:00", "average_delivery_time": 25.5},
        {"date": "2018-12-26 18:18:00", "average_delivery_time": 25.5},
        {"date": "2018-12-26 18:19:00", "average_delivery_time": 25.5},
        {"date": "2018-12-26 18:20:00", "average_delivery_time": 25.5},
        {"date": "2018-12-26 18:21:00", "average_delivery_time": 25.5},
        {"date": "2018-12-26 18:22:00", "average_delivery_time": 31},
        {"date": "2018-12-26 18:23:00", "average_delivery_time": 31},
        {"date": "2018-12-26 18:24:00", "average_delivery_time": 42.5},
    ]

    assert got == expected


def test_boundary_timestamp_counts_same_minute(tmp_path, capsys):
    # Edge case: exact minute boundary should count for the SAME minute in this solution.
    lines = [
        '{"timestamp":"2018-12-26 12:00:00.000000","translation_id":"x","source_language":"en","target_language":"fr","client_name":"c","event_name":"translation_delivered","nr_words":1,"duration":10}'
    ]
    run_cli(tmp_path, lines, window_size=10)

    got = parse_stdout(capsys)
    # Output starts at 12:00:00. With the boundary rule, the event contributes immediately.
    assert got == [
        {"date": "2018-12-26 12:00:00", "average_delivery_time": 10}
    ]


def test_out_of_order_timestamps_fail_fast(tmp_path):
    lines = [
        '{"timestamp":"2018-12-26 12:00:01.000000","duration":1,"event_name":"translation_delivered"}',
        '{"timestamp":"2018-12-26 12:00:00.000000","duration":1,"event_name":"translation_delivered"}',
    ]
    input_file = tmp_path / "events.jsonl"
    input_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    it = main.iter_minute_buckets(
        str(input_file),
        strict_event_name=False,
        skip_invalid=False,
        enforce_order=True,
    )
    with pytest.raises(ValueError, match="timestamp out of order"):
        list(it)


def test_malformed_json_line_raises(tmp_path):
    lines = [
        '{"timestamp":"2018-12-26 12:00:00.000000","duration":1,"event_name":"translation_delivered"}',
        '{not-json}',
    ]
    input_file = tmp_path / "events.jsonl"
    input_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    it = main.iter_minute_buckets(
        str(input_file),
        strict_event_name=False,
        skip_invalid=False,
        enforce_order=True,
    )
    with pytest.raises(ValueError, match="invalid JSON"):
        list(it)


def test_skip_invalid_continues(tmp_path, capsys):
    lines = [
        '{"timestamp":"2018-12-26 12:00:00.000000","duration":10,"event_name":"translation_delivered"}',
        '{not-json}',
        '{"timestamp":"2018-12-26 12:00:30.000000","duration":20,"event_name":"translation_delivered"}',
    ]
    input_file = tmp_path / "events.jsonl"
    input_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    buckets = main.iter_minute_buckets(
        str(input_file),
        strict_event_name=False,
        skip_invalid=True,
        enforce_order=True,
    )
    got = list(buckets)
    assert len(got) >= 1
    err = capsys.readouterr().err
    assert "invalid JSON" in err


def test_wrong_structure_not_object(tmp_path):
    lines = [
        '["not","an","object"]',
    ]
    input_file = tmp_path / "events.jsonl"
    input_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    it = main.iter_minute_buckets(
        str(input_file),
        strict_event_name=False,
        skip_invalid=False,
        enforce_order=True,
    )
    with pytest.raises(ValueError, match="must be an object"):
        list(it)
