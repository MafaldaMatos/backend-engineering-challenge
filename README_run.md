# Transperfect Backend Engineer Challenge

## How to run

Using Python 3.X, run the command:

```
python3 main.py --input_file [events.json] --window_size [10] --strict_event_name --skip_invalid --enforce_order
```

- input_file - "events.json" is the JSON Lines input file;
- window_size - "10" is the window_size desired in minutes;
- [OPTIONAL] strict_event_name - is a flag. When added to the command, it requires event_name == 'translation_delivered' (default: off);
- [OPTIONAL] skip_invalid - is a flag. When added to the command, it skips invalid lines instead of failing (errors go to stderr) (default: off);
- [OPTIONAL] enforce_order - is a flag. When added to the command, it checks that event timestamps are in order (non-decreasing) (default: off).

## Considerations
- events.json is in a JSON Lines format. Events are already ordered by timestamp which allows us to stream the JSON Lines directly.
- Because of this format, we want to use a FIFO data structure and in Python the most efficient is deque.
- In the code, we define effective minutes. These are the minutes that the timestamps will be rounded to, calculated based on the logic of edge cases (see below).

### Edge cases
We consider the intervals as open on the lower boundary and closed on the higher boundary. For example, for the interval (18:00:00, 18:01:00), we end up getting ]18:00:00, 18:01:00], or:

- 17:59:59.9999 -> 18:00:00
- 18:00:00.0000 -> 18:00:00
- 18:00:00.0001 -> 18:01:00
- 18:01:00.0000 -> 18:01:00
- 18:01:00.0001 -> 18:02:00

This ensures that we don't lose any events (and also don't count the same event in 2 different minutes), but it also ensures that all events are only added to the moving avg AFTER they have actually occured.

## Tests

The tests are located in `test_main.py` and they represent 3 different categories:
- Edge case - as described in the previous section;
- Challenge input example case - makes sure the input-output example of the challenge is matched;
- Event errors - tests the event.json file for out of order timestamps and incorrect structure.

In order to run the tests, run the following commands:
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pytest
```