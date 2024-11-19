from datetime import datetime as dt

import pandas as pd
import pytz
import requests
from icalendar import Calendar


def import_calendar(calendar):
    if calendar.startswith("http"):
        try:
            response = requests.get(calendar)
            calendar_data = response.text
        except requests.exceptions.RequestException:
            raise ValueError("Cannot import calendar from invalid URL")
    else:
        try:
            with open(calendar, "rb") as f:
                calendar_data = f.read()
        except FileNotFoundError:
            raise FileNotFoundError("Invalid file path to the iCal file.")
    cal = Calendar.from_ical(calendar_data)

    return cal


def calculate_list_subset(repeating_list):
    for i in range(1, len(repeating_list) // 2 + 1):
        subset = repeating_list[:i]
        repeat_count = len(repeating_list) // i
        remainder = repeating_list[: len(repeating_list) % i]
        if subset * repeat_count + remainder == repeating_list:
            return subset

    return repeating_list


def get_rotation_turn_length_seconds(rrule):
    frequency = rrule.get("freq")[0]
    if frequency == "WEEKLY":
        frequency = 604800
    elif frequency == "DAILY":
        frequency = 86400
    else:
        frequency = None

    return frequency


def get_timezone(calendar):
    timezone = calendar["X-WR-TIMEZONE"]
    timezone = pytz.timezone(timezone)

    return timezone


def get_rotation_virtual_start(timezone, start_dt):
    rotation_virtual_start = dt.now(timezone).replace(
        hour=start_dt.hour,
        minute=start_dt.minute,
        second=start_dt.second,
        microsecond=start_dt.microsecond,
    )

    return rotation_virtual_start


def create_event(start_dt, end_dt, rrule, user_id, timezone):
    # create an event object
    event_data = {
        "user": user_id,
        "start": start_dt,
        "end_dt": end_dt,
        "start_time": start_dt.time(),
        "end_time": end_dt.time(),
        "rrule": rrule,
        "end": rrule.get("until", "null"),
        "duration_seconds": (end_dt - start_dt).total_seconds(),
        "rotation_turn_length_seconds": get_rotation_turn_length_seconds(rrule),
        "day_of_week_start": start_dt.isoweekday(),
        "rotation_virtual_start": get_rotation_virtual_start(timezone, start_dt),
    }

    return event_data


def create_calendar_df(calendar):
    # Prepare a list to hold event data
    events = []

    timezone = get_timezone(calendar)

    for event in calendar.walk("vevent"):
        # extract event start and end
        start_dt = event.get("dtstart").dt
        start_dt = start_dt.astimezone(timezone)
        end_dt = event.get("dtend").dt
        end_dt = end_dt.astimezone(timezone)

        # extract pd_user_id from the event
        user_id = str(event.get("summary"))

        if event.get("rrule"):
            # add recurring events to the dataframe
            rrule = event.get("rrule")

        event_data = create_event(start_dt, end_dt, rrule, user_id, timezone)
        events.append(event_data)

    df = pd.DataFrame(events)
    df = df.sort_values(by=["start"])

    return df
