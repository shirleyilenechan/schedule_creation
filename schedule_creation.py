import requests
import pandas as pd
from icalendar import Calendar
from datetime import datetime as dt, timedelta, timezone
from dateutil.rrule import rrule, rrulestr
import numpy as np
import pytz


def import_calendar(calendar):
    if calendar.startswith('http'):
        # import from an ical URL
        try: 
            response = requests.get(calendar)
            calendar_data = response.text
        except requests.exceptions.RequestException:
            raise ValueError("Cannot import calendar from invalid URL")
    else:
        # import from an ical file
        try:
            with open(calendar, 'rb') as f:
                calendar_data = f.read()
        except FileNotFoundError:
            raise FileNotFoundError("Invalid file path to the iCal file.")
        
    cal = Calendar.from_ical(calendar_data)
    
    return cal


def create_event_object(start_dt, end_dt, user_data, recurring, byday=None):
    duration = (end_dt - start_dt).total_seconds()
    day_of_week_start = start_dt.isoweekday()
    week_number = start_dt.isocalendar()[1]
    
    # create an event object
    event_data = {
        'user': user_data,
        'start_dt': start_dt.astimezone(pytz.utc),
        'end_dt': end_dt.astimezone(pytz.utc),
        'start_time': start_dt.time(),
        'end_time': end_dt.time(),
        'duration': duration,
        'dow_start': day_of_week_start,
        'week': week_number,
        'recurring': recurring,
        'byday': byday
    }

    return event_data

def get_byday_list(rrule):
    byday = rrule.get('byday')
    # Convert byday to a list if it's not None; otherwise, use an empty list
    byday_list = list(byday) if byday else []

    # Sort byday only if it's not empty
    if byday_list:
        byday = sorted(byday_list)
    else:
        byday = []
    
    # Convert byday to a tuple, which is hashable and can be used for grouping
    return tuple(byday)


def get_until(rrule,current_dt):
    until = rrule.get('until')

    # when creating the dataframe, limit the data to Now+365 days
    max_timeframe_to_check = current_dt + timedelta(days=365)

    # set a default until date if None is provided
    if until is None:
        until = max_timeframe_to_check
    else:
        # extract until from the rrule
        until = until[0].astimezone(start_dt.tzinfo)
        # limit until to the max timeframe
        if until > max_timeframe_to_check:
            until = max_timeframe_to_check
    
    return until


def extract_recurring_events(start_dt, end_dt, user_data, recurring, rrule):
    current_dt = dt.now(start_dt.tzinfo)
    duration = (end_dt - start_dt)
    until = get_until(rrule, current_dt)
    
    sorted_byday = get_byday_list(rrule)

    # Create an rrule string from the vrecur
    rrule_string = rrule.to_ical().decode('utf-8')
    recurring_events = []

    # Use the recurrence information in the rrulestr to create a list of start_dt
    rule = rrulestr(rrule_string, dtstart=start_dt)
    start_dt_list = rule.between(current_dt, until, inc=True)
    
    # add recurring events to the recurring events list
    for start in start_dt_list:
        event_end = start + duration
        event = create_event_object(start, event_end, user_data, recurring, sorted_byday)
        recurring_events.append(event)

    return recurring_events


def create_calendar_df(calendar):
    # Prepare a list to hold event data
    events_data = []

    for event in calendar.walk('vevent'):
        # extract event start and end
        start_dt = event.get('dtstart').dt
        end_dt = event.get('dtend').dt

        # extract pd_user_id from the event
        user_id = str(event.get('summary'))
        
        recurring = 'rrule' in event

        if recurring:
            # add recurring events to the dataframe
            rrule = event.get('rrule')
            recurring_events = extract_recurring_events(start_dt, end_dt, user_id, True, rrule)
            events_data.extend(recurring_events)
        else:
            # otherwise, just append the event
            event_object = create_event_object(start_dt, end_dt, user_id, False)
            events_data.append(event_object)

        df = pd.DataFrame(events_data)
    
    return df


def calculate_rotation_subset(user_list):
    for i in range(1, len(user_list) // 2 + 1):
        subset = user_list[:i]
        repeat_count = (len(user_list) // i)
        remainder = user_list[:len(user_list) % i]
        if subset * repeat_count + remainder == user_list:
            return subset
    
    return user_list


def get_rotation_pattern(row):
    if row['shift_handover_frequency'] == 'weekly_restriction':
        # Create a DataFrame from the week and user lists
        df = pd.DataFrame({
            'week': row['week'],
            'user': row['user']
        })
        # Drop duplicate 'week' entries to ensure one user per week
        df = df.drop_duplicates(subset='week')
        # The rotation pattern is the sequence of users by week
        user_list = df['user'].tolist()
        rotation_pattern = calculate_rotation_subset(user_list)
    elif row['shift_handover_frequency'] == 'daily_restriction':
        user_list = row['user']
        rotation_pattern = calculate_rotation_subset(user_list)
    
    return rotation_pattern


def calculate_handover_frequency(row):
    # Create a DataFrame from the week and user lists
    week_user_df = pd.DataFrame({
        'week': row['week'],
        'user': row['user']
    })

    # Group by week and check if all entries for each week have the same user
    single_user_week = week_user_df.groupby('week')['user'].nunique() == 1

    # Check if all weeks have a single user
    weekly = single_user_week.all()

    if weekly:
        return "weekly_restriction"
    else:
        return "daily_restriction"


def create_schedule_layers(df):
    # Sort the dataframe by 'start_dt' to ensure the order of rotation is correct.
    df = df.sort_values(by=['start_dt'])

    df["byday"] = df["byday"].apply(str)
    # Create the 'dows_worked_str' using groupby, converting the days worked list into a string
    df['dows_str'] = df.groupby('user')['dow_start'].transform(lambda x: ','.join(map(str, sorted(set(x)))))

    # Group by 'start_time', 'duration', 'dow_start'
    user_combinations = df.groupby(['start_time', 'duration', 'byday', 'recurring', 'dows_str']).agg({
        'dow_start': 'unique',
        'user': list,
        'week': list,
        'start_dt': 'min', # Get the earliest start datetime for each group
        'end_dt': 'max'
    }).reset_index()

    user_combinations['shift_handover_frequency'] = user_combinations.apply(
        lambda row: calculate_handover_frequency(row), axis=1)
    
    user_combinations['shift_pattern'] = user_combinations.apply(
        lambda row: get_rotation_pattern(row), axis=1)
    
    user_combinations = user_combinations.drop(columns=['user', 'week', 'byday', 'dows_str'])
    
    return user_combinations

