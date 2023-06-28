import requests
import os
from icalendar import Calendar
from datetime import datetime as dt, timedelta, timezone
from dateutil.rrule import rrule as rr, rrulestr
from dateutil import tz
import pandas as pd
import argparse
import sys


def import_calendar(calendar):
    if calendar.startswith('http'):
        try: 
            response = requests.get(calendar)
            calendar_data = response.text
        except requests.exceptions.RequestException:
            raise SystemExit("Cannot import calendar from invalid URL")
            
    else:
        try:
            with open(calendar, 'rb') as f:
                calendar_data = f.read()
        except FileNotFoundError:
            raise FileNotFoundError("Invalid file path to the iCal file.")

    calendar = Calendar.from_ical(calendar_data)

    return calendar


def get_user(pd_user_id, api_key):
    api_url = f"https://api.pagerduty.com/users/{pd_user_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.pagerduty+json;version=2",
        "Authorization": f"Token token={api_key}"
    }

    try: 
        response = requests.get(api_url, headers=headers)
        user = response.json()
    except requests.exceptions.RequestException:
        raise SystemExit(f"Error getting user from PagerDuty'{pd_user_id}'")

    return user


def create_schedule_df(calendar, api_key):
    shifts = []
    
    for component in calendar.walk():
        pd_user_id = component.get('summary')

        #TODO: error handling if we cannot extract the vevent component
        if component.name == 'VEVENT':
            if pd_user_id:
                user = get_user(pd_user_id, api_key)
            
            start_dt = component.get('dtstart').dt
            end_dt = component.get("dtend").dt
        
            if component.get("rrule"):
                rrule = component.get("rrule")
                instances = extract_recurring_events(pd_user_id, rrule, start_dt, end_dt)
                shifts.extend(instances)
            #TODO: determine how to handle events that do not recurr
            else:                 
                shift = {
                    "user_id": pd_user_id,
                    "start": start_dt,
                    "end": end_dt,
                    "recurrence_end": True
                }
                shifts.append(shift)

                
    df = pd.DataFrame(shifts)

    df = df.drop_duplicates().sort_values("start")
    
    return df

                
def extract_recurring_events(name, rrule, start, end):
    instances = []
    rrule_str = rrule.to_ical().decode('utf-8')
    dateutil_rrule = rrulestr(rrule_str, dtstart=start)
    until = rrule.get("until", [None])[0]
    shift_duration = end - start
    max_timeframe = start + timedelta(days=365)
    
    for event_start in dateutil_rrule:
        if event_start > max_timeframe:
            break
        else: 
            shift = {
                "user_id": name,
                "start": event_start,
                "end": event_start + shift_duration,
                "recurrence_end": until is not None,
            }
            instances.append(shift)

    return(instances)


def add_columns_for_creating_layers(df):
    
    df["start"] = (pd.to_datetime(df['start'], utc=True)).dt.tz_convert('UTC')
    df["end"] = (pd.to_datetime(df['end'], utc=True)).dt.tz_convert('UTC')
    
    df["dow"] = df["start"].dt.dayofweek
    df["start_time"] = df["start"].dt.time
    df["duration"] = (df["end"] - df["start"]).dt.total_seconds()
    df["week"] = df["start"].dt.isocalendar().week

    return df


def create_weekly_layers(df):

    schedule_restrictions_df = df.groupby(['start_time', 'duration', "user_id", "recurrence_end"]).agg({
        "dow": lambda x: str(set(x)), "week": "first", "start": "first", "end": "last"}).reset_index().sort_values("week")
    

    schedule_restrictions_df = schedule_restrictions_df.groupby(['start_time', 'duration', "recurrence_end", "dow"]).agg({
        "user_id": list, "week": list, "start": "min", "end": "max"}).reset_index()
    
    week_span = (schedule_restrictions_df["end"] - schedule_restrictions_df["start"]).dt.days/7

    weekly_rotation_group = schedule_restrictions_df[schedule_restrictions_df['week'].apply(lambda x: len(set(x)) > 1)]

    weekly_rotation_individual = schedule_restrictions_df[(schedule_restrictions_df['week'].apply(lambda x: len(set(x)) == 1)) & 
    (week_span > 1)]

    weekly_rotation = pd.concat([weekly_rotation_group, weekly_rotation_individual], ignore_index=True)

   
    
def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--calendar", "-c", dest="calendar", required=True)
    parser.add_argument("--api_key", "-a", dest="api_key", required=True)
    args = parser.parse_args()
    calendar = import_calendar(args.calendar)
    df = create_schedule_df(calendar, args.api_key)
    df = add_columns_for_creating_layers(df)
    schedule_layers = create_weekly_layers(df)


if __name__ == '__main__':
    sys.exit(main())

