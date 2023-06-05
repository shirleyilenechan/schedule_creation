import requests
import os
from icalendar import Calendar
from datetime import datetime as dt, timedelta, timezone
import pytz
from dateutil.rrule import rrulestr
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
    df = pd.DataFrame(columns = ["name", "start" "end"])
    shifts = []
    
    for component in calendar.walk():
        pd_user_id = component.get('summary')

        if component.name == 'VEVENT':
            if pd_user_id:
                user = get_user(pd_user_id, api_key)
            
            start_dt = component.get('dtstart').dt
            end_dt = component.get('dtend').dt
            
            if component.get("rrule"):
                rrule = component.get("rrule")
                instances = extract_recurring_events(pd_user_id, rrule, start_dt, end_dt)
                shifts.extend(instances)

            else:                 
                shift = {
                    "name": pd_user_id,
                    "start": start_dt,
                    "end": end_dt,
                    "recurring": False
                }

                shifts.append(shift)

    df = pd.DataFrame(shifts)
    return df

                
def extract_recurring_events(name, rrule, start, end):

    instances = []
    rrule_str = rrule.to_ical().decode("utf-8")
    rule = rrulestr(rrule_str)
    
    shift_length = end - start

    shifts = list(rule)

    for shift_start in shifts:
        shift = {
            "name": name,
            "start":shift_start,
            "end": shift_start + shift_length,
            "recurring": True
        }

        instances.append(shift)

    return instances

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--calendar", "-c", dest="calendar", required=True)
    parser.add_argument("--api_key", "-a", dest="api_key", required=True)
    args = parser.parse_args()
    calendar = import_calendar(args.calendar)
    create_schedule_df(calendar, args.api_key)

if __name__ == '__main__':
    sys.exit(main())

