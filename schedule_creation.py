import requests
import os
from icalendar import Calendar
import datetime

def import_calendar(calendar):
    if calendar.startswith('http'):
        response = requests.get(calendar)
        response.raise_for_status()
        calendar_data = response.text
    else:
        try:
            with open(calendar, 'rb') as f:
                calendar_data = f.read()
        except FileNotFoundError:
            raise FileNotFoundError("Invalid file path to the iCal file.")

    calendar = Calendar.from_ical(calendar_data)
    return calendar

def get_user(user_id, api_key):
    api_url = f"https://api.pagerduty.com/users/{user_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.pagerduty+json;version=2",
        "Authorization": api_key 
    }

    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    user = response.json()

    return user

def create_schedule_layers(calendar, api_key):
    schedule_layers = {}
    i = 1

    for component in calendar.walk():
        if component.name == 'VEVENT':
            pd_user_id = component.get('SUMMARY', '')
            event_date = component.get('DTSTART').dt.date()
            event_start_time = component.get('DTSTART').dt.time()
            event_end_time = component.get('DTEND').dt.time()
            weekday_number = event_date.isoweekday()

            try:
                user = get_user(pd_user_id, api_key)

                start = datetime.combine(event_date, event_start_time).strftime("%Y-%m-%dT%H:%M")
                rotation_virtual_start = start

                start_datetime = datetime.combine(event_date, event_start_time)
                end_datetime = datetime.combine(event_date, event_end_time)
                rotation_turn_length_seconds = (end_datetime - start_datetime).total_seconds()

                users = [
                    {
                        'user_id': pd_user_id,
                        'type': 'user_reference'
                    }
                ]

                layer = {
                    'start': start,
                    'rotation_virtual_start': rotation_virtual_start,
                    'rotation_turn_length_seconds': rotation_turn_length_seconds,
                    'users': users
                }

                layer_name = f'layer_{i}'
                schedule_layers[layer_name] = layer
            except requests.exceptions.RequestException:
                raise RequestException(f"Error getting user from PagerDuty'{pd_user_id}'")

    return schedule_layers
