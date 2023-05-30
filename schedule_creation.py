import requests
import os
from icalendar import Calendar
import datetime

def import_calendar(calendar_source):
    if calendar_source.startswith('http'):
        response = requests.get(calendar_source)
        response.raise_for_status()
        calendar_data = response.text
    else:
        try:
            with open(calendar_source, 'rb') as f:
                calendar_data = f.read()
        except FileNotFoundError:
            raise FileNotFoundError("Invalid file path to the iCal file.")

    calendar = Calendar.from_ical(calendar_data)
    return calendar

def get_user(user_id):
    api_url = f"https://api.pagerduty.com/users/{user_id}"
    headers = {
        "Accept": "application/vnd.pagerduty+json;version=2",
        "Authorization": "API TOKEN"  # Replace with your PagerDuty API key
    }

    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    user_data = response.json()

    return user_data

def create_schedule_layers(calendar):
    schedule_layers = {}

    for component in calendar.walk():
        if component.name == 'VEVENT':
            event_name = component.get('SUMMARY', '')
            event_date = component.get('DTSTART').dt.date()
            event_start_time = component.get('DTSTART').dt.time()
            event_end_time = component.get('DTEND').dt.time()
            weekday_number = event_date.isoweekday()

            try:
                user_data = get_user(event_name)

                start = datetime.combine(event_date, event_start_time).strftime("%Y-%m-%dT%H:%M")
                rotation_virtual_start = start

                start_datetime = datetime.combine(event_date, event_start_time)
                end_datetime = datetime.combine(event_date, event_end_time)
                rotation_turn_length_seconds = (end_datetime - start_datetime).total_seconds()

                users = [
                    {
                        'user_id': event_name,
                        'type': 'user_reference'
                    }
                ]

                schedule_layer = {
                    'start': start,
                    'rotation_virtual_start': rotation_virtual_start,
                    'rotation_turn_length_seconds': rotation_turn_length_seconds,
                    'users': users
                }

                schedule_layers[layer_1] = schedule_layer
            except requests.exceptions.RequestException:
                raise(f"Error getting user from PagerDuty'{event_name}'")

    return schedule_layers
