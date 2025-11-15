import boto3
import json
import os

class EventBridge:
    def __init__(self):
        self.client = None
        self.bus_name = None  # Lazy load

    def _get_client(self):
        if self.client is None:
            self.client = boto3.client('events')
            self.bus_name = os.environ.get('EVENT_BUS_NAME')  # Use .get() to avoid KeyError if missing
            if not self.bus_name:
                raise ValueError("EVENT_BUS_NAME environment variable not set")
        return self.client

    def publish_event(self, source, detail_type, detail):
        client = self._get_client()  # Lazy init here
        client.put_events(
            Entries=[
                {
                    'Source': source,
                    'DetailType': detail_type,
                    'Detail': json.dumps(detail),
                    'EventBusName': self.bus_name
                }
            ]
        )
