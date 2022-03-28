import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import datetime
import time
from Adafruit_IO import MQTTClient
import json

def connected(client):
    print("Connect successfully...")
    for feed_id in AIO_FEED_ID:
        client.subscribe(AIO_FEED_ID[feed_id])

def subscribe(client , userdata , mid , granted_qos):
    print("Subcribe successfully...")

def disconnected(client):
    print("Disconnecting...")
    sys.exit (1)

def message(client , feed_id , payload):
    room_id = '1'
    feed_id = feed_id.split('/')[-1]
    # If we have more than one room: the room id needs to be inferred from feed_id
    if feed_id != 'bbc-cam':
        print(f"Receive data from {feed_id} of room {room_id}: {payload}")
    if rooms[room_id].init:
        if feed_id == 'bbc-relay':
            rooms[room_id].states['Light'] = bool(int(payload))
        elif feed_id == 'bbc-human':
            rooms[room_id].states['No_people'] = int(payload)
        elif feed_id == 'bbc-door':
            rooms[room_id].states['Door'] = bool(int(payload))
        elif feed_id == 'bbc-cam':
            rooms[room_id].states['Frame'] = payload
        return

    if feed_id == 'bbc-relay' and rooms[room_id].states['Light'] != bool(int(payload)):
        rooms[room_id].on_change = True
        rooms[room_id].states['Light'] = bool(int(payload))
    elif feed_id == 'bbc-human' and rooms[room_id].states['No_people'] != int(payload):
        rooms[room_id].on_change = True
        rooms[room_id].states['No_people'] = int(payload)
    elif feed_id == 'bbc-door' and rooms[room_id].states['Door'] != bool(int(payload)):
        print('Hi')
        rooms[room_id].on_change = True
        rooms[room_id].states['Door'] = bool(int(payload))
    elif feed_id == 'bbc-cam':
        rooms[room_id].new_frame = True
        rooms[room_id].states['Frame'] = payload

class Room:
    def __init__(self, room_id, room_name):
        self.states = {}
        self.room_id = room_id
        self.room_name = room_name
        # if db not contain the document for this room:
        # initialize collection -> document -> fields
        self.on_change = True
        self.new_frame = True
        self.init = True

    def init_fetch(self):
        global client
        client.receive('bbc-door')
        client.receive('bbc-relay')
        client.receive('bbc-human')
        client.receive('bbc-cam')
        while len(self.states) != 4:
            continue
        # self.states = {'Door': True, 'No_people': 3, 'Light': False}
        self.init = False
        print("Initial data:")
        for key in self.states:
            if key != 'Frame':
                print(f'{key}: {self.states[key]}')
    def add_record(self):
        # new_id = len(db.collection(u'Room').get())
        if self.on_change:
            record = {
                'Door status': self.states['Door'],
                'Light status': self.states['Light'],
                'Number of people': self.states['No_people'],
                'Time stamp': datetime.datetime.now(tz=datetime.timezone.utc)
                }
            room_ref = db.collection('Room').document(str(room_id))
            room_ref.update({'Records': firestore.ArrayUnion([record])})
            self.on_change = False
        if self.new_frame:
            frame_ref = db.collection('Frame').document(f'Room {self.room_id}')
            frame_ref.set({'frame64': self.states['Frame']})
            self.new_frame = False


if __name__ == "__main__":
    # Use a service account
    cred = credentials.Certificate('service-account-key.json')
    firebase_admin.initialize_app(cred)

    db = firestore.client()
    
    # All room data
    rooms = {'1': Room('1', 'Lab ABC')}
    
    # Client info
    with open("adafruit_key.json", "r") as f:
        api_key = json.load(f)
    
    AIO_FEED_ID = {"human": "bbc-human",
                   "frame": "bbc-cam",
                   "relay": "bbc-relay",
                   "buzzer": "bbc-buzzer",
                   "door": "bbc-door"
                   }

    AIO_USERNAME = str(api_key['Username'])
    AIO_KEY = str(api_key['Key'])

    # Establish MQTT connections:
    client = MQTTClient(AIO_USERNAME , AIO_KEY)
    client.on_connect = connected
    client.on_disconnect = disconnected
    client.on_message = message
    client.on_subscribe = subscribe
    client.connect()
    client.loop_background()
    # wait 5 secs for the client to be established
    time.sleep(5)
    for room_id in rooms:
        rooms[room_id].init_fetch()
 

    while True:
        for room_id in rooms:
            rooms[room_id].add_record()
        # Update the data every 10 seconds (if there is sth changed)
        time.sleep(10)

# case there is no data in the feed yet ...