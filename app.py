import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import datetime
import time
from Adafruit_IO import MQTTClient

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
    print(f"Receive data from {feed_id} of room {room_id}: {payload}")
    
    if feed_id == 'bbc-relay':
        rooms[room_id].states['Light'] = bool(payload)
    elif feed_id == 'bbc-human':
        rooms[room_id].states['No_people'] = int(payload)
    elif feed_id == 'bbc-door':
        rooms[room_id].states['Door'] = bool(payload)

class Room:
    def __init__(self, room_id, room_name):
        self.init_fetch() 
        self.room_id = room_id
        self.room_name = room_name
        # self.states = {'Door': None, 'No_people': None, 'Light': None}
    def init_fetch(self):
        self.states = {'Door': True, 'No_people': 3, 'Light': False}
    def add_record(self):
        # new_id = len(db.collection(u'Room').get())
        record = {
            'Door status': self.states['Door'],
            'Light status': self.states['Light'],
            'Number of people': self.states['No_people'],
            'Time stamp': datetime.datetime.now(tz=datetime.timezone.utc)
            }
        room_ref = db.collection('Room').document(str(room_id))
        room_ref.update({'Records': firestore.ArrayUnion([record])})


if __name__ == "__main__":
    # Use a service account
    cred = credentials.Certificate('service-account-key.json')
    firebase_admin.initialize_app(cred)

    db = firestore.client()

    # All room data
    rooms = {'1': Room('1', 'Lab ABC')}
    
    # Client info
    AIO_FEED_ID = {"human": "bbc-human",
                   "frame": "bbc-cam",
                   "relay": "bbc-relay",
                   "buzzer": "bbc-buzzer",
                   "door": "bbc-door"
                   }

    AIO_USERNAME = "KanNan312"
    AIO_KEY = "aio_KlLB95MM7gRPWXNmsk0ZDymlZMfh"

    # Establish MQTT connections:
    client = MQTTClient(AIO_USERNAME , AIO_KEY)
    client.on_connect = connected
    client.on_disconnect = disconnected
    client.on_message = message
    client.on_subscribe = subscribe
    client.connect()
    client.loop_background()

    while True:
        for room_id in rooms:
            rooms[room_id].add_record()
        time.sleep(5)

