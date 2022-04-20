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
        elif feed_id == 'bbc-alert':
            rooms[room_id].states['Alert'] = bool(int(payload))
        return

    if feed_id == 'bbc-relay' and rooms[room_id].states['Light'] != bool(int(payload)):
        rooms[room_id].on_change = True
        rooms[room_id].states['Light'] = bool(int(payload))
    elif feed_id == 'bbc-alert' and rooms[room_id].states['Alert'] != bool(int(payload)):
        rooms[room_id].on_change = True
        rooms[room_id].states['Alert'] = bool(int(payload))
    elif feed_id == 'bbc-human' and rooms[room_id].states['No_people'] != int(payload):
        rooms[room_id].on_change = True
        rooms[room_id].states['No_people'] = int(payload)
    elif feed_id == 'bbc-door' and rooms[room_id].states['Door'] != bool(int(payload)):
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
        # self.time_frame = None

    def init_fetch(self):
        # global client
        # client.receive('bbc-door')
        # client.receive('bbc-relay')
        # client.receive('bbc-human')
        # client.receive('bbc-cam')

        self.time_frame = datetime.datetime.now(tz=datetime.timezone.utc)

        # wait until all data has been received
        while len(self.states) != 5:
            continue
        
        # self.states = {
        #     'Light': True,
        #     'No_people': 0,
        #     'Door': True,
        #     'Frame': "",
        #     'Alert': False
        # }
        self.init = False
        print("Initial data:")
        for key in self.states:
            if key != 'Frame':
                print(f'{key}: {self.states[key]}')

    def add_record(self):
        # if there are changes or after every 5 minutes, add a new record and update the statistics values
        curr_time = datetime.datetime.now(tz=datetime.timezone.utc)
        if self.on_change or (curr_time - self.time_frame).seconds >= 60:
            self.time_frame = curr_time
            time_string = curr_time.strftime('%Y-%m-%d')
            # create a new record
            record = {
                'Door status': self.states['Door'],
                'Light status': self.states['Light'],
                'Number of people': self.states['No_people'],
                'Alert': self.states['Alert'],
                'Timestamp': datetime.datetime.now(tz=datetime.timezone.utc)
                }
            room_ref = db.collection('Room').document(str(self.room_id))
            room_doc = room_ref.get()
            # if room document does not exist, create it
            if room_doc.exists:
                room_ref.update({u'Status': record})
            else:
                room_ref.set({
                    u'Id': self.room_id,
                    u'Name': self.room_name,
                    u'Status': record,
                    u'Statistics': [],
                    u'Frame': {}
                })

            # document of the current day
            doc_ref = room_ref.collection('Day').document(time_string)
            doc = doc_ref.get()
            # if document already exists, update statistics also
            if doc.exists:
                doc_dict = doc.to_dict()
                # print(doc_dict)
                num_records = len(doc_dict['Records'])
                last_record = doc_dict['Records'][-1]

                if self.states['No_people'] == 0:
                    average_people = doc_dict['People']
                else:
                    average_people = self.states['No_people']
                    n = 1
                    for rec in doc_dict['Records']:
                        if rec['Number of people'] > 0:
                            n += 1
                            average_people += rec['Number of people']
                    average_people = average_people / n

                # Light usage 
                light_usage = doc_dict['Usage']
                if last_record['Light status'] == True:
                    light_usage += (curr_time - last_record['Timestamp']).seconds


                doc_ref.update({
                    u'Records': firestore.ArrayUnion([record]),
                    u'People': average_people,
                    u'Usage': light_usage
                })
            # if document not exist, create new one
            else:
                average_people = self.states['No_people']
                light_usage = 0
                doc_ref.set({
                    u'People': self.states['No_people'],
                    u'Usage': 0,
                    u'Records': [record]
                })

            # denormalization: also update statistics in parent node to save access time
            stat = {
                'Date': time_string,
                'People': average_people,
                'Usage': light_usage
            }
            room_doc = room_ref.get()
            room_dict = room_doc.to_dict()
            # list of room statistics
            statistics = room_dict['Statistics']
            if len(statistics) == 0 or statistics[-1]['Date'] != time_string:
                statistics.append(stat)
            else:
                statistics[-1] = stat

            room_ref.update({'Statistics': statistics})

            # set change flag to false
            self.on_change = False
        
        if self.new_frame:
            curr_time = datetime.datetime.now(tz=datetime.timezone.utc)
            room_ref = db.collection('Room').document(self.room_id)
            frame_data = {
                u'Frame64': self.states['Frame'],
                u'Timestamp': curr_time
            }
            room_ref.update({u'Frame': frame_data})
            self.new_frame = False

if __name__ == "__main__":
    # Use a service account
    cred = credentials.Certificate('service-account-key.json')
    firebase_admin.initialize_app(cred)

    db = firestore.client()
    
    # All room data
    rooms = {'1': Room('1', 'Lab ABC')}
    # rooms = {'2': Room('2', 'Lab DEF')}
    
    # Client info
    with open("adafruit_key.json", "r") as f:
        api_key = json.load(f)
    
    AIO_FEED_ID = {"human": "bbc-human",
                   "frame": "bbc-cam",
                   "relay": "bbc-relay",
                   "buzzer": "bbc-buzzer",
                   "door": "bbc-door",
                   "alert": "bbc-alert"
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
    # # wait 5 secs for the client to be established
    # time.sleep(5)

    for room_id in rooms:
        rooms[room_id].init_fetch()
 

    while True:
        for room_id in rooms:
            rooms[room_id].add_record()
        # delay for 2 seconds
        time.sleep(2)

# case there is no data in the feed yet ...