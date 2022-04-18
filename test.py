import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

cred = credentials.Certificate('service-account-key.json')
firebase_admin.initialize_app(cred)

db = firestore.client()
time_string = datetime.datetime.now(tz=datetime.timezone.utc).strftime('%Y-%m-%d')
stats = {
    'Average people': 1,
    'Light usage': 0,
    'Records': [] 
}
doc_ref = db.collection('Room-test').document("2").collection("Day").document(time_string)
doc = doc_ref.get()
if doc.exists:
    doc_dict = doc.to_dict()
    num_records = len(doc_dict['Records'])
    doc_ref.update({
        ''
        'Light usage': 
    })
else:
    doc_ref.set({
        'Average people': 0,
        'Light usage': 0,
        'Records': []
    })

print(room_ref.get().to_dict())
# room_ref.update({'a': 0})
# room_ref.set(stats)
# room_ref.update({'Total': 1})