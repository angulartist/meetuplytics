import * as admin from 'firebase-admin'
/* Init admin SDK */
admin.initializeApp()

/* PubSub */
export const rsvpsOutput: string = 'rsvps_out'

/* Firestore */
export const db: FirebaseFirestore.Firestore = admin.firestore()
db.settings({timestampsInSnapshots: true})
export const serverTimestamp: FirebaseFirestore.FieldValue = admin.firestore.FieldValue.serverTimestamp()
export const transaction = admin.firestore.FieldValue