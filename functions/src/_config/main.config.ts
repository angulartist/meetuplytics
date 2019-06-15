import * as admin from 'firebase-admin'
import * as PubSub from '@google-cloud/pubsub'

admin.initializeApp()

/* PubSub */
export const pSub: PubSub.PubSub = new PubSub.PubSub()
export const rsvpsOutput: string = 'rsvps_out'

/* Firestore */
export const db: FirebaseFirestore.Firestore = admin.firestore()
db.settings({timestampsInSnapshots: true})
export const ts: FirebaseFirestore.FieldValue = admin.firestore.FieldValue.serverTimestamp()
export const fv = admin.firestore.FieldValue