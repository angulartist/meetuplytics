import * as functions from 'firebase-functions'
import {rsvpsOutput, db, fv, ts} from './_config/main.config'
import {Category, CategoryType} from './models/Category'
// Paths
const statsRef = db.doc('rsvps/stats')
const topicStatsRef = statsRef.collection('topics')

const handleHotTopics = (collection: any[]) => {
    if (!collection) throw new Error('Collection argument is required.')

    const batch = db.batch()

    collection.forEach((element: any) => {
        const {topic_key, topic_name, score, window_start, window_end, timestamp} = element
        const topicRef = topicStatsRef.doc(`${topic_key}`)
        const topicTimestampRef = topicRef.collection('timestamps').doc(`${timestamp}`)

        batch.set(topicRef, {
            name: topic_name,
            weight: fv.increment(score),
            lastUpdated: ts
        }, {merge: true})

        batch.set(topicTimestampRef, {
            score: fv.increment(score),
            windowStart: window_start,
            windowEnd: window_end,
            timestamp
        }, {merge: true})
    })

    try {
        return batch.commit()
    } catch (e) {
        throw new Error(e)
    }

}

const updateEventsCounter = (score: number) => {
    if (!score) throw new Error('Score argument is required.')

    try {
        return statsRef.set({events: score, lastUpdated: ts}, {merge: true})
    } catch (e) {
        throw new Error(e)
    }
}

export const onPublishRSVPS = functions.pubsub
    .topic(rsvpsOutput)
    .onPublish(async (payload: functions.pubsub.Message) => {
        const {category, collection, score} = JSON.parse(Buffer.from(payload.data, 'base64').toString())

        switch (category) {
            case CategoryType.get(Category.HOT_TOPICS):
                return handleHotTopics(collection)
            case CategoryType.get(Category.GLOBAL_EVENTS):
                return updateEventsCounter(score)
            default:
                throw new Error('Unknown Category')
        }
    })