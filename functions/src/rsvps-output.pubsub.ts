import * as functions from 'firebase-functions'
import {rsvpsOutput, db, serverTimestamp} from './_config/main.config'
import {Category, CategoryType} from './models/Category'
// Paths
const statsRef = db.doc('rsvps/stats')
const hotTopicStatsRef = statsRef.collection('hot_topics')

const handleHotTopics = (output: any) => {
    const {topics, timestamp, window} = output
    const topicTimestampRef = hotTopicStatsRef.doc(`${timestamp}`)

    try {
        return topicTimestampRef.set({
            topics,
            timestamp,
            window: window,
            lastUpdated: serverTimestamp
        }, {merge: true})
    } catch (e) {
        throw new Error(e)
    }
}

const updateEventsCounter = (output: any) => {
    try {
        return statsRef.set({
            events: output,
            lastUpdated: serverTimestamp
        }, {merge: true})
    } catch (e) {
        throw new Error(e)
    }
}

export const onPublishRSVPS = functions.pubsub
    .topic(rsvpsOutput)
    .onPublish(async (payload: functions.pubsub.Message) => {
        const {category, output} = JSON.parse(Buffer.from(payload.data, 'base64').toString())

        switch (category) {
            case CategoryType.get(Category.HOT_TOPICS):
                return handleHotTopics(output)
            case CategoryType.get(Category.GLOBAL_EVENTS):
                return updateEventsCounter(output)
            default:
                throw new Error('Unknown Category')
        }
    })