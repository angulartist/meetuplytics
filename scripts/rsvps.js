const WebSocket = require('ws');
const {PubSub} = require('@google-cloud/pubsub');
const ws = new WebSocket('ws://stream.meetup.com/2/rsvps');
const pSub = new PubSub({projectId: 'notbanana-7f869'});
const topicName = 'rsvps_source';

const publisher = pSub
    .topic(topicName, {
        batching: {
            maxMessages: 20,
            maxMilliseconds: 5000,
        },
    });

async function addToBatch(
    dataBuffer,
    customAttributes,
) {
    try {
        const messageId = await publisher.publish(dataBuffer, customAttributes);
        console.log(`published ${messageId}`)
    } catch (e) {
        throw new Error('Could not publish.' + e)
    }
}

ws.on('message', incoming = async (data) => {
    const strIt = (e) => e.toString();

    const parsedData = JSON.parse(data);

    const obj = {
        event_id: parsedData['rsvp_id'],
        timestamp: parsedData['mtime'],
        group: parsedData['group'],
        event: parsedData['event'],
        response: parsedData['response'],
        venue: parsedData['venue']
    };

    const str = JSON.stringify(obj);

    const base64str = Buffer.from(str, 'utf8');

    const customAttr = {
        event_id: strIt(obj['event_id']),
        timestamp: strIt(obj['timestamp'])
    };

    try {
        await addToBatch(base64str, customAttr);
    } catch (e) {
        throw new Error(e)
    }
});