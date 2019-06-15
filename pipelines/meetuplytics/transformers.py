from datetime import datetime

import apache_beam as beam


def timestamp_to_str(timestamp, fmt='%Y-%m-%d %H:%M:%S'):
    """Converts a unix timestamp into a formatted string """
    return datetime.fromtimestamp(timestamp).strftime(fmt)


class AddTimestampFn(beam.DoFn):
    """
    Use event-time instead of processing-time
    Divide by 1000.0 to works with seconds and to keep precision
    Event-time is when the event has been produced on the device
    """

    def to_runner_api_parameter(self, unused_context):
        pass

    def __init__(self):
        super(AddTimestampDoFn, self).__init__()

    def process(self, element, **kwargs):
        yield beam.window.TimestampedValue(element, int(element['timestamp']) / 1000.0)


class FilterNoVenueEventsFn(beam.DoFn):
    """ Keep away RSVPS which have a private venue """

    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self, element, *args, **kwargs):
        if 'venue' in element:
            yield element
        else:
            return


class PairTopicWithOneFn(beam.DoFn):
    """ Build a paired-with-one tuple (topic, 1) """

    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self, element, *args, **kwargs):
        _, topics = [element[k] for k in ('group_country', 'group_topics')]

        for topic in topics:
            yield ('%s$%s' % (topic['urlkey'], topic['topic_name']), 1)


class FormatTopTopicFn(beam.DoFn):
    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self,
                element=beam.DoFn.ElementParam,
                timestamp_param=beam.DoFn.TimestampParam,
                window_param=beam.DoFn.WindowParam):
        window_start, window_end, timestamp = [timestamp_to_str(window_param.start),
                                               timestamp_to_str(window_param.end),
                                               int(timestamp_param) + 1000]

        topics = []
        for pair in element:
            key, score = pair
            topic_key, topic_name = key.split('$')
            topics.append({
                    "topic_key" : topic_key,
                    "topic_name": topic_name,
                    "score"     : score
            })

        yield {
                'topics'   : topics,
                'timestamp': timestamp,
                'window'   : {
                        "start": window_start,
                        "end"  : window_end
                }
        }
