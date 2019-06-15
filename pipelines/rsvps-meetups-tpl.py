"""
Streaming processing pipeline to build some data upon meetups RSVPS events
"""

from __future__ import absolute_import

import argparse
import json
import logging
import six

# Beam
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions
from apache_beam.transforms import trigger
from transformers import *
from utils import *

SECONDS_IN_1_DAY = 3600 * 24


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
            '--input_topic', default='projects/notbanana-7f869/topics/rsvps_source')
    parser.add_argument(
            '--output_topic', default='projects/notbanana-7f869/topics/rsvps_out')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True

    # pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'notbanana-7f869'
    google_cloud_options.staging_location = 'gs://notbanana-7f869.appspot.com/staging'
    google_cloud_options.temp_location = 'gs://notbanana-7f869.appspot.com/temp'
    google_cloud_options.job_name = 'demo-job'

    with beam.Pipeline(options=pipeline_options) as p:
        """
        -> Consumes/collects events sent by the input Pub/Sub topic.
        @: id_label argument is a unique identifier used by the pipeline to
        deduplicate events : Exactly-once semantic.
        """
        inputs = \
            (p
             | 'Read From Pub/Sub' >> beam.io.ReadFromPubSub(
                            topic=known_args.input_topic,
                            # id_label='event_id'
                    ).with_output_types(six.binary_type)
             | 'Decode Binary' >> beam.Map(lambda element: element.decode('utf-8'))
             | 'Transform Json To Dict' >> beam.Map(lambda element: json.loads(element))
             | 'Filter noVenue' >> beam.ParDo(FilterNoVenueEventsFn()))

        """ 
        -> Outputs the total number of events globally processed by the pipeline.
        Triggering early results from the window every X seconds (processing time trigger)
        or triggering when the current pane has collected at least N elements (data-driven trigger)
        Values used are for testing purposes.
        """
        (inputs
         | 'Apply Global Window' >> beam.WindowInto(
                        beam.window.GlobalWindows(),
                        trigger=trigger.Repeatedly(
                                trigger.AfterAny(
                                        trigger.AfterCount(25),
                                        trigger.AfterProcessingTime(1 * 60)
                                )),
                        accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
         | 'Count events globally' >> beam.CombineGlobally(
                        beam.combiners.CountCombineFn()).without_defaults()
         | 'Publish %s' % 'Events' >> WriteToPubSub(topic=known_args.output_topic,
                                                    category=Category.GLOBAL_EVENTS))

        """
        -> Outputs the top 10 hottest topics within a Fixed Window of X seconds. 
        Values used are for testing purposes.
        """
        (inputs
         | 'Apply Window of time %s' % 'Topics' >> beam.WindowInto(
                        beam.window.FixedWindows(size=5 * 60))
         # trigger=trigger.Repeatedly(trigger.AfterCount(2)),
         # accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
         | beam.Map(lambda element: element['group'])
         | beam.ParDo(PairTopicWithOneFn())
         | beam.CombinePerKey(sum)
         | 'Top 10 Topics' >> beam.CombineGlobally(
                        beam.combiners.TopCombineFn(n=10,
                                                    compare=lambda a, b: a[1] < b[
                                                        1])).without_defaults()
         | 'DictFormat %s' % 'Topics' >> beam.ParDo(FormatTopTopicFn())
         | 'Publish %s' % 'Topics' >> WriteToPubSub(topic=known_args.output_topic,
                                                    category=Category.HOT_TOPICS))
        # ...BigQueryIO Example...

        # | '' >> WriteToBigQuery(
        #            'topics', 'rsvps', {
        #                'country': 'STRING',
        #                'topic': 'STRING',
        #                'score': 'INTEGER',
        #                'window_start': 'STRING',
        #                'window_end': 'STRING',
        #                'timestamp': 'INTEGER',
        #            }, options.view_as(GoogleCloudOptions).project))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
