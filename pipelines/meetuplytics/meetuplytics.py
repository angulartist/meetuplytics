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
from meetuplytics.custom.combiners.top import TopDistinctFn
from meetuplytics.transformers import *
from meetuplytics.utils import *

SECONDS_IN_1_DAY = 3600 * 24


def run(argv=None):
    class MyOptions(PipelineOptions):

        @classmethod
        def _add_argparse_args(cls, parser):
            parser.add_argument(
                    '--input', default='projects/notbanana-7f869/topics/rsvps_source')
            parser.add_argument(
                    '--output', default='projects/notbanana-7f869/topics/rsvps_out')

    options = PipelineOptions(flags=argv)

    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'notbanana-7f869'
    google_cloud_options.staging_location = 'gs://notbanana-7f869.appspot.com/staging'
    google_cloud_options.temp_location = 'gs://notbanana-7f869.appspot.com/temp'
    google_cloud_options.job_name = 'demo-job'
    """
    -> Run the pipeline on the Cloud Dataflow runner.
    $ python pipelines/main.py --setup_file path/to/setup.py
    """
    # options.view_as(StandardOptions).runner = 'DataflowRunner'

    with beam.Pipeline(options=options) as p:
        my_options = options.view_as(MyOptions)
        input_topic = my_options.input
        output_topic = my_options.output

        """
        -> Consumes/collects events sent by the input Pub/Sub topic.
        @: id_label argument is a unique identifier used by the pipeline to
        deduplicate events : Exactly-once semantic.
        """
        inputs = \
            (p
             | 'Read From Pub/Sub' >> beam.io.ReadFromPubSub(
                            topic=input_topic,
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
                                        trigger.AfterCount(20),
                                        # AfterProcessingTime is experimental.
                                        # Not implemented yet.
                                        trigger.AfterProcessingTime(1 * 60)
                                )),
                        accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
         | 'Count events globally' >> beam.CombineGlobally(
                        beam.combiners.CountCombineFn()).without_defaults()
         | 'Publish %s' % 'Events' >> WriteToPubSub(topic=output_topic,
                                                    category=Category.GLOBAL_EVENTS))

        """
        -> Outputs the top 10 hottest topics within a Fixed Window of X seconds. 
        Values used are for testing purposes.
        TODO: Fix duplicated k/v pairs when using TopCombineFn with triggers.
        """
        (inputs
         | 'Apply Window of time %s' % 'Topics' >> beam.WindowInto(
                        beam.window.FixedWindows(size=10 * 60),
                        trigger=trigger.Repeatedly(trigger.AfterCount(10)),
                        accumulation_mode=trigger.AccumulationMode.ACCUMULATING)
         | beam.Map(lambda element: element['group'])
         | beam.ParDo(PairTopicWithOneFn())
         | beam.CombinePerKey(sum)
         | 'Top 10 Topics' >> beam.CombineGlobally(
                        TopDistinctFn(n=10,
                                      compare=lambda a, b: a[1] < b[
                                          1])).without_defaults()
         | 'DictFormat %s' % 'Topics' >> beam.ParDo(FormatTopTopicFn())
         | 'Publish %s' % 'Topics' >> WriteToPubSub(topic=output_topic,
                                                    category=Category.HOT_TOPICS))
