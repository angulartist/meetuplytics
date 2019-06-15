import six
import json
import logging
import apache_beam as beam


class Category(object):
    HOT_TOPICS = "HOT_TOPICS"
    TOPICS_PER_COUNTRY = "TOPICS_PER_COUNTRY"
    GLOBAL_EVENTS = "GLOBAL_EVENTS"


class PrintFn(beam.DoFn):
    """ A DoFn that prints the current element, its window, and its timestamp. """

    def to_runner_api_parameter(self, unused_context):
        pass

    def __init__(self, *unused_args, **unused_kwargs):
        super(PrintFn, self).__init__(*unused_args, **unused_kwargs)

    def process(self, element, timestamp=beam.DoFn.TimestampParam,
                window=beam.DoFn.WindowParam):
        logging.info('element=%s | window=%s | timestamp=%s', element, window, timestamp)

        yield element


class WriteToBigQuery(beam.PTransform):
    """ Generate, format, and write BigQuery table row information. """

    def __init__(self, table_name, dataset, schema, project):
        """
        Initializes the transform.
        Args:
          table_name: Name of the BigQuery table to use.
          dataset: Name of the dataset to use.
          schema: Dictionary in the format {'column_name': 'bigquery_type'}
          project: Name of the Cloud project containing BigQuery table.
        """
        super(WriteToBigQuery, self).__init__()
        self.table_name = table_name
        self.dataset = dataset
        self.schema = schema
        self.project = project

    def get_schema(self):
        """ Build the output table schema. """
        return ', '.join(
                '%s:%s' % (col, self.schema[col]) for col in self.schema)

    def get_schema(self):
        """ Build the output table schema. """
        return ', '.join(
                '%s:%s' % (col, self.schema[col]) for col in self.schema)

    def expand(self, p):
        return (
                p
                | 'Convert To Row' >> beam.Map(
                lambda element: {col: element[col] for col in self.schema})
                | beam.io.WriteToBigQuery(self.table_name, self.dataset, self.project,
                                          self.get_schema()))


class BindCategoryFn(beam.DoFn):
    def to_runner_api_parameter(self, unused_context):
        pass

    def __init__(self, category, *unused_args):
        super(BindCategoryFn, self).__init__(*unused_args)
        self.category = category

    def process(self, element):
        yield {"category": self.category, "output": element}


class WriteToPubSub(beam.PTransform):
    def __init__(self, topic, category):
        super(WriteToPubSub, self).__init__()
        self.topic = topic
        self.category = category

    def expand(self, p):
        output = (p
                  | 'Attach Category' >> beam.ParDo(
                        BindCategoryFn(category=self.category))
                  | 'Make base64 string' >> beam.Map(
                        lambda element: json.dumps(element))
                  | 'DEBUG:' >> beam.ParDo(PrintFn()))

        return output | 'Publish To Pub/Sub' >> beam.io.WriteToPubSub(
                topic=self.topic).with_output_types(six.binary_type)
