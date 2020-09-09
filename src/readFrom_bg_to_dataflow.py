import argparse
import csv
import logging
import os
import sys

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict

class Printer(beam.DoFn):
    def process(self, data_item):
        print(data_item)


class MyClass(beam.DoFn):

    def process(self, element):
        symbol = str(element.pop('Symbol'))
        element['current_price'] = str(element['current_price'])
        return [(symbol, element)]



options = PipelineOptions(
    flags=argv,
    runner='DataflowRunner',
    project='global-cursor-278922',
    job_name='unique-job-name',
    temp_location='gs://egen-project/dataflow_temp',
    region='us-central1'
)

def run():
    with beam.Pipeline(options=options) as p:
        (p
         | beam.io.Read(beam.io.BigQuerySource(
                    query='SELECT * FROM 
                    global-cursor-278922.stock_data.streaming_data', 
                    use_standard_sql=True))

         | "Print" >> beam.ParDo(Printer())

         )



if __name__ == '__main__':
    run()









