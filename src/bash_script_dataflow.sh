#!/usr/bin/env bash

python3 -m src.pubsub_dataflow_pubsub  --runner DataflowRunner  --project global-cursor-278922  --region us-central1  --temp_location gs://egen_project/temp_data/  --input_topic "projects/global-cursor-278922/topics/test_stocks"  --output_topic "projects/global-cursor-278922/topics/email_topic"  --streaming
