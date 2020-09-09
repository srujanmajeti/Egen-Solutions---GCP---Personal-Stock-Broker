import csv 
import json 
from google.cloud import pubsub_v1
from google.oauth2 import service_account

class Stocks:
    def __init__(self, current_timestamp, open, high, low, close, volume, dividends,stock_splits, symbol):
        self.current_timestamp = current_timestamp
        self.open              = open
        self.high              = high
        self.low               = low
        self.close             = close
        self.volume            = volume
        self.dividends         = dividends
        self.stock_splits      = stock_splits
        self.symbol            = symbol

publisher = pubsub_v1.PublisherClient.from_service_account_json('egen_project_sa.json')

topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id='global-cursor-278922',
    topic='test_stocks',
)


with open('data_duplicate.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    next(csv_reader)

    for row in csv_reader:
        print(f"\n {row}")
        obj = Stocks(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
        jsonStr = json.dumps(obj.__dict__)
        

        publisher.publish(topic_name, jsonStr if isinstance(jsonStr, bytes) else jsonStr.encode("utf-8"), spam='stock_data')
        print("End of script")






