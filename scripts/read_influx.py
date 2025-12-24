import datetime as datetime
import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from influxdb_client import Dialect, InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

## INFLUX
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url_influx = os.getenv("INFLUXDB_URL")
bucket = os.getenv("INFLUXDB_BUCKET")

def main():
    influx = InfluxDBClient(url=url_influx, token=token, org=org, debug=False)
    query_api = influx.query_api()
    query = '''
    from(bucket: "800xA")
        |> range(start: -10m)
        |> filter(fn: (r) => r["_measurement"] == "UA")
        
        |> last()
    '''
    
    records = query_api.query_stream(query=query)
    for record in records:
        print(f"Value: {record}")

    # query = '''
    # from(bucket: "800xA")
    #     |> range(start: -1m)
    # '''
    csv_result = query_api.query_csv(query, dialect=Dialect(header=False, delimiter=',', comment_prefix='#', annotations=[], date_time_format="RFC3339"))
    d = {}
    for csv_line in csv_result:
        # print(f"CSV: {csv_line[7]} - {csv_line[6]}")
        print(csv_line)
        d[csv_line[2]] = {"tag": csv_line[7], "value": csv_line[6]}
    print(d)
    df = pd.DataFrame.from_dict(data=d, orient='index') #, columns=['Tag', 'Val'])
    # index = pd.Index(np.linspace(1, df.shape[0], 7))
    # df = df.set_index(index)
    print(df[df['value'] == 1])

    data_frame = query_api.query_data_frame(query=query)
    columns = data_frame.columns
    data_frame = data_frame.rename(columns={columns[6]:'sensor'})
    data_frame['diagrams'] = columns[6]
    print(data_frame)
if __name__ == "__main__":
    main()