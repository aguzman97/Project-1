import json
import boto3
import random 
import datetime
import yfinance as yf 
import pandas as pd
import json

kinesis = boto3.client('kinesis', "us-east-2")

def lambda_handler(event, context):
    tickers=["FB","SHOP","BYND","NFLX","PINS","SQ","TTD","OKTA","SNAP","DDOG"]
    data=yf.download(tickers,start="2021-05-11", end="2021-05-12", interval="5m",period="1d")
    data=data.stack().reset_index().rename(index=str, columns={"level_1": "Symbol"}).sort_values(['Symbol','Datetime'])
    data.rename(columns={'Datetime': 'ts','High':'high','Low':'low'}, inplace=True)
    data=data.drop(['Adj Close','Close','Volume','Open'],axis=1)
    data['ts'] = data['ts'].dt.strftime('%Y-%m-%d %H:%M:%S')
    data['high']=data['high'].round(decimals=1)
    data['low']=data['low'].round(decimals=2)
    
    for index,rows in data.iterrows():
        data2= json.dumps({"high":rows.high,"low":rows.low,"ts":rows.ts,"name":str(rows.Symbol)},
                      separators = (", ", " : "))+"\n"
        print(data2)
        kinesis.put_record(StreamName="STA9760F2021_stream1",Data=data2,PartitionKey="partitionkey")
    return {
        'statusCode': 200,
        'body': json.dumps(f"Done")
    }






