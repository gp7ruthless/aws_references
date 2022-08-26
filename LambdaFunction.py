import base64
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

# Sample Recieved Packet :
# {'price': 3.0, 'price_timestamp': '2022-03-04 12:30:00-05:00', '52_week_low': 2.585, 'stock_id': 'PSFE', '52_week_high': 17.25}

def lambda_handler(event, context):
  client = boto3.client("sns")
  dynamodb = boto3.resource('dynamodb')
  table = dynamodb.Table("DynamoDB_GL_Project")
  
  for record in event['Records']:
    #Kinesis data is base64 encoded therefore decoding is done :
    payload=base64.b64decode(record["kinesis"]["data"])
    res = json.loads(payload, parse_float=Decimal)

  lower_limit = 1.35*float(res["52_week_low"]) # 135% of 52 Week Low
  upper_limit = 0.7*float(res["52_week_high"]) # 70% of 52 Week High

  date = res['price_timestamp'][:10] # Extracting the date 
  #print(date)
  
  alert_flag = False
  
  # Querying in the DynamoDB table if any alert already exists for the stock :
  response = table.query(KeyConditionExpression=Key('stock_id').eq(res["stock_id"]))
  items = response['Items']
  #print(items)
  
  # Check for each queried items, if an alert is there for THAT DAY ONLY,
  # Then no further alerts will be raised.
  for item in items:
    if(item['price_timestamp'][:10] == date):
      alert_flag = True
  
  # Checking for the stock alert condition ONLY IF alert_flag is False :
  if(alert_flag == False):
    if(float(res["price"])<=lower_limit or float(res["price"])>=upper_limit):
      # Generate Alert on SNS
      sns_response = client.publish(
        TopicArn='arn:aws:sns:us-east-1:796109910587:SNS_GL_Project',
        Message="Stock price alert!\n" + str(res)
      ) 
      # Write entry to DynamoDB
      table.put_item(Item=res)