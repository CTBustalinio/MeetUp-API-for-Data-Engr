import boto3
import json
import requests

client = boto3.client('firehose','us-east-1')


r = requests.get('http://stream.meetup.com/2/rsvps', stream=True)





def fire_hose():
    counter=0
    for line in r.iter_lines():
      try:
        if 'rsvp' in line:
            counter += 1
            client.put_record(
                   DeliveryStreamName='MeetUpStream',
                   Record={'Data': json.dumps(line)+'|||'})
                   # Record={'Data':json.dumps(str(tweet)+'\n')})
            if counter %100 == 0:
                print(counter)
      except:
        pass

if  __name__ == '__main__':
    fire_hose()
