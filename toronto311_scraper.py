import pandas as pd
import requests
import json
import datetime as dt
from datetime import datetime
import dateutil.parser
import sys

ENDPOINT = "https://secure.toronto.ca/webwizard/ws/requests.json?"

def date_range(start, end, N, tz_string="-05:00"):
    """Splits a given date range into N contiguous subintervals in ISO8601 timestamp format. 
    
    start and end must be strings in "YYYYMMDD" format.
    tz_string assumed to be Toronto timezone.
    
    """
    start = datetime.strptime(start,"%Y%m%d")
    end = datetime.strptime(end,"%Y%m%d")
    diff = (end  - start ) / N
    dates = []
    for i in range(N):
        dates += [(start + diff * i).strftime("%Y-%m-%dT%H:%M:%S") + tz_string]
    dates += [end.strftime("%Y-%m-%dT%H:%M:%S") + tz_string]
    return dates

service_requests = []

dates = date_range(sys.argv[1], sys.argv[2], int(sys.argv[3]))

# TODO: Add error handling
for i, d in enumerate(dates[:-1]):
    curr_params = {
        "start_date": dates[i],
        "end_date": dates[i+1],
        "jurisdiction_id":"toronto.ca"
     }
    resp = requests.get(ENDPOINT, params=curr_params)
    resp_json = resp.json()['service_requests']
    service_requests += resp_json
    print("Step {0}: {1} to {2}, number of results: {3}".format(i, dates[i], dates[i+1], len(resp_json)))

# Encode our list into a JSON string
json_str = json.dumps(service_requests)

# Read the JSON string into a DataFrame
df = pd.read_json(json_str, orient='records')

########################################
# Can insert data cleaning, analysis here
########################################

df.to_csv('toronto311_{0}_{1}.csv'.format(sys.argv[1], sys.argv[2]))