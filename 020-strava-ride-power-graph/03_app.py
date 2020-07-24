# PROGRAM ARGUMENTS

# setup program usage and parse arguments
import os, sys, argparse
parser = argparse.ArgumentParser()
parser.add_argument("--access_token", "-t", help="Access Token")
args = parser.parse_args()
# terminate if access token is not set
if args.access_token is None:
    sys.exit("Please specify an Access Token (--access_token) or read Usage (--help)")

# instanciate strava client
from stravalib import Client, exc
# stop at wrong token
try:
    # https://pythonhosted.org/stravalib/api.html#stravalib.client.Client
    client = Client(access_token=args.access_token)
except exc.AccessUnauthorized:
    sys.exit("Your token is not valid. Please run login.py to get one.")
# stop at API rate limit
try:
    athlete = client.get_athlete()
    print("Hello ", athlete.firstname.strip(), " ", athlete.lastname.strip(), "!")
except exc.RateLimitExceeded:
    sys.exit("API rate limit exceeded. Please retry in a bit.")

# EFFORT DATA

# prepare data collection
import pandas as pd
# start/end index might be usefull later to identify effort position in activity stream
df = pd.DataFrame(columns=['start_index','end_index','dist','avg_grade','date','avg_power','duration'])
# stop at API rate limit
try:
    # work on last activity only
    activities = client.get_activities(limit=1) # request last activity only
except exc.RateLimitExceeded:
    sys.exit("API rate limit exceeded. Please retry in a bit.")

# work on last activity only
act_summary = next(activities)
# stop at API rate limit
try:
    # https://developers.strava.com/docs/reference/#api-models-SummaryActivity
    act_detail = client.get_activity(act_summary.id, include_all_efforts="true")
except exc.RateLimitExceeded:
    sys.exit("API rate limit exceeded. Please retry in a bit.")

# https://developers.strava.com/docs/reference/#api-models-DetailedActivity
# store activity summary
with open(str(act_summary.id) + ".txt", "w") as text_file:
    print("Name: {}".format(act_summary.name), file=text_file)
    print("Date: {}".format(act_summary.start_date_local), file=text_file)
    print("Dist: {} km".format(str(act_summary.distance / 1000).split(' ')[0]), file=text_file)
    print("Elevation: {}".format(act_summary.total_elevation_gain), file=text_file)
    print("Time: {}".format(act_summary.moving_time), file=text_file)
    print("Speed: {} km/h".format(str(act_summary.average_speed * 3.6).split(' ')[0]), file=text_file)
    print("Power: {} w".format(act_summary.average_watts), file=text_file)

# get segment efforts
for seg_effort_summary in act_detail.segment_efforts:
    try:
        # https://developers.strava.com/docs/reference/#api-models-SummarySegmentEffort
        seg_effort_detail = client.get_segment_effort(seg_effort_summary.id)
        # https://developers.strava.com/docs/reference/#api-models-DetailedSegmentEffort
        # https://developers.strava.com/docs/reference/#api-models-SummarySegment
        seg_summary = client.get_segment(seg_effort_detail.segment.id)
        df.loc[len(df)] = [
            seg_effort_detail.start_index, seg_effort_detail.end_index, 
            seg_effort_summary.distance,
            seg_summary.average_grade,
            seg_effort_detail.start_date_local,
            seg_effort_detail.average_watts,
            seg_effort_detail.elapsed_time
        ]
    except:
        print ("API rate limit exceeded. Data collected until now will be persisted.")
        break

# result overview and serialization
print(df)
df.to_pickle(str(act_summary.id) + '.pkl')
