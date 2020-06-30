# PROGRAM ARGUMENTS

# setup program usage and parse arguments
import os, sys, argparse, datetime as dt
parser = argparse.ArgumentParser()
parser.add_argument("--access_token", "-t", help="Access Token")
parser.add_argument("--batch_size", "-b", help="Limit activity batch size (default: 10)")
parser.add_argument("--filter_before", "-s", help="Filter activity before YYYY-MM-DD (default: None)")
parser.add_argument("--filter_after", "-e", help="Filter activity after YYYY-MM-DD (default: None)")
args = parser.parse_args()
# verify input argument formats
if args.filter_before:
    dt.datetime.strptime(args.filter_before, "%Y-%m-%d")
if args.filter_after:
    dt.datetime.strptime(args.filter_after, "%Y-%m-%d")
# set default argument value (see --help)
if not args.batch_size:
  args.batch_size = 10
# terminate if access token is not set
if not args.access_token:
    sys.exit("Please specify an Access Token (--access_token) or read Usage (--help)")

# connect to strava
from stravalib import Client, exc
# https://pythonhosted.org/stravalib/api.html#stravalib.client.Client
client = Client(access_token=args.access_token)
# verify token
try:
    athlete = client.get_athlete()
    print("Hello ", athlete.firstname.strip(), " ", athlete.lastname.strip(), "!")
# stop at wrong token
except exc.AccessUnauthorized:
    sys.exit("Your token is not valid. Please run login.py to get one.")
# stop at API rate limit
except exc.RateLimitExceeded:
    sys.exit("API rate limit exceeded. Please retry in a bit.")

# prepare data collection
import pandas as pd
# start/end index might be usefull later to identify effort position in activity stream
df = pd.DataFrame(columns=['start_index','end_index','dist','avg_grade','date','avg_power','duration'])
# stop at API rate limit
try:
    activities = client.get_activities(
            limit=int(args.batch_size), 
            after=args.filter_after, 
            before=args.filter_before
        )
except exc.RateLimitExceeded:
    sys.exit("API rate limit exceeded. Please retry in a bit.")

# handle process retry while informing the user at smaller intervals
import time, math
# after a fixed period of time (now replaced by retry_loop_next_quarter)
def retry_loop_fixed_period():
    wait = 60
    while wait > 0:
        print ("Auto-retry in " + str(wait) + "s...")
        time.sleep(10)
        wait -= 10
    return
# at the next quarter of an hour as currently handled by the api
# https://stackoverflow.com/questions/13071384/ceil-a-datetime-to-next-quarter-of-an-hour
def retry_loop_next_quarter():
    dt_now = dt.datetime.now()
    # how many secs have passed this hour
    nsecs = dt_now.minute*60 + dt_now.second + dt_now.microsecond*1e-6
    # number of seconds to next quarter hour mark
    delta = math.ceil(nsecs / 900) * 900 - nsecs
    # time + number of seconds to quarter hour mark
    dt_next_quarter = dt_now + dt.timedelta(seconds=delta)
    delta_sec = 1
    while delta_sec > 0:
        time.sleep(30)
        dt_diff = dt_next_quarter - dt.datetime.now()
        delta_sec = dt_diff.days * 86400 + dt_diff.seconds
        delta = divmod(delta_sec, 60)
        print ("Auto-retry in " + str(list(delta)[0]) + " min " + str(list(delta)[1]) + " sec...")
    return

# loop over activity list
for act_summary in activities:
    # https://developers.strava.com/docs/reference/#api-models-SummaryActivity
    if act_summary.type == 'Ride' and act_summary.name != 'Gravel': # filter road rides only
        # store activity summary
        with open(str(act_summary.id) + ".txt", "w") as text_file:
            print("Name: {}".format(act_summary.name), file=text_file)
            print("Date: {}".format(act_summary.start_date_local), file=text_file)
            print("Dist: {} km".format(str(act_summary.distance / 1000).split(' ')[0]), file=text_file)
            print("Elevation: {}".format(act_summary.total_elevation_gain), file=text_file)
            print("Time: {}".format(act_summary.moving_time), file=text_file)
            print("Speed: {} km/h".format(str(act_summary.average_speed * 3.6).split(' ')[0]), file=text_file)
            print("Power: {} w".format(act_summary.average_watts), file=text_file)
        # manage API rate limit
        while True:
            try:
                act_detail = client.get_activity(act_summary.id, include_all_efforts="true")
            except exc.RateLimitExceeded:
                print ("API rate limit exceeded.")
                retry_loop_next_quarter()
                continue
            break
        # https://developers.strava.com/docs/reference/#api-models-DetailedActivity
        for seg_effort_summary in act_detail.segment_efforts:
            # managed API rate limit
            while True:
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
                except exc.RateLimitExceeded:
                    print ("API rate limit exceeded.")
                    retry_loop_next_quarter()
                    continue
                break
        # result overview and serialization
        print(df)
        df.to_pickle(str(act_summary.id) + '.pkl')
