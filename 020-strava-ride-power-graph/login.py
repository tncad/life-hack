# PROGRAM ARGUMENTS
# setup program usage and parse arguments
import os, sys, argparse
parser = argparse.ArgumentParser()
help_client_id="Client ID (default: $STRAVA_CLIENT: " + str(os.environ.get('STRAVA_CLIENT')) + ")"
parser.add_argument("--client_id", "-i", help=help_client_id)
help_client_secret="Client Secret (default: $STRAVA_SECRET: " + str(os.environ.get('STRAVA_SECRET')) + ")"
parser.add_argument("--client_secret", "-s", help=help_client_secret)
parser.add_argument("--access_code", "-c", help="Access Code (default: None)")
help_refresh_token="Refresh Token (default: $STRAVA_REFRESH_TOKEN: " + str(os.environ.get('STRAVA_REFRESH_TOKEN')) + ")"
parser.add_argument("--refresh_token", "-t", help=help_refresh_token)
args = parser.parse_args()
# set default argument value if available as environment variables (see --help)
if args.client_id is None:
  args.client_id = os.environ.get('STRAVA_CLIENT')
  print("Set Strava client ID to %s" % args.client_id)
if args.client_secret is None:
  args.client_secret = os.environ.get('STRAVA_SECRET')
  print("Set Strava client Secret to %s" % args.client_secret)
if args.refresh_token is None:
  args.refresh_token = os.environ.get('STRAVA_REFRESH_TOKEN')
  print("Set Strava Refresh Token to %s" % args.refresh_token)
# check mandatory arguments (client_id, secret_key) and terminate as necessary
if args.client_id is None:
    sys.exit("Your input is invalid. Please specify a Client ID (--client_id) or read Usage (--help)")
if args.client_secret is None:
    sys.exit("Your input is invalid. Please specify a Client Secret (--client_secret) or read Usage (--help)")

# AUTHORIZATION FLOW
# get temporary access_code or access_token (depending on input parameters)
import requests, webbrowser
from stravalib import Client
client = Client()
# obtain a new access token if a refresh token is available
if args.refresh_token is not None:
    token_url = "https://www.strava.com/api/v3/oauth/token"
    token_opt = {'client_id': args.client_id, 'client_secret': args.client_secret, 'grant_type': 'refresh_token', 'refresh_token': args.refresh_token}
    x = requests.post(token_url, data = token_opt)
    print(x.text)
    sys.exit("You are ready to use app.py")
# have the user sign in to strava and authorize
if args.access_code is None:
    url = client.authorization_url(client_id=args.client_id,
        redirect_uri='http://127.0.0.1:5000/authorization')
    # authorize (you might have to login first)
    webbrowser.open(url, new=2)    
    args.access_code = input("Copy your authorization code from response URL here: ").strip()
# get access_token from access_code
if args.access_code is not None:
    access_dict = client.exchange_code_for_token(
        client_id=args.client_id,
        client_secret=args.client_secret,
        code=args.access_code
    )
    print(access_dict)
    os.putenv("STRAVA_ACCESS_TOKEN", access_dict['access_token'])
    os.putenv("STRAVA_REFRESH_TOKEN", access_dict['refresh_token'])
    print("You are ready to use app.py")
    os.system('bash') # just a tweak, creating a subprocess for re-using env
    sys.exit()
