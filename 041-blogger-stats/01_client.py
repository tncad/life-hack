import os #, imp
import requests, json, bs4 
from bs4 import BeautifulSoup
import pandas as pd, nltk
from nltk.tokenize import word_tokenize
nltk.download('punkt', download_dir='./')

# init parameters inline or from secret.py
baseUrl = 'https://www.googleapis.com/blogger/v3/blogs'
try:
    #imp.find_module('secret')
    import secret
    blogId = secret.BLOG_ID
    apiKey = secret.API_KEY
except ImportError:
  blogId = "<Your Blog ID>"
  apiKey = "<Your API Key>"
common_params = {'key': apiKey, 'fetchImages':'false'}
params = common_params
params.update( {'fetchBodies':'false','maxResults':'100'} )

# api extract headers
r = requests.get( baseUrl + '/' + blogId + '/posts', params=params )
if r.status_code == requests.codes.ok:
  postDic = r.json()
else:
  r.raise_for_status()
  exit

# prepare dataframe and output file
df = pd.DataFrame(columns=['num','date','wc'])
if os.path.exists(blogId + '.dat'):
  os.remove(blogId + '.dat')
f = open(blogId + '.dat', 'w')

# api extract bodies
for postItem in postDic['items']:
  r = requests.get( postItem['selfLink'], params=common_params )
  if r.status_code == requests.codes.ok:
    postDic = r.json()
  else:
    r.raise_for_status()
  exit
  raw = BeautifulSoup(postDic['content'], features="html.parser").get_text('\n').replace('\n',' ')
  wc = len(word_tokenize(raw))
  df.loc[len(df)] = [ postItem['title'][0:3], postItem['published'], wc ]
  f.write("%s\n" % raw)
print(df)

# serialize metadata
df.to_pickle(blogId + '.pkl')
