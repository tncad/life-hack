import pandas as pd, numpy as np
import matplotlib.pyplot as plt

# read and filter previously serialized data
df = pd.read_pickle("./8549843909067791036.pkl").sort_values(by=['num'], ascending=True)

# word count by post id
plt.bar(df['num'],df['wc'], 0.25)
plt.xlabel('Publish id')
plt.ylabel('Word count')
plt.title('Post length by id')
plt.xticks(
    np.arange(
  	start=int(df['num'].to_numpy().min()), 
        stop=int(df['num'].to_numpy().max()), 
	step=2
   ), 
   rotation=90)
plt.show()

# post by week
df['date'] = pd.to_datetime(df['date']) - pd.to_timedelta(7, unit='d')
df = df.groupby([pd.Grouper(key='date', freq='W-MON')]).count().reset_index().sort_values('date')
ax = plt.bar(df['date'],df['num'], 0.25)
plt.xlabel('Publish week')
plt.ylabel('Post count')
plt.title('Number of posts by week')
plt.xticks(
    rotation=45
)
plt.show()

# wordcloud imports as per https://www.datacamp.com/community/tutorials/wordcloud-python
from os import path
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator

# create stopword list
stopwords = set(STOPWORDS)
ObjRead = open("stopwords.txt", "r")
stopwords.update(ObjRead.read().replace('\n','').split(','))
ObjRead.close()

# generate word cloud image
ObjRead = open("8549843909067791036.dat", "r")
wordcloud = WordCloud(max_words=50,stopwords=stopwords, background_color="white").generate(ObjRead.read())
ObjRead.close()

# display the image
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis("off")
plt.show()
