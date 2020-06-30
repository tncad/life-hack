import pandas as pd

# read and filter previously serialized data
df = pd.read_pickle("./efforts.pkl")
df = df[(df.avg_grade > -3) & (df.avg_grade < 24)]
print(df)

import matplotlib.pyplot as plt

# draw bar chart of power per elevation grade
plt.bar(df['avg_grade'],df['avg_power'], 0.25)
plt.xlabel('Grade (%)')
plt.ylabel('Power (W)')
plt.title('Activity efforts')
plt.show()

import seaborn as sns

sns.regplot(x='avg_grade',y='avg_power',data=df[['avg_grade','avg_power']], fit_reg=True)
plt.show()
