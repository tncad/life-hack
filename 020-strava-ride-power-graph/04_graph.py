import pandas as pd

# read and filter previously serialized data
df = pd.read_pickle("./efforts.pkl")
df = df[(df.avg_grade > -3) & (df.avg_grade < 24)]
print(df.head())

import matplotlib.pyplot as plt

# draw bar chart of power per elevation grade
plt.bar(df['avg_grade'],df['avg_power'], 0.2, alpha=0.5)
plt.xlabel('Grade (%)')
plt.ylabel('Power (W)')
plt.title('Activity efforts')
plt.show()
