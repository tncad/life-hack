# global params
min_grade = -4
max_grade = 20
x_label = "Grade (%)"
y_label = "Power (W)"
pkl_file = "./efforts.pkl"

# read and filter previously serialized data
import pandas as pd
df = pd.read_pickle(pkl_file)[['avg_grade','avg_power']]
df1 = df[(df.avg_grade > min_grade) & (df.avg_grade < max_grade)]
df2 = df1.groupby(['avg_grade'])['avg_power'].agg('max').reset_index()

# create figure
import matplotlib.pyplot as plt
fig,a = plt.subplots(2)
fig.suptitle('Strava segment power analysis', fontsize=12)

# basic plots
#a[0].set_title("Raw data", fontsize=10)
#a[0].plot(df1['avg_grade'], df1['avg_power'],"b.")

# lazy analysis
import seaborn as sns
a[0].set_title("Boxplot distribution", fontsize=10)
#ax = sns.boxplot(x='avg_grade',y='avg_power',data=df1[['avg_grade','avg_power']], ax=a[0])
#ax.set_xticklabels(ax.get_xticklabels(),rotation=90)
a[0].set_title("Mean expectation", fontsize=10)
sns.regplot(x='avg_grade',y='avg_power',data=df1[['avg_grade','avg_power']], ax=a[0])

# bars
a[1].set_title("Fit to Max", fontsize=10)
ax = a[1].bar(df2['avg_grade'],df2['avg_power'], 0.2, alpha=0.5)

# prepare fitting
import numpy as np
import numpy.polynomial.polynomial as poly
df2_x_np = df2['avg_grade'].to_numpy()
df2_y_np = df2['avg_power'].to_numpy()

# linear fit
lin_coefs = poly.polyfit(df2_x_np, df2_y_np, 1)
x = np.arange(df2_x_np.min(), df2_x_np.max(), 0.1)
df2_f_np = poly.polyval(x, lin_coefs)
a[1].plot(x, df2_f_np, 'y', label="linear")

# logarithmic fit
x = df2_x_np - df2_x_np.min() + 1
log_coefs = poly.polyfit(np.log(x), df2_y_np, 1)
df2_f_np = poly.polyval(np.log(x), log_coefs)
#y = df2_f_np(np.log(x))
a[1].plot(df2_x_np, df2_f_np, 'r', label="logarithmic")

# gaussian fit
import numpy.polynomial.polynomial as poly
gaus_coefs = poly.polyfit(df2_x_np, df2_y_np, 3)
x = np.arange(df2_x_np.min(), df2_x_np.max(), 0.1)
df2_f_np = poly.polyval(x, gaus_coefs)
a[1].plot(x, df2_f_np, 'g', label="gaussian")

# annotate axes
plt.xlabel(x_label)
plt.ylabel(y_label)
plt.legend()
plt.show()
