#
# Initialisation
#

# presets
pkl_path = "./"
min_grade = -5
max_grade = 25
x_label = "Grade (%)"
y_label = "Power (W)"
df = None

# fetch and read all pkl files
import pandas as pd
from os import listdir
from os.path import isfile, join
for pkl_file in listdir(pkl_path):
  if isfile(join(pkl_path, pkl_file)) & pkl_file.endswith(".pkl"):
    print(pkl_file)
    if df is None:
      df = pd.read_pickle(pkl_file) 
    else: 
      df = pd.concat([df, pd.read_pickle(pkl_file)])

# restrict observation to domain of interest
df = df[(df.avg_grade > min_grade) & (df.avg_grade < max_grade)] # excluding wrong segment grade of 30%
df = df[(df.avg_grade < 7) | (df.avg_power > 100)] # excluding irrelevant effort of 50W / 8%

# use same value range for all diagrams
min_grade = df['avg_grade'].min()
max_grade = df['avg_grade'].max()
min_power = df['avg_power'].min()
max_power = df['avg_power'].max()

import numpy as np
x_data = np.linspace(min_grade, max_grade, num=100)

#
# Distribution analysis (overall)
#

import matplotlib.pyplot as plt
import seaborn as sns

plt.title("Distribution analysis (overall)")
plt.xlabel(x_label)
plt.ylabel(y_label)
plt.xlim(min_grade, max_grade)
plt.ylim(min_power, max_power)
ax = sns.boxplot(x='avg_grade',y='avg_power',data=df.round({'avg_grade': 0}))
ax.set_xticklabels(ax.get_xticklabels(),rotation=90)
plt.show()

#
# Distribution analysis (by category of effort)
#

import datetime
df['category'] = ["anaerobic" if d < datetime.timedelta(minutes=2) else "aerobic" for d in df['duration']]

plt.title("Distribution analysis (by category of effort)")
plt.xlabel(x_label)
plt.ylabel(y_label)
plt.xlim(min_grade, max_grade)
plt.ylim(min_power, max_power)
ax = sns.boxplot(x='avg_grade',y='avg_power',data=df.round({'avg_grade': 0}), hue='category')
ax.set_xticklabels(ax.get_xticklabels(),rotation=90)
plt.show()

#
# Linear fitting
#

import numpy.polynomial.polynomial as poly

# create figure
fig,a = plt.subplots(2)
fig.suptitle('Linear fitting', fontsize=12)

# anaerobic: reference data
df_x_np = df[(df.category == 'anaerobic')]['avg_grade'].to_numpy()
df_y_np = df[(df.category == 'anaerobic')]['avg_power'].to_numpy()
a[0].scatter(df_x_np, df_y_np, color='lightgrey')
a[0].set_title("Anaerobic efforts", fontsize=10)
a[0].legend()
a[0].set_ylabel(y_label)
a[0].set_xlim(min_grade, max_grade)
a[0].set_ylim(min_power, max_power)

# anaerobic: plynomial fit
x = np.arange(df_x_np.min(), df_x_np.max(), 0.1)
for degree in range(1, 4, 1):
  coefs = poly.polyfit(df_x_np, df_y_np, degree)
  df_f_np = poly.polyval(x, coefs)
  a[0].plot(x, df_f_np, label="degree " + str(degree))
a[0].legend()

# aerobic: reference data
df_x_np = df[(df.category == 'aerobic')]['avg_grade'].to_numpy()
df_y_np = df[(df.category == 'aerobic')]['avg_power'].to_numpy()
a[1].scatter(df_x_np, df_y_np, color='lightgrey')
a[1].set_title("Aerobic efforts", fontsize=10)
a[1].set_xlabel(x_label)
a[1].set_ylabel(y_label)
a[1].set_xlim(min_grade, max_grade)
a[1].set_ylim(min_power, max_power)

# aerobic: polynomial fit
x = np.arange(df_x_np.min(), df_x_np.max(), 0.1)
for degree in range(1, 4, 1):
  coefs = poly.polyfit(df_x_np, df_y_np, degree)
  df_f_np = poly.polyval(x, coefs)
  a[1].plot(x, df_f_np, label="degree " + str(degree))
a[1].legend()

plt.legend()
plt.show()

#
# Non-linear fitting
#

from scipy.optimize import curve_fit

# keep best perf in case different exist for same seg or avg_grade
df = df.groupby(['category', 'avg_grade'])['avg_power'].agg('max').reset_index()

# generic logistic function as per https://stackoverflow.com/questions/60160803/scipy-optimize-curve-fit-for-logistic-function
def logifunc(x, A, x0, k, off):
    return A / (1 + np.exp( -k * (x - x0) ) ) + off

# create figure
fig,a = plt.subplots(2)
fig.suptitle('Non-linear fitting', fontsize=12)

# anaerobic: reference data
df_x_np = df[(df.category == 'anaerobic')]['avg_grade'].to_numpy()
df_y_np = df[(df.category == 'anaerobic')]['avg_power'].to_numpy()
a[0].scatter(df_x_np, df_y_np, color='lightgrey')
a[0].set_title("Anaerobic efforts", fontsize=10)
a[0].set_ylabel(y_label)
a[0].set_xlim(min_grade, max_grade)
a[0].set_ylim(min_power, max_power)

# anaerobic: logarithmic fit
x = df_x_np - df_x_np.min() + 1 
coefs = poly.polyfit(np.log(x), df_y_np, 1)
df_f_np = poly.polyval(np.log(x), coefs)
a[0].plot(df_x_np, df_f_np, 'y', label="logarithmic")

# anaerobic: logistic fit
popt, pcov = curve_fit(logifunc, df_x_np, df_y_np, p0=[df_y_np.max() - df_y_np.min(), 0, 0.1, df_y_np.min()])
a[0].plot(x_data, logifunc(x_data, *popt), 'r-', label='logistic')
a[0].legend()

# aerobic: reference data
df_x_np = df[(df.category == 'aerobic')]['avg_grade'].to_numpy()
df_y_np = df[(df.category == 'aerobic')]['avg_power'].to_numpy()
a[1].scatter(df_x_np, df_y_np, color='lightgrey')
a[1].set_title("Aerobic efforts", fontsize=10)
a[1].set_xlabel(x_label)
a[1].set_ylabel(y_label)
a[1].set_xlim(min_grade, max_grade)
a[1].set_ylim(min_power, max_power)

# aerobic: logarithmic fit
x = df_x_np - df_x_np.min() + 1
coefs = poly.polyfit(np.log(x), df_y_np, 1)
df_f_np = poly.polyval(np.log(x), coefs)
a[1].plot(df_x_np, df_f_np, 'y', label="logarithmic")

# aerobic: logistic fit
popt, pcov = curve_fit(logifunc, df_x_np, df_y_np, p0=[df_y_np.max() - df_y_np.min(), 0, 0.1, df_y_np.min()])
a[1].plot(x_data, logifunc(x_data, *popt), 'r-', label='logistic')
a[1].legend()

plt.legend()
plt.show()
