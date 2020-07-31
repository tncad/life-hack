# setup program usage and parse arguments
import sys, argparse
parser = argparse.ArgumentParser()
parser.add_argument("--pickle_files", "-f", help="Coma separated list of pickle file(s) ex. --pickle_files=file1.pkl,file2.pkl")
args = parser.parse_args()
# terminate if mandatory argument is not set
if args.pickle_files is None:
    sys.exit("Please specify some pickle file(s) via --pickle_files (--help for Usage)")

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# read and filter serialized data
for filename in args.pickle_files.split(','):
    # read metadata file
    with open(filename.split('.')[0] + '.txt') as f:
        metadata = f.readlines()
        metadata = [x.strip() for x in metadata]
    # ride date is at line 2 word 2    
    activity_date = metadata[1].split(' ')[1]
    # read pickle file
    df = pd.read_pickle(filename)
    # filter wrong or non relevant elevation grades
    df = df[(df.avg_grade > -3) & (df.avg_grade < 24)]
    # add graph item
    sns.regplot(x='avg_grade', y='avg_power',
            data=df[['avg_grade','avg_power']], 
            label=activity_date, fit_reg=True)
plt.xlabel('Grade (%)')
plt.ylabel('Power (W)')
plt.title('Activity efforts')
plt.legend(loc='lower right')
plt.show()
