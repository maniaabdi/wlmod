#!/usr/bin/python3
import pandas as pd
import datetime
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

df = pd.read_csv('../conf.csv')
print(df.columns)


df['submitTime'] = df['submitTime'].astype('int64') 
df['submitTimeSec'] = df['submitTime']//1000
df['DateTime'] = [datetime.datetime.fromtimestamp(d) for d in df["submitTimeSec"]]
df['Date'] = [datetime.datetime.date(d) for d in df['DateTime']] 
df['Hour'] = [d.hour for d in df['DateTime']]
df['Min'] = [d.minute for d in df['DateTime']]
df['Sec'] = [d.second for d in df['DateTime']]

print('Maximum runtime:', df['runTime'].max()//(3600*1000), 'hour, Minimum runtime:' , df['runTime'].min()//1000, 'sec')
print('Maximum queueTime:', df['queueTime'].max()//(60*1000), 'min, Minimum queueTime:' , df['queueTime'].min()//1000, 'sec')
print(df.head)


df1 = df.groupby(['Date', 'Hour']).size().reset_index(name='counts')
hours = df1.index.values
submit_per_hour = df1['counts'].values

df2 = df.groupby(['Date', 'Hour', 'Min', 'Sec']).size().reset_index(name='counts')
secs = df2.index.values
submit_per_sec = df2['counts'].values
print(df2.head)


# Data for plotting
t = hours
s = submit_per_hour
fig, ax = plt.subplots(figsize= (8, 3))
ax.grid(False)

print(min(s), max(s))
min_t = int(min(t))
ax.plot(t, s, color='purple', linewidth=2.5)

ax.set_xlabel('time (hour)', fontsize=16)
ax.set_ylabel('# submitted jobs (ph)', fontsize=16)
ax.tick_params(axis='both', which='major', labelsize=12)

plt.locator_params(axis='x', nbins=10)
plt.subplots_adjust(left=0.1, bottom=0.17, right=0.98, top=0.97)
plt.savefig('jobsubmission_ph.pdf', format='pdf', dpi=200)
plt.savefig('jobsubmission_ph.png', format='png', dpi=200)

plt.show()

def format_xticks(x, pos=None):
    return str(int(x))

# Data for plotting
t = secs
s = submit_per_sec
fig, ax = plt.subplots(figsize= (8, 3))
print(min(s), max(s))
min_t = int(min(t))
ax.plot(t, s, color='purple', linewidth=0.3)

ax.set_xlabel('time (sec)', fontsize=16)
ax.set_ylabel('# submitted jobs (ps)', fontsize=16)
ax.tick_params(axis='both', which='major', labelsize=12)

ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_xticks))
plt.locator_params(axis='x', nbins=10)
plt.subplots_adjust(left=0.1, bottom=0.17, right=0.98, top=0.97)
plt.savefig('jobsubmission_ps.pdf', format='pdf', dpi=200)
plt.savefig('jobsubmission_ps.png', format='png', dpi=200)

plt.show()
