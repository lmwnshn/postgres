import matplotlib.pyplot as plt
import pandas as pd


cpus = [
  #6,3,1,0.75,0.5,0.25,0.2,0.15,
  0.1,0.09,0.08,0.07,0.06,0.05,0.045,0.04,0.035,0.03,0.025,0.02,0.015,0.01,
  ]

data = {
  'mean': {},
  '10': {},
  '25': {},
  '50': {},
  '75': {},
  '90': {},
  '95': {},
  '99': {},
}
for cpu in cpus:
  x = pd.read_csv(f"./cmudb/experiments/ha/replay_lag_{cpu}.txt", delimiter='|')
  data['mean'][str(cpu * 100 // 1) + "%"] = x['Replay Lag (microseconds)'].mean() / 1e3
  for percentile in [10,25,50,75,90,95,99]:
    data[str(percentile)][str(cpu * 100 // 1) + "%"] = x['Replay Lag (microseconds)'].quantile(percentile/100) / 1e3

df = pd.DataFrame(data)
df[::-1].plot(xlabel='CPU allowed', ylabel='Replay Lag (milliseconds)', grid=True)
plt.show()
