import argparse
import json
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _json_flatten(nested_json):
  result = {}

  def flatten(x, name=''):
    if isinstance(x, dict):
      for key in x:
        flatten(x[key], f'{name}{key}.')
    elif isinstance(x, list):
      for i, element in enumerate(x):
        flatten(element, f'{name}{i}.')
    else:
      result[name[:-1]] = x

  flatten(nested_json)
  return result


def _augment_stats(df, benchmark):
  # Time conversions.
  df.at[0, 'preread'] = pd.NA
  for col in ['read', 'preread']:
    df[col] = pd.to_datetime(df[col])

  # https://docs.docker.com/engine/api/v1.41/#operation/ContainerStats

  # Get container stats based on resource usage
  # This endpoint returns a live stream of a container’s resource usage statistics.
  # The precpu_stats is the CPU statistic of the previous read, and is used to calculate the CPU usage percentage. It is not an exact copy of the cpu_stats field.
  # If either precpu_stats.online_cpus or cpu_stats.online_cpus is nil then for compatibility with older daemons the length of the corresponding cpu_usage.percpu_usage array should be used.
  # On a cgroup v2 host, the following fields are not set
  #   blkio_stats: all fields other than io_service_bytes_recursive
  #   cpu_stats: cpu_usage.percpu_usage
  #   memory_stats: max_usage and failcnt Also, memory_stats.stats fields are incompatible with cgroup v1.
  # To calculate the values shown by the stats command of the docker cli tool the following formulas can be used:
  #   used_memory = memory_stats.usage - memory_stats.stats.cache
  #   available_memory = memory_stats.limit
  #   Memory usage % = (used_memory / available_memory) * 100.0
  #   cpu_delta = cpu_stats.cpu_usage.total_usage - precpu_stats.cpu_usage.total_usage
  #   system_cpu_delta = cpu_stats.system_cpu_usage - precpu_stats.system_cpu_usage
  #   number_cpus = length(cpu_stats.cpu_usage.percpu_usage) or cpu_stats.online_cpus
  #   CPU usage % = (cpu_delta / system_cpu_delta) * number_cpus * 100.0
  df['used_memory'] = df['memory_stats.usage'] - df['memory_stats.stats.cache']
  df['available_memory'] = df['memory_stats.limit']
  df['memory_usage_%'] = df['used_memory'] / df['available_memory'] * 100.0
  df['cpu_delta'] = df['cpu_stats.cpu_usage.total_usage'] - df['precpu_stats.cpu_usage.total_usage']
  df['system_cpu_delta'] = df['cpu_stats.system_cpu_usage'] - df['precpu_stats.system_cpu_usage']
  df['number_cpus'] = df['cpu_stats.online_cpus']
  df['cpu_usage_%'] = df['cpu_delta'] / df['system_cpu_delta'] * df['number_cpus'] * 100.0

  assert (df['blkio_stats.io_service_bytes_recursive.0.op'] == 'Read').all()
  assert (df['blkio_stats.io_service_bytes_recursive.1.op'] == 'Write').all()
  assert (df['blkio_stats.io_service_bytes_recursive.2.op'] == 'Sync').all()
  assert (df['blkio_stats.io_service_bytes_recursive.3.op'] == 'Async').all()
  assert (df['blkio_stats.io_service_bytes_recursive.4.op'] == 'Discard').all()
  assert (df['blkio_stats.io_service_bytes_recursive.5.op'] == 'Total').all()

  assert (df['blkio_stats.io_serviced_recursive.0.op'] == 'Read').all()
  assert (df['blkio_stats.io_serviced_recursive.1.op'] == 'Write').all()
  assert (df['blkio_stats.io_serviced_recursive.2.op'] == 'Sync').all()
  assert (df['blkio_stats.io_serviced_recursive.3.op'] == 'Async').all()
  assert (df['blkio_stats.io_serviced_recursive.4.op'] == 'Discard').all()
  assert (df['blkio_stats.io_serviced_recursive.5.op'] == 'Total').all()

  for i in range(0,6):
    assert len(set(df[f'blkio_stats.io_serviced_recursive.{i}.op'])) == 1
    assert len(set(df[f'blkio_stats.io_service_bytes_recursive.{i}.op'])) == 1
    assert set(df[f'blkio_stats.io_serviced_recursive.{i}.op']) == set(df[f'blkio_stats.io_service_bytes_recursive.{i}.op'])
    opname = df[f'blkio_stats.io_serviced_recursive.{i}.op'][0]
    df[f'IO_TotalRequests_{opname}'] = df[f'blkio_stats.io_serviced_recursive.{i}.value']
    df[f'IO_TotalBytes_{opname}'] = df[f'blkio_stats.io_service_bytes_recursive.{i}.value']
    df[f'IO_DiffRequests_{opname}'] = df[f'IO_TotalRequests_{opname}'].diff()
    df[f'IO_DiffBytes_{opname}'] = df[f'IO_TotalBytes_{opname}'].diff()

  df['benchmark'] = str(benchmark)
  df['elapsed_time'] = pd.Series(range(len(df)))

  return df


def parse_docker_stats(fpath, cpu):
  df_vals = []
  with open(fpath, 'r', encoding='utf8') as f:
    for line in f:
      df_vals.append(pd.Series(_json_flatten(json.loads(line))))
  return _augment_stats(pd.DataFrame(df_vals), cpu)


def parse_replay_lag(fpath, b):
  df = pd.read_csv(fpath, sep='|')
  df['benchmark'] = str(b)
  df['elapsed_time'] = pd.Series(range(len(df)))
  return df


def main():
  parser = argparse.ArgumentParser(description='Process the stats that were output by Docker.')
  parser.add_argument('data_files', help='Path to the data files to analyze.')
  parser.add_argument('--instructions', action='store_true', help='Print the instructions for gathering stats, and then quit.')
  args = parser.parse_args()

  if args.instructions:
    print('To capture docker stats, run the following command which will stream output to a file:')
    print(r'echo -ne "GET /containers/CONTAINER/stats HTTP/1.1\r\nHost: woof\r\n\r\n" | sudo nc -U /var/run/docker.sock | grep "^{" > docker_stats_CONTAINER.txt')
    sys.exit(0)

  benchmarks = ['smallbank', 'tatp', 'tpcc', 'ycsb']
  primary_stats_files = [(f'{args.data_files}/{benchmark}_docker_monitoring_primary.txt', benchmark) for benchmark in benchmarks]
  replica_stats_files = [(f'{args.data_files}/{benchmark}_docker_monitoring_replica.txt', benchmark) for benchmark in benchmarks]
  replay_lag_files = [(f'{args.data_files}/{benchmark}_replication_lag.txt', benchmark) for benchmark in benchmarks]

  primary_stats = pd.concat([parse_docker_stats(f, b) for f, b in primary_stats_files], ignore_index=True)
  replica_stats = pd.concat([parse_docker_stats(f, b) for f, b in replica_stats_files], ignore_index=True)
  replay_lag = pd.concat([parse_replay_lag(f, b) for f, b in replay_lag_files], ignore_index=True)

  def get_benchmark(df, benchmark):
    return df[df['benchmark'] == str(benchmark)]

  plotters = [
    ('IO_DiffBytes_Total', 'IO Total Bytes Per Sec'),
    ('cpu_usage_%', 'CPU Usage % Per Sec'),
    ('memory_usage_%', 'Memory Usage % Per Sec'),
  ]

  for attribute, ylabel in plotters:
    fig, ax1 = plt.subplots(figsize=(16,6))
    ax2 = ax1.twinx()

    colors = plt.cm.tab20(np.linspace(0, 1, len(benchmarks)*2))
    color_i = 0

    for benchmark in benchmarks:
      get_benchmark(primary_stats, benchmark).plot(x='elapsed_time', y=attribute, ax=ax1, ylabel=f'{ylabel} (Primary)', label=f'Primary {benchmark}', color=colors[color_i])
      get_benchmark(replica_stats, benchmark).plot(x='elapsed_time', y=attribute, ax=ax2, ylabel=f'{ylabel} (Replica)', label=f'Replica {benchmark}', color=colors[color_i+1])
      color_i += 2
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    lines, labels = lines1 + lines2, labels1 + labels2
    ax2.legend(lines, labels, loc=0)

    fig.tight_layout()
    plt.title(f'{ylabel} (Primary, Replica) for various benchmarks')

  fig, ax = plt.subplots(1, 1, figsize=(16,6))
  for benchmark in benchmarks:
    get_benchmark(replay_lag, benchmark)['Replay Lag (microseconds)'].plot(ylabel=f'Replay Lag (microseconds)', label=f'{benchmark}')
  plt.legend()
  plt.title('Replay Lag (microseconds)')

  plt.show()


if __name__ == '__main__':
  main()
