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
      # The last line might be abruptly cut off.
      # It's fine to leave the last line out because we
      # just have a long-running monitoring process that
      # we kill loosely some time after we execute BenchBase.
      try:
        df_vals.append(pd.Series(_json_flatten(json.loads(line))))
      except json.decoder.JSONDecodeError:
        try:
          next(f)
          assert False
        except StopIteration:
          pass
  return _augment_stats(pd.DataFrame(df_vals), cpu)


def parse_replay_lag(fpath, b):
  df = pd.read_csv(fpath, sep='|')
  df['benchmark'] = str(b)
  df['elapsed_time'] = pd.Series(range(len(df)))
  return df


def get_benchmark(df, benchmark):
  return df[df['benchmark'] == str(benchmark)]


def plot_metrics(benchmarks, plotters, primary_stats, replica_stats):
  for filename, attribute, ylabel in plotters:
    fig, ax1 = plt.subplots(figsize=(16,6))
    ax2 = ax1.twinx()
    # ax1.ticklabel_format(style='plain', useOffset=False)
    # ax2.ticklabel_format(style='plain', useOffset=False)

    for benchmark in benchmarks:
      get_benchmark(primary_stats, benchmark).plot(x='elapsed_time', y=attribute, ax=ax1, xlabel='Elapsed Time ($s$)', ylabel=f'{ylabel}', label=f'Primary {benchmark}', color='tab:orange')
      get_benchmark(replica_stats, benchmark).plot(x='elapsed_time', y=attribute, ax=ax2, xlabel='Elapsed Time ($s$)', ylabel=f'{ylabel}', label=f'Replica {benchmark}', color='tab:cyan')
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    lines, labels = lines1 + lines2, labels1 + labels2
    ax1.legend(lines, labels, loc=0)
    ax2.get_legend().remove()

    fig.tight_layout()
    plt.title(f'{ylabel}')
    plt.savefig(filename, bbox_inches='tight')


def plot_replay_lag(benchmarks, replay_lag, replay_lag_col, replay_lag_label):
  fig, ax = plt.subplots(1, 1, figsize=(16,6))
  ax.ticklabel_format(style='plain', useOffset=False)
  for benchmark in benchmarks:
    get_benchmark(replay_lag, benchmark)[replay_lag_col].plot(xlabel='Elapsed Time (s)', ylabel=replay_lag_label, label=f'{benchmark}', color='tab:orange')
  plt.legend()
  plt.title(replay_lag_label)
  plt.savefig('replay_lag.png', bbox_inches='tight')

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
  benchmarks = ['smallbank']
  primary_stats_files = [(f'{args.data_files}/{benchmark}_docker_monitoring_primary.txt', benchmark) for benchmark in benchmarks]
  replica_stats_files = [(f'{args.data_files}/{benchmark}_docker_monitoring_replica.txt', benchmark) for benchmark in benchmarks]
  replay_lag_files = [(f'{args.data_files}/{benchmark}_replication_lag.txt', benchmark) for benchmark in benchmarks]

  primary_stats = pd.concat([parse_docker_stats(f, b) for f, b in primary_stats_files], ignore_index=True)
  replica_stats = pd.concat([parse_docker_stats(f, b) for f, b in replica_stats_files], ignore_index=True)
  replay_lag = pd.concat([parse_replay_lag(f, b) for f, b in replay_lag_files], ignore_index=True)

  print(primary_stats.columns.values)
  sys.exit(0)

  plotters = [
    ('io_read.png', 'IO_DiffBytes_Read', r'Read I/O (bytes)'),
    ('io_write.png', 'IO_DiffBytes_Write', r'Write I/O (bytes)'),
    ('io_read_util.png', 'IO_Read_Util', r'Read I/O Util %'),
    ('io_write_util.png', 'IO_Write_Util', r'Write I/O Util %'),
    ('io_total.png', 'IO_DiffBytes_Total', r'Total I/O (bytes)'),
    ('cpu.png', 'cpu_usage_%', r'CPU Usage %'),
    ('memory.png', 'memory_usage_%', r'Memory Usage %'),
  ]

  def transform():
    replay_lag['Replay Lag (seconds)'] = replay_lag['Replay Lag (microseconds)'] / 1e6
    for stats in [primary_stats, replica_stats]:
      # SAMSUNG MZQLB960HAJR-00007
      # Max Seq Read 3000 MB/s, Seq Write 1050 MB/s
      stats['IO_Read_Util'] = stats['IO_DiffBytes_Read'] / 3000e6 * 100
      stats['IO_Write_Util'] = stats['IO_DiffBytes_Write'] / 1050e6 * 100

  transform()

  plot_metrics(benchmarks, plotters, primary_stats, replica_stats)
  plot_replay_lag(benchmarks, replay_lag, 'Replay Lag (seconds)', 'Replay Lag ($s$)')


if __name__ == '__main__':
  main()
