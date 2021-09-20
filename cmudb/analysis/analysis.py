import argparse
import json
import sys

import matplotlib.pyplot as plt
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


def _augment_stats(df):
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
  df['memory_usage_%'] = df['memory_stats.usage'] - df['memory_stats.stats.cache']
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

  return df


def parse_docker_stats(fpath):
  df_vals = []
  with open(fpath, 'r', encoding='utf8') as f:
    for line in f:
      df_vals.append(pd.Series(_json_flatten(json.loads(line))))
  return _augment_stats(pd.DataFrame(df_vals))


def main():
  parser = argparse.ArgumentParser(description='Process the stats that were output by Docker.')
  parser.add_argument('docker_stats_file', help='Path to the docker stats file to analyze.')
  parser.add_argument('--instructions', action='store_true', help='Print the instructions for gathering stats, and then quit.')
  args = parser.parse_args()

  if args.instructions:
    print('To capture docker stats, run the following command which will stream output to a file:')
    print(r'echo -ne "GET /containers/CONTAINER/stats HTTP/1.1\r\nHost: woof\r\n\r\n" | sudo nc -U /var/run/docker.sock | grep "^{" > docker_stats_CONTAINER.txt')
    sys.exit(0)

  df = parse_docker_stats(args.docker_stats_file)
  df.plot(x='read', y=['IO_DiffBytes_Read', 'IO_DiffBytes_Write', 'IO_DiffBytes_Total'])
  plt.show()


if __name__ == '__main__':
  main()
