import dask.dataframe as dd
import time

dataframe = dd.read_csv('datasets/big_partitions_dask/*.csv')

time_start = time.perf_counter_ns()
dataframe = dataframe.compute()
dataframe.iloc[1000000]
time_end = time.perf_counter_ns()
time_miliseconds = (time_end - time_start) / 1e6
print(f"{time_miliseconds},")
