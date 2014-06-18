from timeit import Timer
import numpy as np
import csv
import matplotlib.pyplot as plt
from collections import OrderedDict
import argparse
import os

from queue_benchmark import QueueBenchmark
from thread_benchmark import ThreadBenchmark
from process_benchmark import ProcessBenchmark
from greenlet_benchmark import GreenletBenchmark
from multiqueue_benchmark import MultiQueueBenchmark
from pipe_benchmark import MultiPipeBenchmark

TYPE_PROCESS = 0
TYPE_COMMUNICATION = 1

# slave_numbers = [2, 5, 10, 25, 50, 100]
slave_numbers = [10, 50, 100]
query_numbers = [100000, 1000000, 2000000]
# slave_numbers = [2, 50, 100]
# query_numbers = [10000, 100000, 1000000, 10000000, 25000000, 50000000]
# query_numbers = [1000, 10000, 100000]

def run_benchmark(Benchmark, context={}, type=TYPE_PROCESS):
    benchmark_key = str(Benchmark).split(".")[1]

    result = {benchmark_key: []}
    for i in slave_numbers:
        context['number'] = i
    
        for j in query_numbers:
            context['query_number'] = j

            benchmark = Benchmark(context)
            timer = Timer(lambda: benchmark.run())

            print "Executing Benchmark %s (%d slaves, %d queries) ..." % (benchmark_key, i, j)
            
            mean_execution_time = np.mean(timer.repeat(repeat=3, number=1))

            # print "%d slaves took avg. %f s" % (i, mean_execution_time)
            result[benchmark_key].append({'type': type, 'execution_time': mean_execution_time, 'slaves': i, 'queries': j})

    return result


def write_results_to_csv(benchmark_results):

    with open('results.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        
        # write the headers
        headers = ['Benchmark', 'Execution time', 'Type', 'Slaves', 'Queries']
        writer.writerow(headers)

        # write the results
        for benchmark, results in benchmark_results.items():
            for execution_result in results:
                writer.writerow([benchmark, execution_result['execution_time'], execution_result['type'], execution_result['slaves'], execution_result['queries']])
        csvfile.close()

def write_results_to_stdout(benchmark_results):
    for benchmark, results in benchmark_results.items():
        for result in results:
            print "%s (%d, %d): ~ %f" % (benchmark, result['slaves'], result['queries'], result['execution_time'])

def write_results_to_plots(benchmark_results):
    data = {}
    # sort the data first to be feasible for usage with pyplot
    for benchmark, results in benchmark_results.items():
        for result in results:
            # create the type content if not already there
            type = result['type']
            if type not in data:
                data[type] = OrderedDict()

            # handle by query count
            queries = result['queries']
            if queries not in data[type]:
                data[type][queries] = OrderedDict()

            # handle the benchmark itself
            if benchmark not in data[type][queries]:
                data[type][queries][benchmark] = []

            data[type][queries][benchmark].append(result['execution_time'])

    # print data

    # check if the export dir exists, if not, create
    if not os.path.exists('plots'):
        os.makedirs('plots')

    # actually plot the data
    for type, query_data in data.items():

        # gather the x- and y-values
        for query_number, execution_data in query_data.items():
            figure = plt.figure()

            ax = plt.subplot(111)
            
            for benchmark, benchmark_data in execution_data.items():
                ax.plot(slave_numbers, benchmark_data, label="%s" % benchmark)
            
            ax.set_xlabel("Number of slaves")
            ax.set_ylabel("Execution time (s)")
            box = ax.get_position()
            ax.set_position([box.x0, box.y0 + box.height * 0.1,
                             box.width, box.height * 0.9])

            # Put a legend below current axis
            ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
                      fancybox=True, ncol=2)

            plt.title("%d Queries" % query_number)

            plt.savefig('plots/%d_%d.png' % (type, query_number))

benchmark_results = {}

print "Starting Benchmark ..."

# thread benchmarks
benchmark_results.update(run_benchmark(ThreadBenchmark, type=TYPE_PROCESS))

# process benchmarks
benchmark_results.update(run_benchmark(ProcessBenchmark, type=TYPE_PROCESS))

# greenlet benchmarks
benchmark_results.update(run_benchmark(GreenletBenchmark, type=TYPE_PROCESS))

# queue benchmarks
benchmark_results.update(run_benchmark(QueueBenchmark, type=TYPE_COMMUNICATION))

# multiqueue benchmarks
benchmark_results.update(run_benchmark(MultiQueueBenchmark, type=TYPE_COMMUNICATION))

# # multipipe benchmarks
benchmark_results.update(run_benchmark(MultiPipeBenchmark, type=TYPE_COMMUNICATION))

print "Generating output ..."

write_results_to_csv(benchmark_results)
# write_results_to_stdout(benchmark_results)
write_results_to_plots(benchmark_results)
