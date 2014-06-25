import hashlib
import time
from multiprocessing import Process

from benchmark import Benchmark
from utilities import random_query

def process(queries):
	for i in range(len(queries)):
		hashlib.md5(queries[i]).hexdigest()
		if i%2000 == 0:
			time.sleep(0.8)

class ProcessBenchmark(Benchmark):

	def run(self):
		processes = []
		queries = []

		for i in range(self.context['query_number']):
			queries.append(random_query())

		queries_per_process = len(queries)/self.context['number']
		for i in range(self.context['number']):
			p = Process(target=process, args=[queries[i*queries_per_process: (i+1)*queries_per_process],])
			p.start()
			processes.append(p)

		for p in processes:
			p.join()
