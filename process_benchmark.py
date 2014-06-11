import hashlib
from multiprocessing import Process

from benchmark import Benchmark

def process(queries):
	for query in queries:
		hashlib.md5(query).hexdigest()

class ProcessBenchmark(Benchmark):

	def run(self):
		processes = []
		queries = []

		for line in open(self.context['filename'], 'r'):
			if len(queries) >= self.context['query_number']:
				break
			queries.append(line)

		queries_per_process = len(queries)/self.context['number']
		for i in range(self.context['number']):
			p = Process(target=process, args=[queries[i*queries_per_process: (i+1)*queries_per_process],])
			p.start()
			processes.append(p)

		for p in processes:
			p.join()
