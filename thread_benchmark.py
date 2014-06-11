import pickle
import hashlib
from threading import Thread

from benchmark import Benchmark

def thread(queries):
	for query in queries:
		hashlib.md5(query).hexdigest()

class ThreadBenchmark(Benchmark):

	def run(self):
		threads = []
		queries = []

		for line in open(self.context['filename'], 'r'):
			if len(queries) >= self.context['query_number']:
				break
			queries.append(line)

		queries_per_thread = len(queries)/self.context['number']
		for i in range(self.context['number']):
			t = Thread(target=thread, args=[queries[i*queries_per_thread: (i+1)*queries_per_thread],])
			t.start()
			threads.append(t)

		for t in threads:
			t.join()