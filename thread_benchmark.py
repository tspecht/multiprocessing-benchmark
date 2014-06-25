import pickle
import time
import hashlib
from threading import Thread

from benchmark import Benchmark
from utilities import random_query

def thread(queries):
	for query in queries:
		hashlib.md5(query).hexdigest()
		time.sleep(0.005)

class ThreadBenchmark(Benchmark):

	def run(self):
		threads = []
		queries = []

		for i in range(self.context['query_number']):
			queries.append(random_query())

		queries_per_thread = len(queries)/self.context['number']
		for i in range(self.context['number']):
			t = Thread(target=thread, args=[queries[i*queries_per_thread: (i+1)*queries_per_thread],])
			t.start()
			threads.append(t)

		for t in threads:
			t.join()