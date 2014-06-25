import pickle
import time
import hashlib
from threading import Thread

from benchmark import Benchmark
from utilities import random_query

def thread(queries):
	for i in range(len(queries)):
		hashlib.md5(queries[i]).hexdigest()
		if i%2000 == 0:
			time.sleep(0.8)

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