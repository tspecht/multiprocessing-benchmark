import hashlib
import time
import gevent
from gevent.pool import Pool

from benchmark import Benchmark
from utilities import random_query

def work(queries):
	for i in range(len(queries)):
		hashlib.md5(queries[i]).hexdigest()
		if i%2000 == 0:
			time.sleep(0.8)

class GreenletBenchmark(Benchmark):

	def run(self):
		queries = []

		for i in range(self.context['query_number']):
			queries.append(random_query())
		queries_per_thread = len(queries)/self.context['number']

		pool = Pool(size=self.context['number'])

		# create the tasks
		tasks = []
		for i in range(self.context['number']):
			tasks.append(pool.spawn(work, queries[i*queries_per_thread: (i+1)*queries_per_thread]))

		# wait for the tasks to finish
		gevent.joinall(tasks)