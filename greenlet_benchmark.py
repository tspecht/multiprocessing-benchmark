import hashlib
import gevent
from gevent.pool import Pool

from benchmark import Benchmark
from utilities import random_query

def work(queries):
	for query in queries:
		hashlib.md5(query).hexdigest()

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