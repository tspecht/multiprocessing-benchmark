import hashlib
import gevent
from gevent.pool import Pool

from benchmark import Benchmark

def work(queries):
	for query in queries:
		hashlib.md5(query).hexdigest()

class GreenletBenchmark(Benchmark):

	def run(self):
		queries = []

		for line in open(self.context['filename'], 'r'):
			if len(queries) >= self.context['query_number']:
				break
			queries.append(line)
		queries_per_thread = len(queries)/self.context['number']

		pool = Pool(size=self.context['number'])

		# create the tasks
		tasks = []
		for i in range(self.context['number']):
			tasks.append(pool.spawn(work, queries[i*queries_per_thread: (i+1)*queries_per_thread]))

		# wait for the tasks to finish
		gevent.joinall(tasks)