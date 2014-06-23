import hashlib
from multiprocessing import Process, Queue, Event, active_children
from Queue import Empty, Full
from threading import Thread

from benchmark import Benchmark
from utilities import put_into_queue_until_done, random_query, Counter

def process(input_queue, output_queue, should_terminate_event, counter):
	while True:
		try:
			# get the query
			query = input_queue.get(True, 0.01)

			# do some dummy calculation
			result = hashlib.md5(query).hexdigest()

			# write it back to the output queue
			output_queue.put(result)
		except Empty, e:
			if should_terminate_event.is_set():
				break

	counter.increment()

def fill_queue(queue, should_terminate_event, context):
	for i in range(context['query_number']):
		queue.put(random_query())

	# signal that workers should terminate
	should_terminate_event.set()

class QueueBenchmark(Benchmark):

	def run(self):
		# create general data structures and queues
		input_queue = Queue()
		output_queue = Queue()
		processes = []

		# create the should terminate event
		event = Event()

		finished_slaves_counter = Counter()

		# spawn the processes
		for i in range(self.context['number']):
			p = Process(target=process, args=[input_queue, output_queue, event, finished_slaves_counter])
			p.start()
			processes.append(p)

		# get the queries and push them into the queue
		queue_thread = Process(target=fill_queue, args=[input_queue, event, self.context])
		queue_thread.daemon = True
		queue_thread.start()

		# empty the output_queue
		result_count = 0
		num_result_count_unchanged = 0
		while True:
			try:
				result = output_queue.get_nowait()
				
				# do some dummy calculation
				hashlib.md5(result).hexdigest()

				result_count += 1
				num_result_count_unchanged = 0
			except Empty, e:

				num_result_count_unchanged += 1
				if finished_slaves_counter.value() == self.context['number'] and (result_count == self.context['query_number'] or num_result_count_unchanged >= 1000) and event.is_set():
					break

		input_queue.close()
		output_queue.close()

		# close the output_queue
		queue_thread.terminate()

		print "Processed %d results" % result_count

		# join the processes
		for p in processes:
			p.join()
