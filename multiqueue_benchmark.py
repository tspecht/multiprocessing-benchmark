import hashlib
import time
from multiprocessing import Process, Queue, Event, active_children
from Queue import Empty, Full
from threading import Thread

from benchmark import Benchmark
from utilities import put_into_queue_until_done, get_next_object_and_iterator, random_query, Counter

def process(input_queue, output_queue, should_terminate_event, counter):
	count = 0
	while True:
		try:
			# get the query
			query = input_queue.get(True, 0.01)

			# do some dummy calculation
			result = hashlib.md5(query).hexdigest()

			# write it back to the output queue
			output_queue.put(result)

			if count%2000 == 0:
				time.sleep(0.8)

			count += 1
		except Empty, e:
			if should_terminate_event.is_set():
				break
	counter.increment()

def fill_queue(queues, should_terminate_event, context):
	iterator = None

	for i in range(context['query_number']):
		queue, iterator = get_next_object_and_iterator(queues, iterator)
		queue.put(random_query())

	# signal that workers should terminate
	should_terminate_event.set()


class MultiQueueBenchmark(Benchmark):

	def run(self):
		# create general data structures and queues
		input_queues = [Queue() for i in range(self.context['number'])]
		output_queues = [Queue() for i in range(self.context['number'])]
		processes = []

		# create the should terminate event
		event = Event()

		finished_slaves_counter = Counter()

		# spawn the processes
		for i in range(self.context['number']):
			p = Process(target=process, args=[input_queues[i], output_queues[i], event, finished_slaves_counter])
			p.start()
			processes.append(p)

		# get the queries and push them into the queue
		queue_thread = Process(target=fill_queue, args=[input_queues, event, self.context])
		queue_thread.start()

		# empty the output_queue
		result_count = 0

		iterator = None
		queue, iterator = get_next_object_and_iterator(output_queues, iterator)

		num_result_count_unchanged = 0
		while True:
			try:
				result = queue.get_nowait()
				
				# do some dummy calculation
				hashlib.md5(result).hexdigest()

				result_count += 1
				num_result_count_unchanged = 0
			except Empty, e:
				queue, iterator = get_next_object_and_iterator(output_queues, iterator)

				num_result_count_unchanged += 1
				if finished_slaves_counter.value() == self.context['number'] and (result_count == self.context['query_number'] or num_result_count_unchanged >= 100):
					break

				#time.sleep(0.001)

		# close
		for queue in input_queues:
			queue.close()

		for queue in output_queues:
			queue.close()

		queue_thread.join()

		print "Processed %d results" % result_count

		# join the processes
		for p in processes:
			p.join()
