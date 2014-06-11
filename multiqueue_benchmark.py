import hashlib
import time
from multiprocessing import Process, Queue, Event, active_children
from Queue import Empty, Full
from threading import Thread

from benchmark import Benchmark
from utilities import put_into_queue_until_done, get_next_object_and_iterator

def process(input_queue, output_queue, should_terminate_event, processing_finished_event):
	while True:
		try:
			# get the query
			query = input_queue.get(False)

			# do some dummy calculation
			result = hashlib.md5(query).hexdigest()

			# write it back to the output queue
			put_into_queue_until_done(output_queue, result)
		except Empty, e:
			if should_terminate_event.is_set():
				processing_finished_event.set()
				break

def fill_queue(filename, queues, should_terminate_event, context):
	iterator = None

	line_count = 0
	for line in open(filename, 'r'):
		if line_count >= context['query_number']:
				break
		queue, iterator = get_next_object_and_iterator(queues, iterator)
		put_into_queue_until_done(queue, line)
		line_count += 1

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
		event2 = Event()

		# spawn the processes
		for i in range(self.context['number']):
			p = Process(target=process, args=[input_queues[i], output_queues[i], event, event2])
			p.name = "Slave %d" % i
			p.start()
			processes.append(p)

		# get the queries and push them into the queue
		queue_thread = Process(target=fill_queue, args=[self.context['filename'], input_queues, event, self.context])
		queue_thread.name = "Queue process"
		queue_thread.start()

		# empty the output_queue
		exc_count = 0
		result_count = 0

		iterator = None
		while True:
			try:
				queue, iterator = get_next_object_and_iterator(output_queues, iterator)
				result = queue.get_nowait()
				
				# do some dummy calculation
				hashlib.md5(result).hexdigest()

				result_count += 1
				exc_count = 0
			except Empty, e:
				exc_count += 1

				# ugly fix, don't know why this occurs
				if exc_count > 50000:
					break

				if len(active_children()) == 0:
					break
		# close the output_queue
		for queue in output_queues:
			queue.close()

		queue_thread.terminate()

		print "Processed %d results" % result_count

		# join the processes
		for p in processes:
			p.terminate()
