import hashlib
import time
from multiprocessing import Process, Pipe, Event, active_children
from threading import Thread

from benchmark import Benchmark
from utilities import get_next_object_and_iterator, random_query

def process(input_pipe, output_pipe, should_terminate_event):
	input_receiver, input_sender = input_pipe
	output_receiver, output_sender = output_pipe

	while True:
		try:
			# get the query
			if input_receiver.poll(0.5):
				query = input_receiver.recv()
			else:
				break

			# do some dummy calculation
			result = hashlib.md5(query).hexdigest()

			# write it back to the output queue
			output_sender.send(result)
		except EOFError, e:
			print e.__dict__
			if should_terminate_event.is_set():
				break

def fill_pipe(pipes, should_terminate_event, context):
	iterator = None

	for i in range(context['query_number']):
		pipe, iterator = get_next_object_and_iterator(pipes, iterator)

		pipe_receiver, pipe_sender = pipe
		pipe_sender.send(random_query())

	for pipe_receiver, pipe_sender in pipes:
		pipe_receiver.close()
		pipe_sender.close()

	# signal that workers should terminate
	should_terminate_event.set()


class MultiPipeBenchmark(Benchmark):

	def run(self):
		# create general data structures and queues
		input_connections = [Pipe() for i in range(self.context['number'])]
		output_connections = [Pipe() for i in range(self.context['number'])]
		processes = []

		# create the should terminate event
		event = Event()

		# spawn the processes
		for i in range(self.context['number']):
			p = Process(target=process, args=[input_connections[i], output_connections[i], event])
			p.start()
			processes.append(p)

		# get the queries and push them into the queue
		queue_thread = Process(target=fill_pipe, args=[input_connections, event, self.context])
		queue_thread.start()

		# empty the output_queue
		result_count = 0
		iterator = None

		exc_count = 0
		while True:
			try:
				pipe, iterator = get_next_object_and_iterator(output_connections, iterator)

				pipe_receiver, pipe_sender = pipe

				if pipe_receiver.poll(0.5):
					result = pipe_receiver.recv()
					exc_count = 0
				else:
					break
					# exc_count += 1
					# if exc_count >= 1:
					# 	break
					# else:
					# 	continue
				
				# do some dummy calculation
				hashlib.md5(result).hexdigest()

				result_count += 1
			except EOFError, e:
				break

		# close the output_queue
		for receiver, sender in output_connections:
			receiver.close()
			sender.close()

		queue_thread.terminate()
		event.wait()

		print "Processed %d results" % result_count

		# join the processes
		for p in processes:
			p.terminate()
