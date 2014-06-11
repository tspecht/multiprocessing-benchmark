from Queue import Empty, Full

def put_into_queue_until_done(queue, data):
	has_sent = False
	while not has_sent:
		try:
			queue.put(data, False)
			has_sent = True
		except Full, e:
			pass

def get_next_object_and_iterator(objects, iterator=None):
	object = None
	object_iterator = iterator if iterator is not None else iter(objects)
	try:
		object = object_iterator.next()
	except Exception, e:
		object_iterator = iter(objects)
		object = object_iterator.next()

	return object, object_iterator