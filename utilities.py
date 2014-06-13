from Queue import Empty, Full
from multiprocessing import Value, Lock

class Counter(object):
    def __init__(self, initval=0):
        self.val = Value('i', initval)
        self.lock = Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    def value(self):
        with self.lock:
            return self.val.value

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

	if iterator is None:
		object = objects[0]
	else:
		try:
			object = object_iterator.next()
		except Exception, e:
			object_iterator = iter(objects)
			object = objects[0]

	return object, object_iterator

def random_query():
	return """1400660482: SELECT TOP 1 "MANDT" , "BUKRS" , "BELNR" , "GJAHR" , "BLART" , "BLDAT" , "BUDAT" , "MONAT" , "CPUDT" , "CPUTM" , "AEDAT" , "UPDDT" , "WWERT" , "USNAM" , "TCODE" , "BVORG" , "XBLNR" , "DBBLG" , "STBLG" , "STJAH" , "BKTXT" , "WAERS" , "KURSF" , "KZWRS" , "KZKRS" , "BSTAT" , "XNETB" , "FRATH" , "XRUEB" , "GLVOR" , "GRPID" , "DOKID" , "ARCID" , "IBLAR" , "AWTYP" , "AWKEY" , "FIKRS" , "HWAER" , "HWAE2" , "HWAE3" , "KURS2" , "KURS3" , "BASW2" , "BASW3" , "UMRD2" , "UMRD3" , "XSTOV" , "STODT" , "XMWST" , "CURT2" , "CURT3" , "KUTY2" , "KUTY3" , "XSNET" , "AUSBK" , "XUSVR" , "DUEFL" , "AWSYS" , "TXKRS" , "LOTKZ" , "XWVOF" , "STGRD" , "PPNAM" , "BRNCH" , "NUMPG" , "ADISC" , "XREF1_HD" , "XREF2_HD" , "XREVERSAL" , "REINDAT" , "RLDNR" , "LDGRP" , "PROPMANO" , "XBLNR_ALT" , "VATDATE" , "DOCCAT" , "XSPLIT" , "CASH_ALLOC" , "FOLLOW_ON" , "XREORG" , "SUBSET" , "KURST" , "KURSX" , "KUR2X" , "KUR3X" , "XMCA" , "_DATAAGING" , "YYYTYPE" , "YYYAMNT" , "YYYPERC" , "ZVAT_INDC" , "PSOTY" , "PSOAK" , "PSOKS" , "PSOSG" , "PSOFN" , "INTFORM" , "INTDATE" , "PSOBT" , "PSOZL" , "PSODT" , "PSOTM" , "FM_UMART" , "CCINS" , "CCNUM" , "SSBLK" , "BATCH" , "SNAME" , "SAMPLED" , "EXCLUDE_FLAG" , "BLIND" , "OFFSET_STATUS" , "OFFSET_REFER_DAT" , "PENRC" , "KNUMV" FROM "BKPF" WHERE "MANDT" = '001' AND "BUKRS" = '0001' AND "BELNR" = '0910554226' AND "GJAHR" = '2014';"""