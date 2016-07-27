"""
Using the importer module
=========================

The importer module provides an interface to import larger quantities of data relatively quickly and attempts to cover a range of file formats encountered in practice. In addition to the basic single-threaded :class:`Importer` class, it provides the  helper function :func:`import_with_threads` to ingest multiple files simultaneously (one file per thread), which should give some speed-up since the limitation is I/O both on the file and the database side (despite the :pydoc:`GIL <glossary.html#term-global-interpreter-lock>` - I *think*). 

.. _field_dict:

Configuration dictionaries ('field_dicts')
------------------------------------------

One of the arguments handed to the :class:`Importer` constructor is what I call a 'field_dict', a Python :obj:`dictionary <dict>` object containing the information needed to map each row of the input file to the :class:`Record` subtypes of the :ref:`database model <dbmodel>`. The keys of this dictionary are either :obj:`integers <int>` or :obj:`tuples <tuple>`, depending on whether the input file is of delimited (i.e., csv) or :ref:`fixed-width <fixedwidth>` type. Their associated values describe both the :ref:`time stamp <timestamp>` to be parsed and the :class:`Fields <databarc.schema.Field>` which should be added to the database as metadata objects associated with the imported records. This module contains a few :ref:`predefined field_dicts <prov_fdicts>`.

CSV-type files
^^^^^^^^^^^^^^

Here's an example for a csv file::
	
	# Note: it is the actual class that is passed with key 'type'
	from databarc.schema import Record_int
	
	# Note: 'mult' and 'missing value' are given as str
	field_dict_csv = {
		1 : 'year',
		2 : 'month',
		3 : 'day',
		4 : 'hour',
		5 : {'name':'temp', 'code':'t', 'unit':'deg C', 'mult':'0.1', 'type':Record_int},
		7 : {'name':'precip', 'code':'r', 'unit':'mm', 'mult':'0.1', 'type':Record_int, 'missing':'-9999'},
		8 : {'name':'snowcover', 'code':'r', 'unit':'cm', 'mult':'1', 'type':Record_int,
				'flags':[
					{'value':997,'desc':'less than 0.5 cm','in_data':True},
					{'value':998,'desc':'not continuous','in_data':True}
				]	
			}
	}

In this example, the second column contains the year of the record (remember, python uses 0--based indexing, i.e. here, the first column is ignored), the third the month and so forth. The last three columns contain the actual ':class:`records <databarc.schema.Record>`' to be imported, with each column becoming associated with a :class:`~databarc.schema.Field` instance. For those columns, the value in the dictionary is itself a dictionary, containing as keys the attribute names of the respective :class:`~databarc.schema.Field` that should be set (:attr:`~databarc.schema.Field.name`, :attr:`~databarc.schema.Field.unit` etc). 

There are three further keys that can or **must** be set:

``type`` : :class:`~databarc.schema.Record` subclass (required)
	the record type (e.g., :class:`~databarc.schema.Record_int`) to be used for the records in a column

``missing`` : :obj:`str` (optional)
	If a row/column contains this string, a corresponding value is **not** added to the database (uses the :mod:`re` module, so regular expressions are also supported)

.. _flags:

``flags`` : :obj:`list` (optional)
	a list of dictionaries describing :class:`~databarc.schema.Flag` objects to be added to the database

The 'flag' dictionaries, as with the :class:`~databarc.schema.Field` ones, contain keys of the same name as the :class:`~databarc.schema.Flag` object's attributes, i.e. :attr:`~databarc.schema.Flag.name`, :attr:`~databarc.schema.Flag.desc` and :attr:`~databarc.schema.Flag.in_data`, and their respective values. Attempts are made to match the list of flags with existing an :class:`~databarc.schema.Flag_collection` already in the database, and if none is found, a new one is created.

.. note::
	The value for the key ``'type'`` is a class. The value for the key ``'mult'`` should be a :obj:`str`, not a number, since it is used to initalize a :class:`decimal.Decimal` object (see :attr:`databarc.schema.Field.mult`).

.. _timestamp:

Parsing of time values
^^^^^^^^^^^^^^^^^^^^^^

The values for the 'time' columns can be any :obj:`str` of ``year, month, day, hour, minute, second, microsecond``, i.e., the keyword arguments to the :class:`datetime.datetime` constructor (``year, month, day`` are mandatory). There is also another, :ref:`more flexible way to define the parsing of time values <timecall>`.

.. _fixedwidth:

Fixed-width files
^^^^^^^^^^^^^^^^^

If the keys in the top-level dictionary are tuples, the file is **not** passed on to :func:`csv.reader`; instead, the tuples are used as slice-indexes for a fixed-width file::
	
	from databarc.schema import Record_float
	
	field_dict_fixed_width = {
		(13,17): 'year',
		(17,19): 'month',
		(19,21): 'day',
		(21,27): {'name':'temp', 'type':Record_float}
	}

In this case, characters 14--17 of each row represent the year, 18--19 the month and so on. In other words, if ``string`` is the :obj:`str` object representing a row in the file, ``(a,b)`` will be used to retrieve ``string[a:b]``. 

.. note::
	:obj:`int` and :obj:`tuple` keys use Python conventions --- ``0`` refers to first column in a csv-file, ``(0,2)`` refers to the slice ``0:2`` when applied to the :obj:`str` of a row in the input file, i.e. the interval ``[0,2)``

.. _timecall:

Parsing :class:`datetimes <datetime.datetime>` with callables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Lastly, other timestamp formats can be parsed by replacing the 'time' key-value pairs with a pair of the form ``'datetime' : callable``, where the callable takes a line of the file as input::

	field_dict_csv = {
		'datetime' : lambda s: datetime(*[int(s[i]) for i in range(4)]),
		4 : {'name' : 'temp, 'type': Record_num}
	}
	
	field_dict_csv = {
		'datetime' : lambda s: datetime(int(s[13:17]), int(s[17:19]), int(s[19:21])),
		(21,27) : {'name' : 'temp, 'type': Record_num}
	}

.. note:
	If a :class:`~databarc.schema.Field` with the given :attr:`~databarc.schema.Field.name`, :attr:`~databarc.schema.Field.source` and :attr:`~databarc.schema.Field.station_id` already exists in the datebase, that field is retrieved, and only records with a timestamp **later** than the latest record in the database will be added. This is intended as convenience in case an error occurs during importing, **or** if the database is updated periodically with files that contain **all** data. **It is assumed that the records in the file are in temporal order**.	
	
.. warning::	
	All parsing is done in :pydoc:`try...except <reference/compound_stmts.html#the-try-statement>` blocks. This means that, in order to be remain as general as possible, whatever cannot be parsed is simply ignored and does not upset the importer. In particular, header lines are expected to throw errors when parsed and hence are just ignored. In order to provide some basic check, errors that do **not** belong to some 'expected' set are logged to the file **'importer_parsing.log'** in the working directory. This 'expected' set contains :exc:`~databarc.schema.ValidationError`, raised during validation by the :class:`~databarc.schema.Record` subclasses (e.g. if :class:`~databarc.schema.Record_int` is instantiated with an :attr:`~databarc.schema.Record.x` argument that can't be made into an :obj:`int`), and :exc:`MissingValue`, raised internally by the importer when a value is recognized as equal to the 'missing' key in a 'field_dict'.
"""
import os, csv, logging
import sys
from re import compile
from copy import deepcopy
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import func
from threading import current_thread
from databarc.schema import Field, Record, Record_int, Record_float, Record_num, Flag, ValidationError
from databarc.utils import flags as uflags
from datetime import datetime



class Importer(object):
	"""
An instance of this class imports one file which can contain a number of columns representing time and measurement values. The columns can be mapped to :class:`~databarc.schema.Field` instances with associated records. Instantiated the class first, then call :meth:`do`.

:param session: A database session used for 1) checking if a similar field already exists in the database, and 2) eventually importing the data. If the multithreading helper :func:`import_with_threads` is used, should be a :class:`~sqla:sqlalchemy.orm.scoping.scoped_session` (as also returned by :func:`databarc.schema.session`).
:type session: :class:`~sqla:sqlalchemy.orm.session.Session`

:param str source: populates the :attr:`~databarc.schema.Field.source` attribute of the created fields

:param station_id: populates the :attr:`~databarc.schema.Field.station_id` attribute of the created fields
:type station_is: :obj:`int` or :obj:`str` that can be converted to :obj:`int`

:param dict field_dict: a dictionary describing the column-to-:class:`~databarc.schema.Field` mapping of the imported file, :ref:`described in the module docstring <field_dict>`

:param file file: an opened file handle

:param str delimiter: if the file is to be parsed by the :mod:`csv` module and the delimiter is not a comma, it can be specified here

:return: the :class:`Importer` object

:ivar committed: number of :class:`Records <databarc.schema.Record>` committed to the database

:ivar lines: number of lines of file to be imported

:ivar l: current line counter of Importer

:Example:

::
	
	from databarc.schema import session
	from databarc.importer import Importer, DMI_subd
	
	Session = session()
	
	with open('input_file.csv') as file:
		# the reason for separating initialization from the 'do' method is
		# so that the instance can be returned and inspected if 'do' fails
		Imp = Importer(Session, 'source_name', 5432, DMI_subd, file, delimiter='\\t')
		Imp.do(Session)
		
	# prints a list of <field_name>: <record_count> entries
	# and total percentage of lines from input file read
	print Imp
	
In the example, :data:`DMI_subd` is a :ref:`field_dict<field_dict>` defined in this module which I have used to import files from `DMI <http://www.dmi.dk>`_.
	"""
	
	max_commit = 10**5
	"""This class variable controls after how many ingested records a commit to the database should be performed. I found something like this necessary in the beginning, not sure if it is. Can also be set to 0 to eliminate committing altogether and use Importer just as a file 'reader', or to check the populated fields/records first (the :class:`Fields <databarc.schema.Field>` are retrievable through :meth:`fields`). A :meth:`commit` can always be called"""
	
	fail_lines = 100
	"""number of lines for datetime parsing to fail before an :exc:`UnparsedLineLimit` exception is raised"""
	
	def __init__(self,session,source,station_id,field_dict,file,delimiter=','):
		self.parselog = logging.getLogger('parsing')
		self.out = logging.getLogger(__name__)
		self.source = source
		self.station_id = station_id
		
		# we commit explicitly, otherwise errors seem to occur (cf *max_commit*)
		session.autoflush = False
		
		# for debugging purposes, we give the thread the name of the file we're importing
		th = current_thread()
		th.name = os.path.basename(file.name)
		
		# if we're running in an interactive python shell, we also attach the importer instances to the thread
		# this allows 'import_with_threads' to return a list of all used importer instances
		if hasattr(sys,'ps1'):
			try:
				th.importers.append(self)
			except AttributeError:
				th.importers = [self]
		
		# count number of lines in file for printouts
		for i,j in enumerate(file): pass
		self.lines = i
		self.__n = 0 # is compared against max_commit
		self.l = 0
		self.committed = 0
		file.seek(0)
		
		fd = deepcopy(field_dict) # because we pop stuff, otherwise there would be side effects
		 
		 # uses datetime if present, else instantiates dateparser
		self.datetime = fd.pop('datetime',['year','month','day','hour','minute','second','microsecond'])
		
		# test weather to use a csv.reader or not (tuples or ints as keys in field_dict)
		if isinstance(fd.keys()[0],int):
			self.file_reader = csv.reader(file,delimiter=delimiter)
			self.__initfields(session, fd, lambda s,c: s[c])
			self.out.debug('{} [init as csv]'.format(self))
		else:
			self.file_reader = file
			self.__initfields(session, fd, lambda s,c: s[c[0]:c[1]],last_line)
			self.out.debug('{} [init as fixed-width]'.format(self))
		
	
	def __str__(self):
		s = '; '.join(['{}: {}'.format(f['field'].name,f['field'].count) for f in self.__fields.values()])
		return '{} => {}% read'.format(s, int((float(self.l)/self.lines*100)))		
	
	@property
	def fields(self):
		"""Retrieves a :obj:`list` of tuples (:class:`~databarc.schema.Field`, count), where *count* is the actual number of :class:`Records <databarc.schema.Record>` read by :meth:`do`."""
		return [(f['field'],f['count']) for f in self.__fields.values()]
	
	
	def __initfields(self, session, fd, getv):
		# field_dict is copied and elements of the individual field-dicts ('f') are popped
		self.__fields = {}
		for c,f in fd.iteritems():
			if isinstance(f,str):
				# saves colums where to look 
				self.datetime[self.datetime.index(f)] = c
			else:
				d = {'count':0, 'type':f.pop('type')}
				flags = uflags(session,f.pop('flags',[]))
							
				try: mv = compile(f.pop('missing'))
				except KeyError: d['getv'] = getv
				else:
					def getv_miss(s,c):
						x = getv(s,c)
						if mv.search(x): raise MissingValue
						return x
					d['getv'] = getv_miss
			
				# is there a field with same station_id and same name in the database?
				try: 
					d['field'] = session.query(Field).filter_by(station_id=self.station_id, name=f['name'], source=self.source).one()
					d['start'] = session.query(func.max(Record.t)).filter(Record.field_id==d['field'].id).scalar()
					self.out.debug('{} already exists'.format(d['field']))
				except NoResultFound:
					d['field'] = Field(station_id=self.station_id, source=self.source, **f)
				
				d['field'].flags = flags
				self.__fields[c] = d
		
		# builds a datetime function if none is given in the field_dict
		if isinstance(self.datetime,list):
			getlist = [i for i in self.datetime if not isinstance(i,str)]
			def dt(s):
				return datetime(*[int(getv(s,i)) for i in getlist])
			self.datetime = dt
			
		
	
	def do(self,session):
		"""
Performs the actual import, after the main class has been instantiated.

:param session: a SQLAlchemy session object; it **needs** to be a :class:`~sqla:sqlalchemy.orm.scoping.scoped_session` if multithreading is used
:type session: :class:`~sqla:sqlalchemy.orm.session.Session`
		"""
		fail = 0
		for self.l,s in enumerate(self.file_reader):
			try: 
				t = self.datetime(s)
			except Exception as E: 
				if fail == 100:
					raise UnparsedLineLimit('parsing stopped after 100 lines of no datetime match')
				self.parselog.info('line {}: {} [header?]'.format(self.l+1,s))
				fail += 1
			else:
				for c,f in self.__fields.iteritems():
					if 'start' not in f or t>f['start']: # this line here is very sensitive; f['start'] could be None, t could be anything
						try: 
							f['field'].records.append(f['type'](t=t, x=f['getv'](s,c)))
						except MissingValue: pass
						except ValidationError: pass
						except Exception as E: 
							self.parselog.debug('line {}, field {} [{}]'.format(self.l+1,f['field'].name,f['type'].__name__))
							self.parselog.debug(E)
						else: 
							self.__n += 1
							f['count'] += 1
				if self.max_commit and self.__n>=self.max_commit: 
					self.commit(session)
		if self.max_commit and self.__n: 
			self.commit(session)
		
		if self.committed:
			self.out.info('{} [success]'.format(self))
		elif sum(('start' in f) for f in self.__fields.values()):
			self.out.warning('{} [pre-existing?]'.format(self))
		else:
			self.out.warning('{} [failure?]'.format(self))
			
		
		
	def commit(self,session): 
		"""
Usually called by :meth:`do`, unless :attr:`max_commit` is set to 0. Commits all parsed :class:`Fields <databarc.schema.Field>` and :class:`Records <databarc.schema.Record>` to the database.

:param session: a SQLAlchemy session object; it **needs** to be a :class:`~sqla:sqlalchemy.orm.scoping.scoped_session` if multithreading is used
:type session: :class:`~sqla:sqlalchemy.orm.session.Session`
		"""
		F = []
		for f in self.__fields.values():
			if f['count']>0: session.add(f['field'])
			else: F.append(f['field'].name)
		if F:
			self.out.debug('starting commit, fields without data: {}'.format(', '.join(F)))
		else: 
			self.out.debug('starting commit, all fields have data.')
		try:
			session.commit()
		except Exception:
			session.rollback()
			raise
		else:
			self.committed += self.__n
			self.__n = 0
			self.out.debug('{} [committed]'.format(self))
		


def import_with_threads(files, func, num_threads):
	"""
Import files on *num_threads* :class:`threads <threading.Thread>` using a :class:`~Queue.Queue`, one file per thread at a time.

:param list files: list of files to be imported

:param callable func: function to be executed per file; the function has to accept a file (i.e. one element of *files*) as argument

:param int num: number of threads to be used

:return: a list of :class:`Importer` instances created in the process (for debugging purposes)

:Example:

::
	
	import os
	from databarc.schema import session
	from databarc.importer import Importer, DMI_subd, import_with_threads
	
	Session = session()
	
	def fun(file_path):
		# the files are named by station_id
		station_id = os.path.basename(file_path).split('.')[0]
		with open(file_path) as file:			
			Imp = Importer(Session, 'DMI', station_id, DMI_subd, file, delimiter='\\t')
			Imp.do(Session)
	
	# import all files that end in '.txt'
	files = [os.path.join(dir,f) for f in os.listdir(dir) if f[-3:]=='txt']
	
	# use 6 threads
	import_with_threads(filed, fun, 6)
	"""
	from threading import Thread, Event
	from Queue import Queue, Empty
	
	queue = Queue()
	for file in files:
		queue.put(file)
	
	stopped = Event()
	log = logging.getLogger(__name__)
		
	def worker():
		while not stopped.is_set():
			try:
				file = queue.get_nowait()
			except Empty:
				stopped.wait(1)
			else:
				try: func(file)
				except Exception as E: log.error(E)
				queue.task_done()
	
	threads = []
	for n in xrange(num_threads):
		thread = Thread(target=worker)
		thread.setDaemon(True)
		threads.append(thread)
		thread.start()
	
	try:
		while not queue.empty():
			stopped.wait(1)
	except KeyboardInterrupt:
		stopped.set()
		queue = Queue()
	
	queue.join()
	
	importers = []
	for th in threads:
		try:
			importers += th.importers
		except Exception: pass
	return importers

		
class MissingValue(Exception):
	"""Raised when a missing value as specified in field_dict is encountered; main purpose is to distinguish from other exceptions."""
	pass
			
class UnparsedLineLimit(Exception):
	"""Raised after :attr:`Importer.fail_lines` lines have been read without a successful datetime pasing."""
	pass

# note that 'mult' value is given as str (since type is Decimal, the constructor of which takes strings)
DMI_subd = {
	1: 'year',
	2: 'month',
	3: 'day',
	4: 'hour',
	5: {'name':'d', 'code':'d', 'unit':'deg', 'mult':'1', 'type':Record_int, 'flags':[
			{'value':999,'desc':'variable','in_data':True}
		]},
	6: {'name':'f', 'code':'f', 'unit':'m/s', 'mult':'0.1', 'type':Record_int},
	7: {'name':'n', 'code':'n', 'unit':'octas', 'mult':'1', 'type':Record_int, 'flags':[
			{'value':9,'desc':'obscured','in_data':True}
		]},
	8: {'name':'p', 'code':'p', 'unit':'hPa', 'mult':'0.1', 'type':Record_int},
	9: {'name':'t', 'code':'t', 'unit':'C', 'mult':'0.1', 'type':Record_int},
	10: {'name':'tx', 'code':'tx', 'unit':'C', 'mult':'0.1', 'type':Record_int},
	11: {'name':'tn', 'code':'tn', 'unit':'C', 'mult':'0.1', 'type':Record_int},
	12: {'name':'rh', 'code':'rh', 'unit':'%', 'mult':'1', 'type':Record_int},
	13: {'name':'r', 'code':'r', 'unit':'mm', 'mult':'0.1', 'type':Record_int, 'flags':[
			{'value':-1,'desc':'trace','in_data':True}
		]},
	14: {'name':'s', 'code':'s', 'unit':'cm', 'mult':'1', 'type':Record_int,'flags':[
			{'value':997,'desc':'less than 0.5 cm','in_data':True},
			{'value':998,'desc':'not continuous','in_data':True}
		]}
}


NCDC_subd = {
	(13,17): 'year',
	(17,19): 'month',
	(19,21): 'day',
	(21,23): 'hour',
	(23,25): 'minute',	
	(26,29): {'name':'dir subd NCDC', 'code':'d', 'unit':'deg', 'mult':'1', 'type':Record_float},
	(30,33): {'name':'wdsp subd NCDC', 'code':'f', 'unit':'mph', 'mult':'1', 'type':Record_float},
	(34,37): {'name':'gust subd NCDC', 'code':'gus', 'unit':'mph', 'mult':'1', 'type':Record_float},
	(52,56): {'name':'visib subd NCDC', 'code':'vsb', 'unit':'miles', 'mult':'1', 'type':Record_float},
	(83,87): {'name':'temp subd NCDC', 'code':'t', 'unit':'F', 'mult':'1', 'type':Record_float},
	(88,92): {'name':'dew subd NCDC', 'code':'dewp', 'unit':'F', 'mult':'1', 'type':Record_float},
	(93,99): {'name':'slp subd NCDC', 'code':'p', 'unit':'hPa', 'mult':'1', 'type':Record_float},
	(106,112): {'name':'stp subd NCDC', 'code':'stp', 'unit':'hPa', 'mult':'1', 'type':Record_float},
	(113,116): {'name':'mxtemp subd NCDC', 'code':'tx', 'unit':'F', 'mult':'1', 'type':Record_float},
	(117,120): {'name':'mintemp subd NCDC', 'code':'tn', 'unit':'F', 'mult':'1', 'type':Record_float},
	(121,126): {'name':'pcp01 subd NCDC', 'code':'r', 'unit':'in', 'mult':'1', 'type':Record_float},
	(127,132): {'name':'pcp06 subd NCDC', 'code':'r', 'unit':'in', 'mult':'1', 'type':Record_float},
	(133,138): {'name':'pcp24 subd NCDC', 'code':'r', 'unit':'in', 'mult':'1', 'type':Record_float},
	(139,144): {'name':'pcpXX subd NCDC', 'code':'r', 'unit':'in', 'mult':'1', 'type':Record_float},
	(145,147): {'name':'s subd NCDC', 'code':'s', 'unit':'in', 'mult':'1', 'type':Record_float},
}


NCDC_isd_lite = {
	(0,4): 'year',
	(5,7): 'month',
	(8,11): 'day',
	(11,13): 'hour',
	(13,19): {'name':'temp isd NCDC', 'code':'t', 'unit':'C', 'mult':'0.1', 'type':Record_int, 'missing':'-9999'},
	(19,24): {'name':'dew isd NCDC', 'code':'dewp', 'unit':'C', 'mult':'0.1', 'type':Record_int, 'missing':'-9999'},
	(25,31): {'name':'slp isd NCDC', 'code':'p', 'unit':'hPa', 'mult':'0.1', 'type':Record_int, 'missing':'-9999'},
	(31,37): {'name':'dir isd NCDC', 'code':'d', 'unit':'deg', 'mult':'1', 'type':Record_int, 'missing':'-9999'},
	(37,43): {'name':'wdsp isd NCDC', 'code':'f', 'unit':'m/s', 'mult':'0.1', 'type':Record_int, 'missing':'-9999'},
	(43,49): {'name':'sky isd NCDC', 'code':'n', 'unit':'octas', 'mult':'1', 'type':Record_int, 'missing':'-9999'},
	(49,55): {'name':'pcp01 isd NCDC', 'code':'r', 'unit':'mm', 'mult':'0.1', 'type':Record_int, 'missing':'-9999'},
	(55,61): {'name':'pcp06 isd NCDC', 'code':'r', 'unit':'mm', 'mult':'0.1', 'type':Record_int, 'missing':'-9999'},
}


def _parse(*ind):
	p = compile('(\d+)/(\d+)/(\d+);(\d+):(\d+):(\d+)')
	def parse(line):
		return datetime(*[int(x) for x in p.search(';'.join(line[i] for i in ind)).groups()])
	return parse

flags = [
	{'value': 0, 'desc': 'value ok', 'in_data': False},
	{'value': 1, 'desc': 'removed not clean', 'in_data': False},
	{'value': 2, 'desc': 'not used in concatenation', 'in_data': False},
	{'value': 3, 'desc': 'removed from concatenated timeseries', 'in_data': False}
]

baro_logger = {
	'datetime': _parse(0,1),
	3: {'name':'AP', 'code':'p', 'unit':'cm', 'mult':1, 'type':Record_num, 'flags': flags},
	4: {'name':'AT', 'code':'t', 'unit':'C', 'mult':1, 'type':Record_num, 'flags': flags}
}

baro_logger_2 = {
	'datetime': _parse(1,2),
	4: {'name':'AP', 'code':'p', 'unit':'cm', 'mult':1, 'type':Record_num, 'flags': flags},
	5: {'name':'AT', 'code':'t', 'unit':'C', 'mult':1, 'type':Record_num, 'flags': flags}
}


level_logger = {
	'datetime': _parse(0,1),
	3: {'name':'WP', 'code':'p', 'unit':'cm', 'mult':1, 'type':Record_num},
	4: {'name':'WT', 'code':'t', 'unit':'C', 'mult':1, 'type':Record_num},
	5: {'name':'EC', 'code':'ec', 'unit':'mS/cm', 'mult':1, 'type':Record_num},
}

level_logger_2 = {
	'datetime': _parse(1,2),
	4: {'name':'WP', 'code':'p', 'unit':'cm', 'mult':1, 'type':Record_num},
	5: {'name':'WT', 'code':'t', 'unit':'C', 'mult':1, 'type':Record_num},
	6: {'name':'EC', 'code':'ec', 'unit':'mS/cm', 'mult':1, 'type':Record_num},
}