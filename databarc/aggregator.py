"""
Using the aggregator module
===========================

The aggregator module consists of an abstract base class, :class:`Aggregator`, and, currently, two subclasses implementing the actual aggregation: :class:`Daily_aggregator` and :class:`Monthly_aggregator`. Together with their configuration options via :ref:`aggregation dictionaries<aggr_dicts>`, these subclasses should cover most use cases, and they can be used as examples of how to implement further subclasses should the need arise. The simply contain a :ref:`run method<run_meth>` which iterates over the records to be aggregated (accessible via ``self.parent.records`` after instantiation); the aggregation interval of the subclass is given as a class variable ``interval`` which is used to populate the :attr:`~databarc.schema.Aggregate_field.interval` attribute of the :class:`~databarc.schema.Aggregate_field` resulting from the aggregation in the ``__init__`` method of :class:`Aggregator`.

To provide a lot of flexibility, the :ref:`run method<run_meth>` deals with the temporal binning of records, while the actual computation is handled by a separate, easily replaceable :ref:`aggregation function<aggr_func>`. This function can be given directly or via a :ref:`dictionary<aggr_dicts>` to the :class:`Aggregator` (subclass) constructor and has access to all necessary information via instance variables.

.. _aggr_func:

The aggregation function
------------------------

The *func* argument to the :class:`Aggregator` constructor is a callable which accepts a ``self`` parameter representing the :class:`Aggregator` instance (the parameter name does not have to be 'self', but for clarity's sake, it doesn't hurt). It contains the actual mathematical computation to be performed on each 'bin' of :class:`Records<databarc.schema.Record>` from the 'parent' field. The necessary data are exposed as instance variables on ``self`` (which are populated in the subclass' :ref:`run method<run_meth>`): ``self.bin``, ``self.t``, ``self.p_flags`` (see :class:`Aggregator`).

Of course, all other ivars are similarly available should they be necessary. In turn, *func* is responsible for assigning the computed aggregate value to ``self.x``.

Several :ref:`predefined aggregation functions<prov_afuncs>` are provided in this module.

.. note::
	An unbound convenience method :meth:`self.fx(bin)<Aggregator.fx>` is also provided which returns a copy of :obj:`list` *bin* of :class:`Records<databarc.schema.Record>` from which all records with ``x`` attributes equal to a flag value (from ``self.p_flags``) have been removed.


.. _run_meth:

The ``run`` method
------------------

The ``run`` method of :class:`Aggregator` subclasses has the following responsibilities:

1. Binning of :class:`Records<databarc.schema.Record>` made available through ``self.parent.records``, based on their :attr:`timestamp<databarc.schema.Record.t>` and assignment of the bin to ``self.bin``.
2. Assignment of an appropriate timestamp for the aggregated record to ``self.t``.
3. Call :meth:`~Aggregator.step` at an appropriate time. This method is a wrapper around the aggregation function ``func`` which takes care of some housekeeping.
4. Call :meth:`~Aggregator.finish` after completing the interation over ``self.parent.records``.

Since :meth:`~Aggregator.step` and :meth:`~Aggregator.finish` take care of some synchronization calls for the case when the aggregation is performed on multiple threads and :ref:`auxiliary fields<aux_fields>` are needed, it is not advisable to omit these calls, even though in principle it is possible (if the complete iteration logic is contained in ``run``).


.. _aux_fields:

Auxiliary fields
----------------

It is conceivable that the :ref:`aggregation function<aggr_func>` needs access to contemporary records of another :class:`~databarc.schema.Field` of the same :class:`~databarc.schema.Station`. For example, when dealing with wind direction, the value ``0`` is frequently found in conjunction with a ``0`` value in wind speed, and hence should not be taken into account when computing an average. When instantiating an :class:`Aggregator`, the keyword argument ``aux_fields`` can be specified with a list of :obj:`str` :attr:`codes<databarc.schema.Field.code>` referring to the needed fields. During intialization, the :class:`Aggregator` searches first for other previously instantiated aggregators (via :meth:`Aggregator.run_threads`) for fields with the required codes; if it does not find them, it searches the database if the :class:`Aggregate_fields<databarc.schema.Aggregate_field>` already exist there. If neither search is successful, aggregation proceeds without the auxiliary fields. 

Within the :ref:`aggregation function<aggr_func>`, a dictionary is available as instance variable ``self.aux``, with the auxiliary fields' :attr:`codes<databarc.schema.Field.code>` as keys. The corresponding values are python generators whose ``next()`` method can be called upon from within the :ref:`aggregation function<aggr_func>` and yields the auxiliary field's 'bin' of values for the current aggregation interval::

	auxiliary_bin = self.aux['f'].next()	# if the auxiliary field's code is 'f'
	
This call blocks until the needed aggregation interval is available from the auxiliary field if the fields are aggragated concurrently; if, on the other hand, a particular interval is not available (e.g., because the auxiliary field is missing data), it simply returns an empty list. See the source code for :func:`wind_dir` for a use example. 

.. warning::
	It is the user's responsibility to add needed auxiliary fields which do not have an associated :class:`~databarc.schema.Aggregate_field` in the database yet to a bulk aggregation operation via :meth:`Aggregator.run_threads`. This method will start interdependent aggregations concurrently and synchronize them so that all needed information is available at the right time. If, on the other hand, aggregations are performed manually one-by-one, it is only necessary to perform interdependent ones in the correct order (as long as the results are committed to the database). In above example, wind speed needs to be aggregated before wind direction since ``0`` direction values need to be eliminated on the basis of ``0`` speed values. The reason why the :class:`databarc.schema.Aggregate_field` is needed for the auxiliary field is because the temporal binning has been performed already and the bins can be made available to the aggregator for the dependent field.

.. _aggr_dicts:

Aggregation dictionaries
------------------------

An :class:`Aggregator` has to be instantiated with a number of keyword arguments, which may be collected into predefined dictionaries similar to those :ref:`used for importing data files<field_dict>`. For example, this module contains a few :ref:`predefined dictionaries<prov_adicts>` which use field :attr:`codes<databarc.schema.Field.code>` as keys.::
	
	from databarc.schema import Record_int, Record_float
	from databarc.aggregator import wind_dir, ave
	
	# example aggregation dictionary
	aggr_dict = {
		'd':{'type':Record_int,'func':wind_dir,'aux_fields':['f']},
		'f':{'type':Record_float,'func':ave}
	}
	
	# example usage with 'run_threads' - f, d are field objects with field codes 'f' and 'd'
	Daily_aggregator.run_threads([f, d], aggr_dict, num_threads=2)
	
This example defines a dictionary containing aggregation parameters for fields with :attr:`codes<databarc.schema.Field.code>` 'f' and 'd' (wind speed and wind direction, in the `DMI <http://www.dmi.dk>`_ data). The :meth:`run_threads` class method of :class:`Aggregator` takes a list of :class:`Fields<databarc.schema.Field>` as first argument and matches their code with the corresponding entry from the aggregation dictionary (the second positional argument). Relevant keywords for the dictionary are ``type``, ``func`` and ``aux_fields``, corresponding to the keyword arguments for the :class:`Aggregator` constructor.
"""
from types import MethodType
from datetime import timedelta, datetime
from sqlalchemy import not_
from sqlalchemy.orm import object_session, joinedload
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from threading import Thread, Event, current_thread
from databarc.schema import Record, Record_int, Record_float, Field, Aggregate_field
from databarc.utils import flags
import numpy as np		
import logging



class Aggregator(object):
	"""
Aggregator(parent, type, func [, aux_fields=[], flags=[], commit=True])
Abstract base class for aggregation classes. The keyword arguments passed to the constructor are also available as instance variables, except for *aux_fields* (see :ref:`auxiliary fields<aux_fields>`) and *flags*. Flags (which are passed as dictionaries) are appended only to the newly created :class:`Aggregate_field's<databarc.schema.Aggregate_field>` :attr:`databarc.schema.Aggregate_field.flags` (and are hence available via *field.flags*). There is, however, a *flags* attribute available on instances of :class:`Aggregator`, which contains the *parent*'s flags, for use in *func*.

:keyword parent: field 'containing' (via :attr:`~databarc.schema.Field.records`) the records to be aggregated
:type parent: :class:`~databarc.schema.Field`

:keyword type: class to be used for the aggregated records (e.g. :class:`~databarc.schema.Record_float`)
:type type: :class:`~databarc.schema.Record` subclass

:keyword callable func: function to be used for the aggregation, needs to take ``self`` argument (see :ref:`aggr_func`)

:keyword list aux_fields: a list of fields (identified by :attr:`~databarc.schema.Field.code`) from the same station that are needed by *func*

:keyword list flags: a list of :obj:`dicts<dict>` describing :ref:`flags<flags>` to be referenced from *field* which are not already defined for *parent* (flags defined on *parent* are automatically added to *field*)

:keyword bool commit: whether to commit resulting :class:`~databarc.schema.Aggregate_field` to database or not

:ivar field: field subclass 'containing' the records resulting from aggregation
:vartype field: :class:`~databarc.schema.Aggregate_field`

:ivar datetime t: the current timestep of the aggregation process, i.e. the :attr:`timestamp<databarc.schema.Record.t>` which will be assigned to aggregated record currently being computed

:ivar list bin: a list of records from the *parent* field falling into the current aggregation interval (e.g. a day), to be used by *func*

:ivar list p_flags: a list of :attr:`~databarc.schema.Flags.in_data` flag **values** defined for the records of the *parent* :class:`~databarc.schema.Field`, in case they are necessary for the computation of the aggregate value (e.g. as in the case of the common ``-1`` flag for a trace amount of precipitation)

.. method:: fx(bin)
	
	Convenience method that does::
	
		return [r.x for r in bin if r.x is not None and r.x not in self.p_flags]
	"""
	registry = {}
	def __init__(self,**kw):
		self.log = logging.getLogger(__name__)
		self.type = kw.pop('type')
		self.parent = kw.pop('parent')
		self.commit = kw.pop('commit', True)
		self.func = MethodType(kw.pop('func'), self)
		
		# need to be popped before handing kw to Aggregate_field constructor
		aux = kw.pop('aux_fields',[])
		new_flags = flags(object_session(self.parent), kw.pop('flags',[]))

		# this populates the new instance with default column values in case these are needed for computations
		for c in Aggregate_field.__table__.c:
			if c.name not in kw:
				try: kw[c.name] = c.default.arg
				except AttributeError: pass
		
		self.field = Aggregate_field(
			parent = self.parent,
			type = self.type.__name__,
			func = self.func.__name__,
			interval = self.interval,
			**kw
		)
		self.field.flags = new_flags
		
		# instantiate helper classes for threaded import when auxiliary fields are needed
		if aux:
			self.aux = dict(self.__aux(c) for c in aux)
		
		self.bin = []
		self.p_flags = [f.value for f in self.parent.flags if f.in_data]
		if self.p_flags:
			self.fx = lambda bin:[r.x for r in bin if r.x is not None and r.x not in self.p_flags]
		else:
			self.fx = lambda bin:[r.x for r in bin if r.x is not None]
		
		key = (self.field.code, self.field.station_id, self.field.source)
		if key in self.registry:
			raise Exception('Code/station/source multiplicity ({} / {} / {}) [{name}].'.format(*key, name=self.parent.name))
		else:
			self.registry[key] = self
		
		self.log.debug('{}, {} started'.format(self.field.name,self.field.station_id))
		
		
	
	def step(self):
		"""
Convenience wrapper around the aggregation function *func* which can be called from subclasses' ``run`` method. It instantiates new aggregated :attr:`Records<databarc.schema.Record>`, adds the binned *parent* records to the new record's :attr:`~databarc.schema.Record.binned` attribute appends the record to *field*.
		"""
		self.info = None
		if self.func():
			y = self.type(t=self.t, x=self.x, info=self.info)
			y.binned = self.bin[:] 						# possibly important, [:] ensures COPY
			self.field.records.append(y)
		self.bin = []
		# this synchronizes threads when aux_fields are present
		try:
			self.lock.set()
		except AttributeError: pass

			
	def finish(self):
		"""
Finalization method to be called from subclasses' ``run`` method after iteration through ``self.parent.records`` is complete.
		"""
		# another synchronization lock for the case with aux_fields
		try: 
			self.done.set()
		except AttributeError: pass
		self.step()
		print '{} done'.format(self.field.name)
		if self.commit:
			session = object_session(self.field)
			session.add(self.field)
			session.commit()
			print '{} committed'.format(self.field.name)
			session.close()
	
	
	def __aux(self, code):				
		# are we running concurrently with anself aggregator?
		# if yes, we need synchronization
		try:
			aggregator = self.registry[(code, self.field.station_id, self.field.source)]
			records = aggregator.field.records
			try:
				lock = aggregator.lock
				done = aggregator.done
			except AttributeError:
				aggregator.lock = Event()
				aggregator.done = Event()
				lock = aggregator.lock
				done = aggregator.done
	
			def step(i):
				while True:
					if (not done.is_set()) and (len(records)==0 or self.t>records[-1].t):
						lock.clear()
						lock.wait() # sync
					while i+1<len(records) and records[i].t<self.t:
						i += 1
					yield records[i].binned if records[i].t==self.t else []
				
			self.log.debug('Aux {} for field {} started with running aggregator.'.format(aggregator.field.code, self.parent))
			return code, step(0)
					
		
		# if we're not running concurrently, is the needed Aggregate_field it already in the DB?
		except KeyError:
			session = object_session(self.field.parent)
			try:
				aux_field = session.query(Aggregate_field).filter_by(
					code=code, station_id=self.field.station_id, 
					source=self.field.parent.source, 
					interval=self.field.interval
				).one()
			except NoResultFound:
				raise Exception("Auxiliary PARENT {} for field {} not available.".format(code,self.field.name))
			except MultipleResultsFound:
				raise Exception("Multiple results in database for Auxiliary PARENT {} for field {}.".format(code,self.field.name))
			else:
		
		# if it is, load all the records, including the bins as joinedload
		# in this case, we don't need all the synchronization machinery
				records = session.query(Record).options(joinedload(Record.binned)).filter_by(field_id=aux_field.id).all()
				if not records: 
					raise Exception("No Records located in database for Auxiliary PARENT {} for field {}.".format(code,self.field.name))
				
				def step(i):
					while True:
						while i+1<len(records) and records[i].t<self.t:
							i += 1
						yield records[i].binned if records[i].t==self.t else []
				
				self.log.debug('Aux {} for field {} started from existing aggregation.'.format(aux_field.code, self.parent))
				return code, step(0)
	
	
	
	@classmethod
	def run_threads(cls, fields, aggr_dict, num_threads=1, **kw):
		"""
Helper method to perform aggregations concurrently on *num_threads* :class:`Threads<threading.Thread>`.

:param list fields: list of :class:`~databarc.schema.Field` objects whose records should be aggregated

:param dict aggr_dict: :ref:`dictionary<aggr_dict>` describing the field-dependent aggregation parameters
		"""
		from Queue import Queue, Empty
		from copy import deepcopy
		from collections import OrderedDict
		
		# sort fields such that dependent fields are initialized after auxiliary ones
		def asort(codes):
			d = OrderedDict()
			for c in codes:
				try: 
					e = asort(aggr_dict[c]['aux_fields'])
					e.update(d)
					d = e	
				except KeyError: pass
				d[c] = deepcopy(aggr_dict[c])
			return d
		
		# just to be on the safe side with respect to threading, all aggregators are
		# instantiated on the main thread and added to the queue, and hence the registry, in order
		q = Queue()
		s = asort(f.code for f in fields)
		for c,d in s.iteritems():
			d.update(kw)
			try: q.put(cls(parent=[f for f in fields if f.code==c][0], **d))
			except IndexError: pass
		
		stopped = Event()
		
		def worker():
			while not stopped.is_set():
				try:
					a = q.get_nowait()
				except Empty:
					stopped.wait(1) 
				else:
					a.run()
					q.task_done()
		
		for n in xrange(min(num_threads,len(cls.registry))):
			thread = Thread(target=worker)
			thread.setDaemon(True)
			thread.start()
		
		try:
			while not q.empty():
				stopped.wait(1)
		except KeyboardInterrupt:
			stopped.set()
			q = Queue()
		
		q.join()
		return [a.field for a in cls.registry.values()]
		
		

class Daily_aggregator(Aggregator):
	"""TEST"""
	interval = 'day'
	
	def run(self):
		if self.field.zero_incl:
			self.cmp = lambda x,y:x<y+timedelta(days=1)+self.field.postpone
		else: 
			if hasattr(self.field,'postpone') and self.field.postpone!=timedelta(0):
				self.cmp = lambda x,y:x<y+self.field.postpone
			else: self.cmp = lambda x,y:x<=y
		t = self.parent.records[0].t
		s = np.sign(t.hour-self.field.zero_hour)
		s = s * int(self.field.zero_incl) if s<0 else s*(1-int(self.field.zero_incl))
		self.t = datetime(t.year,t.month,t.day,self.field.zero_hour) + timedelta(days=s)
		
		for r in self.parent.records:
			if self.cmp(r.t,self.t):
				self.bin.append(r)
			else:
				while not self.cmp(r.t,self.t):
					self.step()	
					self.t += timedelta(days=1)
				self.bin = [r]
		self.finish()	
		


class Monthly_aggregator(Aggregator):
	interval = 'month'
	
	def run(self):
		t = self.parent.records[0].t
		self.t = datetime(t.year,t.month,1)
		for r in self.parent.records:
			if r.t.month==self.t.month and r.t.year==self.t.year:
				self.bin.append(r)
			else: 
				if r.t.day==1 and not self.field.zero_incl:	# zero_incl==False implies aggregation
					self.bin.append(r)						# over the previous 24h, i.e. value at 6 UTC
					self.step()							# contains 18h accumulation from previous day
				else:
					self.step()	
					self.bin = [r]
				self.t = datetime(r.t.year,r.t.month,1)
		self.finish()	

	
	
def rain_orig(self):
	# postpone = timedelta(hours=6)
	if not self.bin: return False
	a = 0
	b = 0
	tr = False
	for r in self.bin:
		if self.p_flags and r.x == self.p_flags[0]: tr = True
		if (r.t.hour>=12 and r.t.hour<=18):
			a = max(0,r.x)
		elif (r.t.hour>=0 and r.t.hour<=6):
			b = max(0,r.x)
	a += b
	self.x = -1 if a==0 and tr else a
	return True

def rain_DMI(self):
	# no postpone
	if not self.bin: return False
	a = 0
	b = 0
	tr = False
	for r in self.bin:
		if self.p_flags and r.x == self.p_flags[0]: tr = True
		x = max(0,r.x)
		if r.t.hour==12: a = x
		elif r.t.hour==18: a = x if x>a else x+a
		elif r.t.hour==0: b = x
		elif r.t.hour==6: b = x if x>b else x+b
	a += b
	self.x = -1 if a==0 and tr else a
	return True

def rain_XT(self,hours=lambda r:r.t.hour%12+6,check_start=False):
	"""
rain_XT has to be called with 'postpone' if 'XT15' is used, i.e.
interval length is assumed to be r.t.hour%12+6
otherwise binning starts too early the previous day (>6 UTC instead >=12)
	"""
	if not self.bin: return False
	tr = False
	t = self.bin[-1].t
	x = 0
	for i in range(len(self.bin)):
		r = self.bin[-i-1]
		dt = timedelta(hours=hours(r))
		if r.x==-1: tr = True
		elif r.t<=t:
			x += r.x
			t = r.t-dt
	if check_start:
		try: r = self.field.records[-1].binned[-1]
		except: pass
		else:
			if r.t-dt==t and r.x<=x: 
				x -= r.x
	self.x = -1 if x==0 and tr else x
	return True

def rain_info(self):
	return rain(self,hours=r.info if r.info and r.info!=99 else r.t.hour%12+6)

def rain_month(self):
	# NOTE: monthly rain goes from 6:00 UTC on the first day of the month til 6:00 UTC on 
	# the first day of the following month (c.f. tr13-11/_read.py)
	x = self.fx(self.bin)
	if not x:
		if self.bin:
			self.x = len(self.bin) # since every trace value <= .1 mm and scaling = x10
			return True
		else: return False
	self.x = sum(x)
	self.info = len(self.bin)
	return True

def ave(self):
	x = self.fx(self.bin)
	if not x:
		if self.bin:
			self.x = None
			self.info = 0
			return True
		else: return False
	self.x = np.mean(x)
	self.info = len(x)
	return True
	
def wind_dir(self):
	"""aggregation function for wind direction"""
	x = []
	try:
		# this blocks until times are synchronized, or returns []
		aux_bin = self.aux['f'].next()
		for a in self.bin:
			# check for <=0 in wind SPEED
			try:
				if [b.x for b in aux_bin if b.t==a.t][0] <= 0:
					raise IndexError
			except IndexError: pass
			else: x.append(a)
	except AttributeError: pass
	y = self.fx(x)
	if not y:
		if not x: self.x = None
		if self.bin: self.x = 999
		else: return False
		self.info = len(x)
	else:
		self.info = len(y)
		# https://en.wikipedia.org/wiki/Mean_of_circular_quantities
		y = int(round(np.angle(np.mean(np.exp(np.array(y) * 1j * np.pi/180.)), deg=True)))
		self.x = y + 360 if y<0 else y
	return True




DMI_daily = {
	'd':{'type':Record_int,'func':wind_dir,'aux_fields':['f']},
	'f':{'type':Record_float,'func':ave},
	'n':{'type':Record_int,'func':ave},
	'p':{'type':Record_float,'func':ave},
	't':{'type':Record_float,'func':ave},
	'rh':{'type':Record_float,'func':ave},
	'r':{'type':Record_int,'func':rain_XT,'postpone':timedelta(hours=6)},
	's':{'type':Record_int,'func':ave}
}
	
DMI_monthly = {
	'd':{'type':Record_int,'func':wind_dir},
	'f':{'type':Record_float,'func':ave},
	'n':{'type':Record_int,'func':ave},
	'p':{'type':Record_float,'func':ave},
	't':{'type':Record_float,'func':ave},
	'rh':{'type':Record_float,'func':ave},
	'r':{'type':Record_int,'func':rain_month},
	'rbc':{'type':Record_int,'func':rain_month},
	's':{'type':Record_int,'func':ave}
}

NCDC_daily = {
	'd':{'type':Record_float,'func':wind_dir,'flags':[{'value':990,'desc':'variable','in_data':True}]},
	'f':{'type':Record_float,'func':ave},
	't':{'type':Record_float,'func':ave},
	'dewp':{'type':Record_float,'func':ave},
	'p':{'type':Record_float,'func':ave},
	'stp':{'type':Record_float,'func':ave}
}

	

def _daily():
	q = Session.query(Field).filter(Field.subclass=='basic',Field.source=='DMI_subd',not_(Field.name.in_(['tx','tn'])))
	q = q.filter(not_(Field.aggregates.any())).order_by(Field.station_id, Field.code.desc()).all()
	id = q[0].station_id
	i = 0
	while q:
		if q[0].station_id==id:
			f = q.pop(0)
			print i
			Daily_aggregator(f,**DMI_daily[f.code])
			i += 1
		else:
			if q: id = q[0].station_id
			if i>5:
				Daily_aggregator.start_all()
				i = 0
	Daily_aggregator.start_all()



if __name__ == "__main__":
	q = Session.query(Field).filter(Field.code=='rbc').all()
	while q:
		for i in range(6):
			try: f = q.pop(0)
			except IndexError: pass
			else: Monthly_aggregator(f, name='r bias-corr month', **DMI_monthly[f.code])
		Monthly_aggregator.start_all()
			
