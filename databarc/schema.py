"""
This module uses the object-relational mapper `SQLAlchemy <http://www.sqlalchemy.org>`_ to map database tables to python objects, thereby defining a database model. Each class corresponds to a table in the database of the same name (but in lowercase). The class-level attributes of each class are mapped to columns of the same name in the corresponding database table. A class instance therefore corresponds to a row in the respective table. Attributes can also represent relationships; for example, when accessed, an attribute representing a one-to-many relationship returns a list, as with the :attr:`Field.records` attribute. For more details see the `SQLAlchemy documentation <http://docs.sqlalchemy.org>`_.

Table classes can be instantiated with a list of keywords, e.g.::
	
	field = Field(name='windspeed', unit='m/s', station_id=4321)

corresponds to a row in the table 'field' with values 'windspeed', 'm/s' and '4321' for the columns 'name', 'unit' and 'station_id'. In other words, adding the class instance to the :obj:`session` (see also the examples for :class:`Record`) corresponds to emitting the SQL expression::

	INSERT INTO field (name, unit, station_id)
	VALUES ('windspeed', 'm/s', 4321);

.. note::
	The attributes of the mapped classes can generally be populated by :obj:`str` objects, even if they represent number types in the database - *and vice versa*. SQLAlchemy will perform obvious conversions on persisting the data to the database.
"""
from sqlalchemy import Column,Integer,String,Numeric,Float,Date,DateTime,Boolean,Interval,\
	ForeignKey,Table,Index,cast,Text,UniqueConstraint,text,create_engine,and_,PickleType
from sqlalchemy.ext.declarative import declarative_base,declared_attr
from sqlalchemy.orm import relationship,sessionmaker,scoped_session,backref,column_property,\
	object_session,validates
from sqlalchemy.sql import select,func
from sqlalchemy.types import TypeDecorator
from geoalchemy2 import Geography, Geometry
from datetime import timedelta
from decimal import Decimal


def session(desc=None):
	"""
Create a thread-local session using database connections saved in a ``config.ini`` file in the current directory.

:param desc: if :obj:`None`, the first database connection defined in ``config.ini`` is used, otherwise give the name of the connection here (e.g. as given when invoking :func:`databarc-create <databarc.scripts.create>`)

:return: a :sqla:`thread-local SQLAlchemy session<orm/contextual.html>`
:rtype: :class:`~sqla:sqlalchemy.orm.scoping.scoped_session`

:Example:
	
	if the contents of 'config.ini' are::
	
		[db]
		one = postgresql://user1@/db1
		two = postgresql://user1@/db2
	
	you can connect to database 'db2' like so::

		from databarc.schema import session
		Session = session('two')
	
This function is intended mostly as an example. In practice, it is probably easier to just hardcode the database connection somewhere in your code, e.g.::
	
	from sqlalchemy import create_engine
	from sqlalchemy.orm import sessionmaker,scoped_session
	engine = create_engine( 'postgresql://user@/database' ) 
	Session = scoped_session(sessionmaker(bind=engine))
	
See also the :sqla:`SQLAlchemy tutorial <orm/tutorial.html>` and :func:`~sqla:sqlalchemy.create_engine`. And here's more about :sqla:`using the session <orm/session.html>`.
	
.. note::
	If you do a ``from databarc.schema import *``, many SQLAlchemy classes and functions used here will be imported into your namespace (cf. the source).
	"""
	from ConfigParser import SafeConfigParser
	conf = SafeConfigParser()
	conf.read('databarc.cfg')
	url = conf.get('db',desc) if desc else conf.items('db')[0][1]
	return scoped_session(sessionmaker(bind=create_engine(url)))


class Base(object):
	@declared_attr
	def __tablename__(cls):
		return cls.__name__.lower()

Base = declarative_base(cls=Base)


class Station(Base):
	"""Station"""
	id = Column(Integer, primary_key=True)
	station_id = Column(Integer)
	name = Column(String(100))
	loc = Column(Geography(geometry_type='POINT', srid=4326))
	z = Column(Integer)
	startdate = Column(Date)
	enddate = Column(Date)
	lon = column_property(cast(loc, Geometry('POINT', 4326)).ST_X())
	lat = column_property(cast(loc, Geometry('POINT', 4326)).ST_Y())
	region = Column(String(2))
	
	def distance(self,toOther):
		return object_session(self).scalar(
			select([func.st_distance_sphere(
				cast(self.loc, Geometry('POINT', 4326)), cast(toOther.loc, Geometry('POINT', 4326))
			)])
		)
	
	def __repr__(self):
		# NOTE: some names have unicode characters, hence the "!r" format option
		return '<{} id: {}, name: {!r}, loc: ({:.2f}, {:.2f}), z: {}>'.\
			format(self.__class__.__name__,self.station_id,self.name,self.lon,self.lat,self.z)



record_assoc = Table('record_assoc', Base.metadata,
	Column('parent_id', Integer, ForeignKey('record.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'), index=True),
	Column('child_id', Integer, ForeignKey('record.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'), index=True)
)


class Record(Base):
	"""
Abstract base class for any record type. All actual records are instances of subclasses such as :class:`Record_int`, :class:`Record_float` or :class:`Record_num`, which should be used for instantiation. For details, see :sqla:`mapping class inheritance hierarchies <orm/inheritance.html>`.

This has, among others, the following benefits:
	* different database/python types can be used for different measured quantities
	* :class:`Record` can be used for querying all subclasses
	* relationships between records (see :attr:`Record.binned`) are defined on the abstract base class and therefore do not depend on the record type
	* new record types may easily be defined (by subclassing :class:`Record`) without changing the overall structure of the database model

.. note::
	The :attr:`Record.t` attribute is defined on the abstract base class and corresponds to the python type :class:`datetime.datetime`, since this has proven to be for more practical for many querying purposes than having differing time types defined on the subclasses.
	"""
	id = Column(Integer, primary_key=True)
	type = Column(String(20), index=True)
	info = Column(Integer)
	"""flag which can be used for any purpose"""
	t = Column(DateTime, nullable=False)
	"""time value (:class:`datetime.datetime`)"""
	binned = relationship('Record', secondary=record_assoc, 
		primaryjoin='record_assoc.c.parent_id==record.c.id', 
		secondaryjoin='record_assoc.c.child_id==record.c.id',
	)
	"""If the record's ``x`` value has been computed as a function of other records (e.g. an average over a period of time), contains a list of the original records used in the computation."""
	field_id = Column(Integer, ForeignKey('field.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'), nullable=False, index=True)
	__mapper_args__ = {'polymorphic_on': type, 'with_polymorphic':'*'}
	__table_args__ = (UniqueConstraint('field_id','t',deferrable=True,initially='deferred'),)
	
	def __repr__(self):
		s = '<{} id: {}, '.format(self.__class__.__name__, self.id)
		if self.field: s = '{}field: {}, '.format(s,self.field.name)
		return '{}date: {}, x: {}>'.format(s,self.t.strftime('%Y/%m/%d %H:%M'), self.x)


class Flag(Base):
	"""
Saves information about flag values and can be associated with a :class:`Field` via its :attr:`Field.flags` attribute (a :sqla:`many-t-many relationship<orm/basic_relationships.html#many-to-many>`). Flags can be set in either the data (i.e. the ``x`` attribute of :ref:`record_sub`) or the :attr:`Record.info` attribute. It is assumed that flag :attr:`values<value>` are integers.
	"""
	id = Column(Integer, primary_key=True)
	value = Column(Integer)
	"""flag's :obj:`int` flag value"""
	desc = Column(Text)
	"""description of the flag's meaning as :class:`sqla:sqlalchemy.types.Text`"""
	in_data = Column(Boolean)
	"""whether the flag value is to be found in the :ref:`x attribute<record_sub>` (True) or the :attr:`Record.info` attribute (False)"""
	
	__table_args__ = (UniqueConstraint('value','in_data','desc',deferrable=True,initially='deferred'),)
	def __repr__(self):
		return '{}: {} ({})'.format(self.value,self.desc,'in-data' if self.in_data else 'additional') 


class Field(Base):
	"""
This class is intended to hold any necessary metadata about a given timeseries, in the form of (scalar) attributes. It has a :sqla:`one-to-many relationship <orm/basic_relationships.html#one-to-many>` with :class:`Record` (or rather, its subclasses).

:Example:

::
	
	field = Session.query(Field).get(1) # gets field with id=1
	records = field.records # will return a python list, ordered by time

.. note::
	There is a composite unique constraint (see :sqla:`defining constraints and indexes <core/constraints.html>`) on :attr:`name`, :attr:`station_id` and :attr:`source`, which means that the combination of these three attributes has to be unique, or else the database raises an exception on adding a 'field'. They **don't** have to be all present, i.e. if you only use :attr:`name` to describe your field, each name has to be different.
	"""
	id = Column(Integer, primary_key=True)
	name = Column(String(100))
	"""name of the field"""
	code = Column(String(10))
	"""This is intended as an identifier for the type of variable represented by the field. For example, one might want to distinhuish between monthly and daily accumulated rainfall by giving a different :attr:`name`, but still use the same 'code' value (e.g. 'r')."""
	station_id = Column(Integer)
	"""Stations are currently not relationship-linked, since sometimes there are several :class:`Station` instances for one :attr:`Station.station_id` (when stations were moved, for example)."""
	source = Column(String(100))
	"""to be used at discretion, e.g. for the origin of the data."""
	subclass = Column(String(20), default='basic', nullable=False)
	mult = Column(Numeric(10,4))
	"""A multiplication factor can be specified here for later use in code. For example, if a field's :attr:`~Field.records` contain :class:`Record_ints <Record_int>` whose :attr:`~Record_int.x` attribute represents integer multiples of 0.1, one can save here the factor ``'0.1'``. Since this is currently implemented as a :obj:`decimal.Decimal`, the value should be given as a :obj:`str` on construction."""
	unit = Column(String(20))
	"""unit of the recorded quantity as :obj:`str` (max length 20)"""
	records = relationship('Record', backref='field', order_by='Record.t', cascade='all, delete-orphan', passive_deletes=True)
	"""returns an instrumented list containing the actual records of the timeseries represented by an instance"""
	flags = relationship('Flag', secondary='flag_field',
		primaryjoin='flag_field.c.field_id==field.c.id', 
		secondaryjoin='flag_field.c.flag_id==flag.c.id'
	)
	"""returns a list of :class:`Flag` objects that have been associated with this field"""
	count = column_property(select([func.count(Record.id)]).where(Record.field_id==id))
	"""retrieves the number of records associated with an instance"""
	earliest = column_property(select([func.min(Record.t)]).where(Record.field_id==id),deferred=True)
	"""retrieves the timestamp of the earliest record in a timeseries"""
	latest = column_property(select([func.max(Record.t)]).where(Record.field_id==id),deferred=True)
	"""retrieves the timestamp of the latest record in a timeseries"""
	type = column_property(select([Record.type]).where(Record.field_id==id).limit(1))
	"""retrieves the subtype (:attr:`Record.type`) of the field's records (using the first returned one and assuming all are equal)"""
	__mapper_args__ = {'polymorphic_on': subclass, 'polymorphic_identity': 'basic'}
	__table_args__ = (UniqueConstraint('source','station_id','name',deferrable=True,initially='deferred'),)
	
	def __repr__(self):
		s = '<{} {}, id: {}, source: {}'.format(self.__class__.__name__,self.name,self.id,self.source)
		try: s = '{}, {}-{}'.format(s,self.earliest.strftime('%Y/%m/%d'),self.latest.strftime('%Y/%m/%d'))
		except Exception: pass
		return s + '>'

flag_field = Table('flag_field', Base.metadata,
	Column('field_id', Integer, ForeignKey(Field.id,deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'), index=True),
	Column('flag_id', Integer, ForeignKey(Flag.id,deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'), index=True)
)


class ValidationError(Exception):
	pass


class Record_int(Record):
	id = Column(Integer,ForeignKey('record.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'),primary_key=True)
	x = Column(Integer)
	"""recorded physical quantity (type :obj:`int`)"""
	
	@validates('x')
	def validates_x(self, key, value):
		try: x = int(value)
		except TypeError: return None
		except ValueError: raise ValidationError
		else: return x
		
	__mapper_args__ = {'polymorphic_identity': 'int'}
	
class Record_float(Record):
	id = Column(Integer,ForeignKey('record.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'),primary_key=True)
	x = Column(Float)
	"""recorded physical quantity (type :obj:`float`)"""
	
	@validates('x')
	def validates_x(self, key, value):
		try: x = float(value)
		except TypeError: return None
		except ValueError: raise ValidationError
		else: return x
		
	__mapper_args__ = {'polymorphic_identity': 'float'}

class Record_num(Record):
	"""
This record class has a :class:`decimal.Decimal` pythonic type as :attr:`x`, which gets mapped to a :sqla:`Numeric <core/type_basics.html#sqlalchemy.types.Numeric>` type in the database. This guarantees that decimal numbers are saved as such and not as floating point numbers (not all decimal numbers have exact floating point representations).

.. note::
	The CPython extension `cdecimal <https://pypi.python.org/pypi/cdecimal>`_ is faster than the pure python :mod:`decimal` and can be used as a replacement when installed - see this module's ``__init__.py`` file.
	"""
	id = Column(Integer,ForeignKey('record.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'),primary_key=True)
	x = Column(Numeric(10,4))
	"""recorded physical quantity (type :obj:`decimal`)"""
	
	@validates('x')
	def validates_x(self, key, value):
		try: x = Decimal(value)
		except TypeError: return None
		except ValueError: raise ValidationError
		else: return x
		
	__mapper_args__ = {'polymorphic_identity': 'num'}
	

class Aggregate_field(Field):
	"""
This :class:`Field` subclass is specifically intended to save metadata information related to various forms of aggregation applied to 'raw' time series. The two main types of aggregation that come to mind are averaging (e.g. for temperatures) and accumulation (e.g. for precipitation). Some of the attributes of this class are somewhat idiosyncratic to the `DMI <http://www.dmi.dk>`_ data this schema was originally created to be used with. 

An easy way to instantiate an Aggregeate_field is to use its :attr:`parent`::

	ag_field = Aggregate_field(parent=field)
	
which will copy the values of :attr:`~Field.code`, :attr:`~Field.mult`, :attr:`~Field.station_id`, :attr:`~Field.unit`, :attr:`~Field.source`, :attr:`~Field.flags` from the parent 'field', if they are present. If no :attr:`~Field.name` is given, one will be composed from the name of the earlist parent (the last :class:`Field` in the list returned by :meth:`ancestors`) and the :obj:`str` given as keyword argument for :attr:`interval`, or, if the latter is not given, 'aggr'.

.. note::
	Since this is a subclass of :class:`Field`, all of the attributes of the parent class are also present on :class:`Aggregate_field`. Also, you can always use the parent class for querying: ``Session.query(Field).get(1)`` will automatically return an instance of :class:`Aggregate_field` if the object with primary key '1' happens to be an aggregated timeseries.
	"""
	id = Column(Integer, ForeignKey('field.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'), primary_key=True)
	parent_id = Column(Integer, ForeignKey('field.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'), index=True, nullable=False)
	parent = relationship('Field', backref=backref('aggregates',cascade='all, delete-orphan',passive_deletes=True), foreign_keys=[parent_id])
	"""returns the :class:`Field` whose :attr:`~Field.records` comprise the raw data from which the :attr:`Aggregate_field.records` are computed (see also :attr:`Field.aggregates`)"""
	func = Column(String(20), nullable=False)
	"""name of the function used for aggregation as string (max length 20)"""
	interval = Column(String(20), nullable=False)
	"""
time interval of aggregation as string (max length 20)

.. note::
	This is a string rather than a :class:`datetime.timedelta` since certain time inverals common in climatology (in particular, month) will not have a unique value as such an object.
	"""
	zero_hour = Column(Integer, nullable=False, default=6)
	"""
The hour of the day (as :obj:`int`) at which daily aggregation is started, e.g. 6 if the aggregation is carried out from 06:00 to 06:00 UTC. (This is not uncommon; the default here is 6 since that's what `DMI <http://www.dmi.dk>`_ does.)
	"""
	zero_incl = Column(Boolean, nullable=False, default=False)
	"""
:obj:`bool` value indicating whether the aggregation interval is closed at the starting point or not, or, in other words, whether any value recorded at :attr:`zero_hour` is included at the beginning or the end of an interval. The interval is always time-mapped to the included boundary point, i.e. to the beginning if ``True`` and to the end if ``False``.
	"""
	postpone = Column(Interval,nullable=False,default=timedelta(0))
	"""
This is an :obj:`int` value specific to precipitation aggregation for `DMI <http://www.dmi.dk>`_ data and can be ignored for general purposes. It is used in :class:`~databarc.aggregator.Daily_aggregator` in the following way: It appears that most DMI precipitation data is recorded such that at 06:00 and 18:00 UTC, precipitation has been accumulated over the preceeding 12-hour periods, whereas at 00:00 and 12:00 UTC, the value corresponds to a 6-hour accumulation (implying a 'reset' occurring at 06:00 and 18:00 UTC). It appears in general that for meteorological variables which record instantaneous values (e.g. temperature), a daily average computed from such raw values for any given day includes any values starting at and **including** 06:00 UTC of that day up to (but **excluding**) 06:00 UTC the following day. Clearly, accumulated precipitation recorded at 06:00 UTC needs to be mapped to the preceeding day, even though the end of the aggregation interval is included (i.e. the interval closed at the end), which for other variables would imply a time-mapping of the interval to the endpoint (see :attr:`zero_incl`). 

This could of course also be solved by just specifying that intervals always get mapped to the start of the interval (i.e. :attr:`zero_hour`). However, my own investigations seem to indicate that in some rare instances, precipitation values are recorded at other daily times which have an accumulation time longer than 12 hours; more specifically, 15 hours at 09:00 and 21:00 UTC, or more general, according to ``accumulation_interval(hours) = UTC_hour % 12 + 6``. This is a deduction based on comparing average precipitation values by hour of the day, and admittedly doesn't make all that much sense w.r.t. the daily 'reset' deduced above. 

The point is, that if you set ``postpone`` to some :obj:`int` value >0, any accumulation recorded at a time **before** and **not including** the timepoint given by the end of the aggregation interval **plus** ``postpone`` (in hours) still gets counted toward the interval, no matter which boundary the interval is mapped to (start or end). This way, **if** it so happens that there is a missing value at 06:00 UTC **but** a value recorded at, say, 09:00 UTC, that latter value can be used. Whether you use it or not is up to the aggregation function used, see e.g. :func:`~databarc.aggregator.rain_XT`.
	"""

	__mapper_args__ = {'inherit_condition': (id == Field.id), 'polymorphic_identity': 'aggregate'}
	
	def ancestors(self):
		"""returns a list obtained by recursively calling on :attr:`parent` until :class:`Field` without :attr:`parent` is reached (useful since often we aggregate first daily, then monthly, then yearly etc.)"""
		a = [self]
		while hasattr(a[-1],'parent'):
			a.append(a[-1].parent)
		return a[1:]
	
	def __init__(self,**kw):
		try: pp = kw['parent']
		except KeyError: pass
		else:
			for k in ('code','mult','station_id','unit','source'):
				if k not in kw and hasattr(pp,k):
					kw[k] = getattr(pp,k)
		Field.__init__(self,**kw)
		if 'name' not in kw:
			self.name = self.ancestors()[-1].name+' '+kw.get('interval','aggr')


class Processing(Base):
	"""
This class is intended to hold metadata relating to arbitrary 'processing' of 'input fields' that go into some 'output' of class :class:`Processed_field`. At this point, there is only one type of 'processing' it has been used for, namely the application of an additive :attr:`offset` - specifically for the discharge data collected in the 'AKR' catchment near Kangerlussuaq. There, the processing consisted of concatenating many input timeseries in chronological order, for which the functionality of the :attr:`next` and :attr:`prev` is implemented on the class, allowing to switch easily between consecutive timeseries (or rather, their :class:`Field` representations). However, that only works if the corresponding relationships are actually filled in when performing the processing and is somewhat cumbersome. 

:Example:

::

	input2 = input1.next
	input4 = input2.next.next
	input3 = input4.prev
	
Subclasses or replacements for this class should be easily constructable, depending on the needs. 
	"""
	id = Column(Integer, primary_key=True)
	output_id = Column(Integer, ForeignKey('field.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'))
	input_id = Column(Integer, ForeignKey('field.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'))
	input = relationship('Field', foreign_keys=[input_id])
	"""input :class:`Field` whose processing is described by this association object"""
	prev_id = Column(Integer, ForeignKey('processing.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='SET NULL'))
	next = relationship('Processing',foreign_keys=[prev_id],remote_side=[prev_id],uselist=False)
	"""**if** registered on creation, will return the 'next' input :class:`Field` in a temporal sequence"""
	prev = relationship('Processing',foreign_keys=[prev_id],remote_side=[id],uselist=False)
	"""**if** registered on creation, will return the 'previous' input :class:`Field` in a temporal sequence"""
	offset = Column(Float)
	""":obj:`float` additive offset value (although, in principle, you can use it for anything, of course)"""
	use = Column(Boolean,default=True,nullable=False)
	""":obj:`bool` value indicating whether an 'input' timeseries is actually used in the final :class:`Processed_field` -- in case the existence should be recorded nonetheless"""
	notes = Column(Text)
	""":class:`sqla:sqlalchemy.types.Text` type to record a human-readable description of any applied processing"""
	def __repr__(self):
		return '<Processing in: {}, out: {}, offset: {}, use: {}>'.format(self.input_id,self.output_id,self.offset,self.use)

class Processed_field(Field):
	"""
A more general :class:`Field` subclass which can be related to any number of 'input fields' of any :class:`Field` subclass via an :sqla:`association object <orm/basic_relationships.html#association-object>` pattern. This means that a :class:`Processed_field` is related to one or several objects of class :class:`Processing`, while each :class:`Processing` instance contains one relationship to an input :class:`Field` (or subclass). 
	"""
	id = Column(Integer, ForeignKey('field.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'), primary_key=True)
	processing = relationship('Processing',backref='output',cascade='all',passive_deletes=True,primaryjoin='field.c.id==processing.c.output_id')
	"""returns an instrumented list of :class:`Processing` instances"""
	process = Column(PickleType)
	latest_id = column_property(
		select([Processing.id]).where(and_(Processing.output_id==id,Processing.next==None,Processing.prev!=None)),
		deferred=True
	)
	@property
	def latest_input(self):
		return object_session(self).query(Processing).get(self.latest_id)
	__mapper_args__ = {'polymorphic_identity': 'processed'}
	

class Adj_Field(Field):
	id = Column(Integer, ForeignKey('field.id',deferrable=True,initially='deferred',onupdate='CASCADE',ondelete='CASCADE'), primary_key=True)
	time_adj = Column(Interval)
	__mapper_args__ = {'polymorphic_identity': 'time_adjusted'}
