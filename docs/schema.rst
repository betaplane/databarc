Database schema description
===========================
.. automodule:: databarc.schema
	
	Getting a database session
	--------------------------
	.. autofunction:: session
	
	.. _dbmodel:
	
	The database model
	------------------
	Almost all model entities have a primary key (usually called ``id``) which identifies individual rows in a table. Except for a few tables that are defined directly and serve as many-to-many :sqla:`association tables <orm/basic_relationships.html#many-to-many>`, they are defined as subclasses of the :sqla:`declarative base class <orm/extensions/declarative/basic_use.html>`. Class attributes correspond either directly to columns in the database table, or to relationships with other entities. For example, the :class:`Record` entity has a :sqla:`many-to-one relationship <orm/basic_relationships.html#many-to-one>` with the :class:`Field` entity. This is reflected, on the :class:`Record` side, with two attributes: :attr:`Record.field_id` and :attr:`Record.field`. The former corresponds to the row's foreign key (which is what the database uses to look up the relationship: :attr:`Record.field_id` has the same value as the primary key :attr:`Field.id` it points to). The latter, when accessed, returns an actual :class:`Field` python object populated with the data retrieved from the database. In the documentation, I will usually ignore the ``..._id`` attributes and focus on the actual relationships.
	
	Fields
	^^^^^^
	.. autoclass:: Field
		:members:
		
		.. attribute:: aggregates
			
			returns a list of :class:`Aggregate_fields<Aggregate_field>` which use this field as input (see also :attr:`Aggregate_field.primary_parent`)
	
	.. _records:
	
	Records
	^^^^^^^
	When creating a record, a :ref:`subclass <record_sub>` should be used, depending on what :sqla:`database type <core/type_basics.html>` is desired for the measured quantity. 
	
	Create a new record::
		
		from datetime import datetime
		from databarc.schema import Field, Record_int, session
		
		# create a 'Field' instance as metadata container
		field = Field(name='precip', code='r', station_id=1001)
		
		# create the actual record, here with 'Record_int' subclass
		rec = Record_int(t=datetime.now(), x=1)
		
		# append new record to Field's 'records' attribute
		field.records.append(rec)
		
		# get a session
		Session = session()
		
		# this does not yet modify the database - it only affects the Python side
		Session.add(field)
		
		# now the change is committed to the database
		# if a Session.rollback() were issued before, nothing would happen
		Session.commit()
		
	The class attribute :attr:`Field.records` is an 'instrumented' list which, when accessed, returns the records related to a given field through the :attr:`Record.field_id` foreign key. When elements are added to the instrumented list and the :class:`Field` instance 'field' is added to the :class:`session`, they will automatically be added to the database (and, upon :meth:`~sqla:sqlalchemy.orm.session.Session.commit`, persisted). See also :sqla:`adding and updating objects <orm/tutorial.html#adding-and-updating-objects>`.
	
	An alternative sequence of commands with the same result would be (assuming the same 'field' object as above)::
	
		rec = Record_int(t=datetime.now(), x=1, field_id=field.id)
		Session = session()
		Session.add_all((field,rec))
		Session.commit()
	
	.. note::
		A call to :meth:`session.rollback() <sqla:sqlalchemy.orm.session.Session.rollback>` will undo any changes to the session since the last :meth:`~sqla:sqlalchemy.orm.session.Session.commit` (e.g. through :meth:`~sqla:sqlalchemy.orm.session.Session.add`). If an error occurs during the :meth:`~sqla:sqlalchemy.orm.session.Session.commit` stage, a :meth:`~sqla:sqlalchemy.orm.session.Session.rollback` is mandatory in order to keep using the session. See also :sqla:`rolling back <orm/session_basics.html#rolling-back>`.
	
	.. autoclass:: Record
		:members:
		
		.. attribute:: field
			
			Returns the :class:`Field` object this record is attached to.
	
	.. _record_sub:
	
	Record subclasses
	^^^^^^^^^^^^^^^^^
	
	.. autoclass:: Record_int
		:members:
		
	.. autoclass:: Record_float
		:members:
		
	.. autoclass:: Record_num
		:members:
	
	Flag values
	^^^^^^^^^^^
	
	.. autoclass:: Flag
		:members:
		
	Aggregated and processed fields
	^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
	One of the purposes of this database schema is to keep track of any potential processing applied to a given time series (as represented by a :class:`Field`). For example, if daily averages are computed from hourly data and the resulting time series is saved to a database table, relationships can be established between the resulting daily average and the hourly data that the average is taken over. If it is discovered that somewhere along the line an error must have occurred, detailed investigations will be possible.
	
	For such aggregated or otherwise processed (e.g., bias-corrected) time series, subclasses of :class:`Field` can be created to hold additional metadata about the specific processing applied. So far, the schema contains two such subclasses: :class:`Aggregate_field` and :class:`Processed_field`.
	
	.. autoclass:: Aggregate_field
		:members:
		
	.. autoclass:: Processed_field
		:members:
	
	.. autoclass:: Processing
		:members:
		
		.. attribute:: output
		
			returns :class:`Processed_field` instance for which :attr:`input` is an 'input'
			
		