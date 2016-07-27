*********
Importing
*********

.. automodule:: databarc.simport

	.. function:: from_csv(file_path, station_id, level_name, temp_name)
		
		If you wanted to use this function directly, you could do::
		
			level, temp = from_csv('barologger.csv', 4321, 'AP', 'AT')
			Session.add_all( (level, temp) )
			Session.commit()
		
		.. _session:
			
		Where *Session* is either a :class:`~sqla:sqlalchemy.orm.scoping.scoped_session` returned by :func:`databarc.schema.session` or some otherwise obtained :class:`~sqla:sqlalchemy.orm.session.Session` object., ``barologger.csv`` the name of the file to be parsed, ``4321`` the :attr:`~databarc.schema.Field.station_id` and ``AT`` and ``AP`` the :attr:`names <databarc.schema.Field.name>` of the :class:`Fields <databarc.schema.Field>` to be created.

	The source code of this function demonstrates how the actual importing works:

	.. literalinclude:: ../databarc/simport.py
		:pyobject: from_csv

	.. warning::
		The :func:`dateutil:dateutil.parser.parse` function which is used here returns the current date if an empty string is given, which may upset the error-catching logic, so more care may be needed in some circumstances.
	
	
	.. function:: logger_dir(session, dir, station_id, type, num_threads=6)
		
		Here we import a whole directory with the :mod:`~databarc.importer` module. For this, we do need a :ref:`session <session>` to start with, and it has to be a :class:`~sqla:sqlalchemy.orm.scoping.scoped_session`, since the 'scoping' provides :sqla:`thread-local context <orm/contextual.html>`::
			
			from databarc.schema import session
			
			# this returns a SQLAlchemy *scoped_session*
			Session = session()
			
			logger_dir(Session, 'levellogger_directory', 2002, 'level')

	Let's have a look at what this function does:
	
	.. literalinclude:: ../databarc/simport.py
		:pyobject: logger_dir
	
	We are using the :ref:`field_dicts <field_dict>` for the baro- and level-loggers that are :ref:`defined in the import module <prov_fdicts>`. :exc:`~databarc.importer.NothingParsedError` is used in this example because some of the logger files have a first column containing row numbers whereas others don't. The parsing of the file with the 'regular' field_dict fails and a :exc:`~databarc.importer.NothingParsedError` is raised; then a new field_dict with keys shifted by one is created and a new :class:`~databarc.importer.Importer` instantiated with the new field_dict. (We could of course also just have defined a second field_dict, or adjusted the files beforehand.)
	
	
.. automodule:: databarc.importer
	
Importer API
============
	
	.. autoclass:: Importer
		:members:
	
	.. autofunction:: import_with_threads
	
	.. _prov_fdicts:
	
Predefined 'Field_dicts'
------------------------
	
	.. data:: DMI_subd
		
		:ref:`field_dict<field_dict>` for `DMI <http://www.dmi.dk>`_ sub-daily data
	
	.. data:: NCDC_subd
		
		:ref:`field_dict<field_dict>` for `NCDC sub-daily data <http://www7.ncdc.noaa.gov/CDO/cdo>`_ (simplified version)
		
	.. data:: NCDC_isd_lite
		
		:ref:`field_dict<field_dict>` for NCDC isd-lite data
		
	.. data:: baro_logger
		
		:ref:`field_dict<field_dict>` for baro-logger without row number first column
		
	.. data:: baro_logger_2
		
		:ref:`field_dict<field_dict>` for baro-logger with row number first column
		
	.. data:: level_logger
		
		:ref:`field_dict<field_dict>` for level-logger without row number first column
	
	.. data:: level_logger_2
		
		:ref:`field_dict<field_dict>` for level-logger with row number first column
		