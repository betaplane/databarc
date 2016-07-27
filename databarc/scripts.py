def create():
	"""
This function is installed as command-line script ``databarc-create`` during the ``python setup.py install`` process (see usage example below).

It sets up the database schema and saves a configuration file named ``config.ini`` in the directory from which it is executed, containing the url for connecting to the database. 

:param arg1: description
:param arg2: username
:param arg3: database name

The description is used used with :func:`.schema.session` to retrieve a particular connection from ``config.ini``, either if this command is run several times with different parameters or if ``config.ini`` is altered directly. See also :mod:`ConfigParser`.

:Example:

::

	databarc-create description username database

.. warning::
	Database must have been created and associated with user; for now, we don't use a password.
	"""
	import sys
	from databarc.schema import Base
	from sqlalchemy import create_engine
	from ConfigParser import SafeConfigParser
	if len(sys.argv)==1: 
		print create.__doc__
		return
	desc,user,db = sys.argv[1:]
	url = 'postgresql://{user}@/{db}'.format(user=user,db=db)
	config = SafeConfigParser()
	config.read('databarc.cfg')
	if 'db' not in config.sections():
		config.add_section('db')
	config.set('db', desc, url)
	with open('databarc.cfg','w') as f:
		config.write(f)
	eng = create_engine(url)
	if not eng.table_names():
		eng.execute('create extension postgis;')
		Base.metadata.create_all(bind=eng)