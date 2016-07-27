Installation
============	
Either run::

	python setup.py install

or::

	python setup.py develop
	
(the latter creates a symlink to this source directory in ``site-packages`` instead of copying the files, and if changes are made to the module, they are immediately usable when importing it).

In order to create a fresh database scheme, you can use, from the command line::

	databarc-create description username database
	
but be aware that this only works after the (PostgreSQL) database has been created and is accessible by the given user.

Development and maintenance
---------------------------
If you want to modify the basic database model defined in :mod:`.schema`, you should probably use a version management system such as `alembic <https://pypi.python.org/pypi/alembic>`_. 

Here are some hints as to how to use alembic