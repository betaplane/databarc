from setuptools import setup

setup(
	name='databarc',
	version='0.1',
	description='Database model for climate time series',
	author='Arno C Hammann',
	author_email='arno@hammann.de',
	license='MIT',
	packages=['databarc'],
	zip_safe=False,
	entry_points={'console_scripts':[
		'databarc-create = databarc.scripts:create'
	]},
	install_requires=[
		'sqlalchemy',
		'geoalchemy2',
		'psycopg2',
		'numpy',
		'python-dateutil',
		'sphinx'
	]
)