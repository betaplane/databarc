"""
Example functions in the simport module
=======================================

A simple example of how to import data into the :mod:`databarc` database is given in the documentation about :ref:`records`.

The two functions in this module illustrate further approaches to importing. The function :func:`from_csv` parses a csv file directly and uses only the :mod:`databarc.schema` module. It is written in particular for the baro- and level-logger files which have the columns ``Date, Time, ms, LEVEL, TEMPERATURE``. 

The function :func:`logger_dir` takes a whole directory, looks for all files ending in ``csv``, and imports them using the :mod:`databarc.importer` module and some basic :class:`~threading.Thread`-based concurrency.

"""

def from_csv(file_path, station_id, level_name, temp_name):
	import csv
	import os
	from databarc.schema import Field, Record_num
	from dateutil.parser import parse
	
	# we use the base name of *file_path* as a 'source' identifier
	source = os.path.basename(file).split('.')[0]
	
	# we instantiate two 'Field' objects
	level = Field(name=level_name, source=source, unit='cm', code='p', mult=1, station_id=station_id)
	temp = Field(name=temp_name, source=source, unit='C', code='t', mult=1, station_id=station_id)
	
	
	with open(file_path) as file:
		for r in csv.reader(file):
			# this attempts to parse the datetime from the first two columns
			# and will just ignore the exception for header lines
			# (dateutil.parser.parse raises ValueError if it doesn't succeed in parsing)
			try: 
				timestamp = parse(','.join(r[:2]))
			except ValueError:
				pass
			else:
				# we proceed only if the timestamp is parsed correctly
				# for each row in the csv file, a new 'Record_num' instance is 
				# created and appended to the Fields' 'records' attributes
				level.records.append(Record_num(x=r[3], t=timestamp, info=0))
				temp.records.append(Record_num(x=r[4], t=timestamp, info=0))
	return level, temp
	

def logger_dir(session, dir, station_id, type, num_threads=6):
	import os
	import databarc.importer as im
	
	# here we choose which 'field_dict' to use
	if type=='baro':
		field_dict = im.baro_logger
		field_dict_2 = im.baro_logger_2
	elif type=='level':
		field_dict = im.level_logger
		field_dict_2 = im.level_logger_2
	else:
		raise Exception('unknown field_dict')
	
	# this defines the function to be executed for each file (more precisely, its path)
	def fun(file_path):
		# we use the base name again as 'source' identifier
		source = os.path.basename(file_path).split('.')[0]
		with open(file_path) as file:
			Imp = im.Importer(session, source, station_id, field_dict, file)
			
			# we use this check here for the baro- and level-logger case where the first 
			# column has row numbers - the Importer raises an UnparsedLineLimit exception
			# if after a 100 lines no datetime has been parsed correctly
			try: 
				Imp.do(session)
			except im.UnparsedLineLimit:
				# IMPORTANT! we reset the open file to the beginning
				file.seek(0)
				
				# we use the second field_dict
				Imp = im.Importer(session, source, station_id, field_dict_2, file)
				Imp.do(session)

			
	# this obtains only the files in directory *dir* which end in 'csv'
	files = [os.path.join(dir,f) for f in os.listdir(dir) if f[-3:]=='csv']
	
	# here we call 'run_threads' with the list of files and the function 'fun' as arguments
	# if run interactively, 'run_threads' will return a list of all the importer instances used
	# which we return here to the caller
	return im.import_with_threads(files, fun, num_threads)
	