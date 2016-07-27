import unittest

def rec(f):
	return [round(r.x,11) for r in f.records]

class TestAggregation(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		from sqlalchemy.orm import sessionmaker, scoped_session
		from sqlalchemy import create_engine
		from databarc.schema import Aggregate_field
		from databarc.aggregator import Daily_aggregator, DMI_daily 
		
		engine = create_engine('postgresql://arno@/DMI')
		cls.connection = engine.connect()
		cls.trans = cls.connection.begin()
		cls.S = scoped_session(sessionmaker(bind=cls.connection))
		cls.S.autoflush = False
		
# 		cls.a = cls.S.query(Aggregate_field).filter_by(station_id=4360, aggregation_interval='day')
		cls.a = cls.S.query(Aggregate_field).filter(Aggregate_field.station_id==4360, Aggregate_field.name.in_(['d day','f day']))
		pp = [f.parent for f in cls.a]
		cls.b = Daily_aggregator.run_threads(pp,DMI_daily,commit=False,num_threads=2)
	
	@classmethod
	def tearDownClass(cls):
		cls.S.close()
		cls.trans.rollback()
		cls.connection.close()
	
	def test_d(self):
		a = [f for f in self.a if f.code=='d'][0]
		b = [b.field for b in self.b if b.field.code=='d'][0]
		self.assertEqual(rec(a),rec(b))
		
# 	def test_t(self):
# 		a = [f for f in self.a if f.code=='t'][0]
# 		b = [b.field for b in self.b if b.field.code=='t'][0]
# 		self.assertEqual(rec(a),rec(b))
# 		
	
		
if __name__ == '__main__':
    unittest.main(exit=False)