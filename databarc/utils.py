#!/usr/bin/env python
from databarc.schema import *


def recX(session,op,value):
	from sqlalchemy.orm import with_polymorphic
	from sqlalchemy import or_
	return session.query(with_polymorphic(Record,'*')).filter(or_(*[m.c.x.op(op)(value) for m in Record.__mapper__.polymorphic_map.values()]))


def flags(session, flags):
	"""
Check if a flag already exists in database; create it if not.

:param session: a SQLAlchemy session object
:type session: :class:`~sqla:sqlalchemy.orm.session.Session`

:param list flags: a list of :obj:`dicts<dict>` describing :class:`~databarc.schema.Flag` objects (see :ref:`remarks for importer module<flags>`)

:return: a list of :class:`~databarc.schema.Flag` objects
:rtype: list
	"""
	fl = []
	for f in flags:
		try: 
			flag = Flag(**f)
			session.add(flag)
			session.commit()
		except Exception: 
			session.rollback()
			flag = session.query(Flag).filter_by(**f).one()
		fl.append(flag)
	return fl


def latest(obj=Field,lim=10):
	from sqlalchemy import desc
	return Session.query(obj).order_by(desc(obj.id)).limit(lim).all()
	
def distinct(obj):
	return Session.query(obj).distinct().all()

def field(name,station):
	from sqlalchemy import text
	q = Session.query(Field).filter(Field.station_id==station,text('name similar to :name')).params(name=name)
	return q.all() if q.count()>1 else q.one()
	
def record(field,y=0,m=0,d=0,h=-1):
	from datetime import datetime
	if h>=0:
		q = Session.query(Record).filter(Record.field_id==field.id,Record.t==datetime(y,m,d,h))
	else:
		q = Session.query(Record).filter(Record.field_id==field.id,Record.t==datetime(y,m,d))
	return q.one()

def around(field,date,days=1):
	from dateutil import parser
	from datetime import timedelta
	d = date
	try: d = parser.parse(date)
	except: pass
	return Session.query(Record).filter(Record.field_id==field.id, \
		Record.t>=d-timedelta(days=days),Record.t<=d+timedelta(days=days)).all()

def station(a):
	if type(a)==int:
		for s in Session.query(Station).filter(Station.station_id==a):
			print s
	else:
		for s in Session.query(Station).filter(Station.name.ilike('%'+a+'%')):
			print s
		
def proximity(s, dist):
	return Session.query(Station).filter(func.st_distance_sphere(\
		cast(s.loc, Geometry('POINT', 4326)), cast(Station.loc, Geometry('POINT', 4326)))<dist).all()


def plot(*args):
	import matplotlib.pyplot as plt
	fig = plt.figure()
	for f in args:
		t = [r.t for r in records]
		x = [r.x for r in records]
		plt.plot_date(t,x)
	fig.show()


def compare(*fields, **kwargs):
	"""
compare(*fields [, diff=False])
Produce a table of several fields' records so that the rows contain the values at the 
same time. The first column contains the times. Only times which are present in all 
fields are returned.

:param fields: list of schema.Field objects
:param diff: if True, return only times where NOT all records are numerically identical
	
:return: nested list of rows
:rtype: list
	"""
	from sqlalchemy import select, cast, Date
	from sqlalchemy.orm import object_session
	from sqlalchemy.sql import column
	
	session = object_session(fields[0])
	t = column('t')
	R = Record.__table__
	def sel(field):
		T = session.query(Record).filter(Record.field_id==field.id).first().__table__
		return select([t,column('x')]).select_from(R.join(T)).where(R.c.field_id==field.id).alias()

	for i,f in enumerate(fields):
		if i==0:
			s0 = sel(f)
			s = select([s0.c.t,s0.c.x]).order_by(t)
		else:
			s1 = sel(f)
			if kwargs.get('datecast',False):
				s = s.column(s1.c.x).where(cast(s0.c.t,Date)==cast(s1.c.t,Date))
			else:
				s = s.column(s1.c.x).where(s0.c.t==s1.c.t)
			if kwargs.get('diff',False):
				s = s.where(func.round(s0.c.x)!=func.round(s1.c.x))
	if kwargs.get('plot',False):
		if len(fields)!=2:
			print "Works only with exactly 2 fields as input."
			return None
		import matplotlib.pyplot as plt
		a = float(fields[0].mult) * fields[0].units.convert(fields[1].units)
		b = float(fields[1].mult) * fields[1].units.convert(fields[0].units)
		l = Session.execute(s).fetchall()
		fig = plt.figure(figsize=(6.2,6))
		plt.scatter([float(r[1])*a for r in l],[float(r[2])*b for r in l])
		plt.xlabel(fields[0].name+' '+str(fields[0].station_id))
		plt.ylabel(fields[1].name+' '+str(fields[1].station_id))
		try: 
			x = kwargs['xlim']
			y = x
		except:
			x = plt.xlim()
			y = plt.ylim()
		plt.plot(x,x)
		fig.axes[0].set_xlim(x)
		fig.axes[0].set_ylim(y)
		fig.show()
	else:
		return session.execute(s).fetchall()

class _comp(object):
	def __init__(self,f,g,delta=0,mult=1):
		self.x = []
		self.y = []
		self.t = []
		self.diff = []
		self.zero = 0
		self.trace = 0
		self.err = 0
		i = -1
		n = 0
		t = [r.t.date() for r in g.records]
		for r in f.records:
			try: i = t.index(r.t.date()+timedelta(days=delta),i+1)
			except: pass
			else:
				self.x.append(r.x)
				self.y.append(mult*g.records[i].x)
				self.t.append(r.t)
				d = self.x[-1]-self.y[-1]
				if d!=0:
					if self.x[-1]+self.y[-1]==-1: self.trace += 1
					else:
						l = [r.t,self.x[-1],self.y[-1]]
						for s in r.binned:
							l.extend((s.t.hour,s.info,s.x))
						self.diff.append(l)
				elif self.x[-1]==0 and self.y[-1]==0: self.zero += 1
				self.err += d**2
				n += 1
		self.err = (float(self.err)/n)**.5

def compraw(*args):
	ln = len(args)-1
	sel = ['select m0.t as t, ', ' from ', '', ' where ']
	for i,n in enumerate(args):
		s = str(i)
		sel[0] += 'm'+s+'.x'
		sel[1] += n.type.lower() + ' as m' + s + ', '
		sel[2] += 'record as r' + s
		sel[3] += 'r'+s+'.field_id='+str(n.id)+' and m'+s+'.id=r'+s+'.id and '
		if i>0:
			sel[3] += 'm'+s+'.t=m'+str(i-1)+'.t'
			if i<ln:
				sel[3] += ' and '
		if i<ln:
			sel[0] += ', '
			sel[2] += ', '
	return Session.execute(''.join(sel)+' order by t').fetchall()


def replace_attr(obj,attr,old,new):
	Session.execute(obj.__table__.update().values(**{attr:new}).where("{}='{}'".format(attr,old)))
	Session.commit()

