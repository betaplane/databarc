try: import cdecimal,sys
except ImportError: pass
else: sys.modules["decimal"] = cdecimal

import logging.config
import logging


class Logger(logging.getLoggerClass()):
	def addHandler(self, hdlr):
		if hdlr.name=='out':
			fmt = hdlr.formatter
			hdlr = logging.FileHandler('{}.log'.format(self.name), delay=True)
			hdlr.setFormatter(fmt)
		super(Logger,self).addHandler(hdlr)
					
logging.setLoggerClass(Logger)


logging.config.dictConfig({ 'version': 1,
	'formatters':{'general': {'format': '%(levelname)s: %(threadName)s: %(message)s'}},
	'handlers': {
		'parsing': {'class': 'logging.FileHandler', 'formatter': 'general', 'filename': 'databarc.importer.parsing.log', 'mode': 'w', 'delay': True},
		'screen': {'class': 'logging.StreamHandler', 'formatter': 'general'},
		'out': {'level': 'INFO', 'class': 'logging.NullHandler', 'formatter': 'general'},
# 		'debug': {'class': 'logging.FileHandler', 'formatter': 'general', 'filename': 'importer_debug.log', 'mode': 'w'}
	},
	'loggers': {
		'parsing': {'level': 'INFO', 'handlers': ['parsing'], 'propagate': False},
		'databarc.importer': {'level':'DEBUG', 'handlers': ['out']},
# 		'databarc.aggregator': {'level':'DEBUG', 'handlers': ['out']},
# 		'debug': {'level':'DEBUG', 'handlers': ['debug']}
	},
	'root': {'level':'DEBUG', 'handlers':['screen']}
})