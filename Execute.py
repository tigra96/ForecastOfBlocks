import pymssql
import smtplib
import datetime
import traceback
import pandas as pd

from calendar 		import monthrange
from datetime 		import datetime as dt
from sqlalchemy 	import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

ENGINE_utf8_zeus = create_engine('mssql+pymssql://palomars\Zeus/Vimb')

Session = scoped_session(sessionmaker(bind=ENGINE_utf8_zeus))
s = Session()
			
def executeScriptsFromFile(logger, filenames=['From_Old_VIMB.sql', 'From_New_VIMB.sql']):
	"""
	Данная функция выполняет sql скрипты 
	для объединения данных со старого и 
	нового вимба
	"""

	# Open and read the file as a single buffer
	for filename in filenames:
		fd = open(filename, 'r')
		sqlFile = fd.read()
		fd.close()

		# all SQL commands (split on ';')
		sqlCommands = sqlFile.split(';')

		# Execute every command from the input file
		for command in sqlCommands:
			# This will skip and report errors
			# Fr example, if the tables do not yet exist, this will skip over
			# the DROP TABLE commands
				
			result = s.execute(command)
			s.commit()
			s.close()

		logger.info( u'Command: ' + 
					  filename + 
					  ' is executed ! ' + 
					  str(result.rowcount) + 
					  ' row(s) affected !' )
				

				
				
