#   logging 
#   when some thing is happened ,  what is the level if the important 
#   where the log happened 

import logging
#   DEBUG -->   Developer 
#   INFO  -->   General information
#   WARNING  -->  Something unexpected happened
#   ERROR  -->  More serious problem
#   CRITICAL  -->  Serious error , program may be unable to continue

logging.basicConfig(filename='app.log', filemode='w', 
                    level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

logging.debug('This is a debug message')
logging.info('This is an info message')
logging.warning('This is a warning message')
logging.error('This is an error message')
logging.critical('This is a critical message')