import os
import sys
import datetime

from helpers import logg

process = __file__.rsplit('/', 1)[-1].replace('.py', '')  # NAME OF PROCESS
date = datetime.datetime.now().strftime('%Y%m%d')

###################################################################################################
#### NAME FUNCTION TO MATCH MODULE NAME e.g. template.py module contains tempalte() function ####
###################################################################################################

def template(crawl):
    process_folder = os.path.join(crawl.directory, f'{date}-{process}/')

    os.system(f'mkdir -p {process_folder}')

    ## CONFIGURE LOGGERS ##
    processlogger = crawl.logger.getChild(process)
    log_folder = f'{process_folder}logs/'
    processlogger = logg.configure_handlers(processlogger, log_folder, maxBytes=200*1024*1024, backupCount=5)
    sys.stdout = open(f'{log_folder}std.out', 'a')
    sys.stderr = open(f'{log_folder}err.log', 'a')

    try:
    ###########  INSERT  ###########
    ###########  PROCESS ###########
    ###########  STEPS   ###########


        processlogger.info(f'Process {process} Finished.')
    except:
        raise
    finally:
        crawl.issue.add_label(f'{process}-complete')        #Ensures process won't run and fail over and over