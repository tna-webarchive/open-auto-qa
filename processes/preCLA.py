import os
import sys
import pickle
import datetime

import gwa_qa
import gwa_functions as f
from helpers import logg


process = __file__.rsplit('/', 1)[-1].replace('.py', '')  # NAME OF PROCESS
date = datetime.datetime.now().strftime('%Y%m%d')

##################################################################################################
#### NAME FUNCTION TO MATCH MODULE NAME e.g. template.py module contains tempalte() function ####
##################################################################################################

def preCLA(crawl):
    crawl.reload()

    check_limit = 10000

    process_folder = os.path.join(crawl.directory, f'{date}-{process}/')

    os.system(f'mkdir -p {process_folder}')

    processlogger = crawl.logger.getChild(process)
    log_folder = f'{process_folder}logs/'
    processlogger = logg.configure_handlers(processlogger, log_folder, maxBytes=200*1024*1024, backupCount=5)
    sys.stdout = open(f'{log_folder}std.out', 'a')
    sys.stderr = open(f'{log_folder}err.log', 'a')
    try:
        processlogger.info('Generating RUD.')
        crawl.rud = gwa_qa.crawl_log_rud(crawl.crawl_log)
        processlogger.info('RUD Generated.')

        processlogger.info('Checking Live Site.')
        check_codes = [x for x in crawl.rud.present if x not in list(range(200,400)) + [1, 404]]
        crawl.rud.get_patchlist(codes=check_codes, scope=crawl.scope,
                                check_limit=check_limit, chunk=15, timeout=300,
                                rate=2, extra=100, logger=processlogger)
        processlogger.info('Patchlist Created.')

        processlogger.info('Saving Crawl Data.')
        with open(os.path.join(process_folder, 'crawl.pkl'), 'wb') as handle:
            pickle.dump(crawl, handle, protocol=pickle.HIGHEST_PROTOCOL)

        ## Uses ukgwa-tools obj_to_files function to save data locally
        f.obj_to_files(obj=crawl.rud, folder= os.path.join(process_folder, 'rud-data/'),
                                            include=('counts', 'checklist', 'patchlist', 'report', 'unchecked',
                                                     'discarded', 'checked_rud', 'url_groups', 'out_of_scope'))
        processlogger.info('Crawl Data Saved.')


        comment = 'pre-TNA crawl log analysis:'


        #### LONG CHECKLIST ####
        if len(crawl.rud.checklist) > check_limit:
            comment += '\nLong Checklist, In-Scope URLs Grouped by directory path.'

        #### SHORT CHECKLIST ####
        else:
            comment += '\nShort Checklist, all In-Scope Errors Checked. '
        comment += f'\n{len(crawl.rud.checklist)} URL(s) checked. {len(crawl.rud.patchlist)} to patch.'


        ### PATCHLIST UPLOAD ###
        if len(crawl.rud.patchlist) > 0:
            try:
                crawl.issue.attach_doc(f'pre-tna-cla-patchlist-{crawl.id}.txt', f'{process_folder}rud-data/patchlist.txt')
                processlogger.info('Patchlist Attached')
                comment += '\nPatchlist Attached.'
            except:
                processlogger.exception('Patchlist Upload Failed')
                comment += '\nPatchlist Upload Failed.'

        try:
            crawl.issue.add_comment(comment)
            processlogger.info('Comment Posted')
        except:
            processlogger.exception('Comment/Label Update Failed')


        processlogger.info(f'Process {process} Finished.')
    except:
        raise
    finally:
        crawl.issue.add_label(f'{process}-complete')        #Ensures process won't run and fail over and over