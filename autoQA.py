import os
import sys

import settings
from objects import Crawl
from helpers import select_processes, logg


all_processes = [x.replace('.py', '') for x in os.listdir(os.path.join(settings.root, 'processes')) if x.endswith('.py')]
for process in all_processes:
    exec(f'from processes import {process}')


def autoQA(crawl_id: int):
    '''Takes a crawl ID and runs autoQA on it. a Crawl object is created which bundles all a crawl's realted metdata
    i.e. jira issue, crawl logs/specification etc. The select processes module produces a list of sub-processes to run
    which are run in turn.
    If a process fails, the exception is caught and relayed to the JIRA issue.'''

    # Configures logger
    mainlogger = logg.default_logger('__main__')
    mainlogger.info(f'Initiating AQA for {crawl_id}')

    crawl_dir = os.path.join(settings.aqa_dir, str(crawl_id))
    os.system(f'mkdir -p {crawl_dir}')

    logger = mainlogger.getChild(str(crawl_id))
    log_folder = os.path.join(crawl_dir, 'logs/')
    logger = logg.configure_handlers(logger, log_folder, maxBytes=200 * 1024 * 1024, backupCount=5)
    sys.stdout = open(f'{log_folder}std.out', 'a')
    sys.stderr = open(f'{log_folder}err.log', 'a')

    logger.info('Logger Configured.')

    try:
        logger.info('Loading Crawl.')
        crawl = Crawl.Crawl(crawl_id)

        # Checks that there are sub-processes to run. If not, halts autoQA
        to_run = select_processes.processes_to_run(crawl.issue)
        if not to_run:
            mainlogger.info(f'No sub-processes to run for {crawl_id}. Halting autoQA.')
            return None

        crawl.directory = crawl_dir
        logger.info('Crawl loaded.')
        try:
            # Runs each process in turn added a comment to the issue before, after each process and at the end of the whole autoQA process.
            logger.info(f'To run: {to_run}')
            crawl.issue.add_comment(f'autoQA starting.\nautoQA will run: {to_run}')
            failed_processes = []
            for process in to_run:
                try:
                    logger.info(f'Initiating {process}')
                    exec(f'{process}.{process}(crawl)')
                    logger.info(f'Process {process} complete.')
                except Exception as e:
                    crawl.issue.add_comment(f'{process} failed:\n{repr(e)}')
                    logger.exception(f'Process {process} Errored. Moving onto next process.')
                    failed_processes.append(process)

            successful_processes = [process for process in to_run if process not in failed_processes]

            logger.info(f'autoQA Finished. Successful Processes: {successful_processes}. Failed Processes: {failed_processes}')
            crawl.issue.add_comment(f'autoQA Finished.\nSuccessful Processes: {successful_processes}.\nFailed Processes: {failed_processes}')

        except Exception as e:
            crawl.issue.add_comment(f'autoQA failed:\n{repr(e)}')
            logger.exception('Aborting.')
    except:
        logger.exception('Error loading Crawl. Aborting.')

    sys.stdout.close()
    sys.stderr.close()


# Can be run outside of main.py by calling on cmd line with a crawl ID as the argument. (Mainly for debugging)
if __name__ == '__main__':
    autoQA(int(sys.argv[1]))