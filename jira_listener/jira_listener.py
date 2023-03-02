import os
import sys
import time

import pickle

from requests.auth import HTTPBasicAuth
import gwa_jira
import settings
from helpers import logg, select_processes

# Configure Logs
log_folder = os.path.join(os.path.join(settings.aqa_dir, 'jira_listener'), 'logs/')
mainlogger = logg.default_logger('__main__')
mainlogger.info(f'Initiating JIRA listener')
logger = mainlogger.getChild('JIRAlistener')
logger = logg.configure_handlers(logger, log_folder, maxBytes=200 * 1024 * 1024, backupCount=5)
sys.stdout = open(os.path.join(log_folder, 'std.out'), 'a')
sys.stderr = open(os.path.join(log_folder, 'err.log'), 'a')
logger.info('Logger Configured.')


def queue_issue(issue: gwa_jira.Issue):
    '''Takes a gwa_jira.Issue object (https://github.com/tna-webarchive/ukgwa-tools/blob/main/src/gwa_jira/Issue.py)
    and runs it through a few tests. If conditions are right, the Issue will be
    added to the queue directory (i.e. the Issue objetc pickled and saved in the queue with the filename matching the crawl ID'''

    crawl_dir = os.path.join(settings.aqa_dir, issue.crawl_id)
    queue_path = os.path.join(settings.aqa_queue, issue.crawl_id)

    # Is the Crawl ID numeric? Mainly filters out social links
    if not issue.crawl_id.isnumeric():
        logger.info(f'{issue.crawl_id} not numeric. Skipping')
        return None

    # If the Ticket 'Done'? If so, remove the crawl directory and queue (if they exist).
    # This helps to tidy the crawl directory (if in the middle of autoQA process, it might error without a crawl directory
    # This is desired if the status changes to Done.
    if issue.data['status']['name'] == 'Done':
        logger.info(f'{issue.crawl_id}\tStatus = Done. Removing directory {crawl_dir}')
        os.system(f'rm -f {queue_path}')
        os.system(f'rm -rf {crawl_dir}')
        return None

    # Are there sub-proceses to run on the crawl? If not, remove the crawl from the queue if already there.
    if not select_processes.processes_to_run(issue):
        logger.info('No processes to run.')
        os.system(f'rm -f {queue_path}')
        return None

    # Is the crawl already in the queue? If so and the issue has not been updated since, leave in the queue.
    if os.path.exists(queue_path):
        logger.info(f'{issue.crawl_id} in queue.')
        with open(queue_path, 'rb') as source:
            queued_issue = pickle.load(source)
        if queued_issue.data['updated'] == issue.data['updated']:
            logger.info(f'{issue.crawl_id}\tNo updates since joining queue')
            return None
        else:
            logger.info(f'{issue.crawl_id}\tTicket updated. Updating queued crawl.')

    # Crawls which make it here are added to the queue. (or updated if already in the queue)
    with open(queue_path, 'wb') as dest:
        pickle.dump(issue, dest, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f'{issue.crawl_id}\tAdded to queue')


def get_updated_issues(since_mins: int, auth: HTTPBasicAuth) -> list:
    '''Calls the JIRA API for updated issues. The time span of the check is defined in the since_mins param.
    The auth param is the JIRA credentials in a HTTPBasicAuth object.
    JQL only takes whole numbers (hence int requirement for the param).
    The issues are returned as a gwa_jira Issue objects
    (https://github.com/tna-webarchive/ukgwa-tools/blob/main/src/gwa_jira/Issue.py)'''

    JQL = f'project = UKGWAC AND updated >= -{since_mins}m ORDER BY updated ASC'
    logger.debug(f'Getting tickets updated within the last {since_mins} minute(s).\n{JQL}')
    updated_issues = gwa_jira.get_all(JQL, auth=auth, return_objects=True)
    return updated_issues



def listen_populate_queue(auth: HTTPBasicAuth, wait_seconds: int = 60):
    '''Listens for updated JIRA tickets. Updated tickets are sent to the queue if necessary.
    The auth param is the JIRA credentials in a HTTPBasicAuth object.
    The function runs in a never-ending loop, waiting the number of seconds in the wait_seconds param
    between calls. The JQL query asks for issues updated since the last call and so no updates can be missed.'''

    last_call = 0

    while True:
        last_call = last_call if last_call else time.time() - 5256000           #default is 2 months ago (5256000 seconds)
        mins_since_last_check = int((time.time() - last_call)/60)+1             # int(number) + 1 ceils the number (i.e if the last call happened 1.2 mins ago, we'd need to check for updates within 2 mins.
        last_call = time.time()

        try:
            updated_issues = get_updated_issues(mins_since_last_check, auth)
            logger.info(f'{len(updated_issues)} ticket(s) updated.') if len(updated_issues) else logger.debug(f'{len(updated_issues)} ticket(s) updated.')
            for issue in updated_issues:
                queue_issue(issue)
        except:
            continue

        logger.debug(f'Waiting {int(wait_seconds/60)}min(s) before checking again.')
        time.sleep(wait_seconds)
