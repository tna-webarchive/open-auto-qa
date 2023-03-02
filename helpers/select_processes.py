import gwa_jira
import settings
import re

def processes_to_run(issue: gwa_jira.Issue) -> list:
    '''Takes a gwa_jira Issue object (https://github.com/tna-webarchive/ukgwa-tools/blob/main/src/gwa_jira/Issue.py)
    and checks which autoQA sub processes need to be run. The sub-processes correspond to modules in the processes directory.'''
    processes = []

    if 'run-PDFflash' in issue.data['labels'] and 'PDFflash-complete' not in issue.data['labels']:
        processes.append('PDFflash')

    if 'CLA-complete' not in issue.data['labels']:
        if issue.data['status']['name'] in ['Ready For QA', 'Partner QA']:
            processes.append('CLA')

    guid_regex = re.compile(' \([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\)')
    ### GET DIFFEX FILE (if two diffex.csv have been uploaded, one will include a guid. if the original is deleted the regex ensures the new one is picked up.
    attachments = [guid_regex.sub('', file['filename']).lower() for file in issue.data['attachment']]

    #attachments = [x.get('filename').lower() for x in issue.data['attachment']]
    if 'diffex-complete' not in issue.data['labels'] and settings.diffex_filename in attachments:
        processes.append('diffex')


    return processes

