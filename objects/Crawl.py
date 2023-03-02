import os
import pandas as pd
import logging
import boto3

import settings
from env import secrets

import gwa_aws
import gwa_jira
import gwa_qa

main = logging.getLogger('__main__')

class Crawl:
    '''Crawl object.
    Loads data and metadata associated with a crawl.
    Currently jira issue and logs.
    Initiates a folder structure associated with the crawl.

    #TODO: XML1e, CDXs, WARCs.
    :param: crawl_id - TNA crawl ID'''

    aws_session = boto3.Session(
        aws_access_key_id=secrets.S3ACCESS_KEY,
        aws_secret_access_key=secrets.S3SECRET_KEY)

    def __init__(self, crawl_id: int):
        self.id = int(crawl_id)
        self.directory = os.path.join(settings.aqa_dir, str(self.id))
        self.logger = logging.getLogger(f'__main__.{self.id}')  # gets logger if exists, if not creates unconfigured logger
        self.issue = self.load_issue()
        self.add_logs_to_description()
        self.log_files = self.load_logs(verbose=True)
        self.scope = self.load_scope()

    def add_logs_to_description(self):
        complete_label = 'aqa-logs-in-description'
        if complete_label in self.issue.labels:
            return None
        self.issue.reload()
        s3_aqa_logs = f'https://ENTER-YOUR-DIRECTORY&prefix={self.id}/'

        desc_seperator = {'type': 'paragraph',
                          'content':
                              [{'type': 'hardBreak'},
                               {'type': 'text',
                                'text': '------------------------------------------------------------\nLinks to autoQA logs for TNA use:'}]}

        logs_desc_link = {'type': 'paragraph',
                              'content':
                                  [{'type': 'text',
                                    'text': 'autoQA logs for this crawl (info.log recommended) (syncs every 5 minutes - multiples of 5)',
                                    'marks': [{'type': 'link',
                                               'attrs': {'href': s3_aqa_logs}}]}]}


        desc = self.issue.data['description']
        desc['content'] += [desc_seperator, logs_desc_link]
        self.issue.update_field('description', desc, 'set')
        self.issue.add_label(complete_label)

    def reload(self):
        self.__init__(self.id)

    def load_issue(self) -> gwa_jira.Issue:
        '''Connects to Jira and loads issue data'''
        jql = f'labels = client_ref:{self.id}'
        results = gwa_jira.get_all(jql=jql, auth=secrets.JIRAauth, return_objects=True)
        if len(results) > 1:
            dupe = 'duplicate'
            for issue in results:
                if dupe not in issue.labels:
                    others = '\n'.join([f'- {x.link}' for x in results if x != issue])
                    issue.add_comment(f'Issue appears to be a duplicate of:\n{others}\n'
                        f'Please remove "clientref:" label from redundant issue and change its status to "Done".')
                    issue.add_label(dupe)
                    issue.change_assignee(secrets.report_to_account_ID)
            raise Exception(f'Multiple Results for {jql}')
        elif len(results) == 0:
            raise Exception(f'No Results for {jql}')
        else:
            self.issue = results[0]
            return self.issue

    def load_logs(self, verbose: bool = True) -> pd.core.frame.DataFrame:
        '''Gets log info from S3.
        Returns a Dataframe with logs associated with the crawl.'''
        s3client = self.aws_session.client('s3')

        logs_response = s3client.list_objects_v2(Bucket=secrets.data_bucket, Prefix=f'crawl-logs/tna-{self.id}')
        if logs_response.get('IsTruncated'):
            raise Exception('Crawl has too many log files.')
        if not logs_response.get('Contents'):
            raise Exception('No available log files.')

        self.log_files = pd.DataFrame(logs_response.get('Contents'))
        self.crawl_log = self.get_crawl_log()
        return self.log_files

    def get_crawl_log(self, verbose: bool = True) -> str:
        '''Loads and combines the crawl's crawl logs
        into a single string log.'''

        crawl_logs = self.log_files[self.log_files['Key'].str.contains('/crawl\.log', na=False)]
        if len(crawl_logs) == 0:
            raise Exception('No crawl logs.')

        crawl_logs = crawl_logs.apply(lambda x: gwa_aws.open_file(secrets.data_bucket, x['Key'], self.aws_session).decode(), axis=1)
        self.crawl_log = '\n'.join(crawl_logs)

        return self.crawl_log

    def load_scope(self):                   ####TO CLEAN
        '''Loads Scoping Rules from Specifications.'''

        also_files = list(self.log_files.loc[
                    self.log_files['Key'].str.contains('also-in-scope.txt'), 'Key'])
        also = ''
        for file in also_files:
            also += gwa_aws.open_file(bucket=secrets.data_bucket, path=file, aws_session=self.aws_session).decode()
            also += '\n'
        also = set([x for x in also.split('\n') if x])
        patterns = set([x.split('://')[1] for x in also])
        for patt in patterns:
            also.add('https://'+patt)
            also.add('http://'+patt)

        self.scope = list(also)
        self.homepage = gwa_qa.clean_url(self.scope[0])

        scopes = list(self.log_files.loc[
                    self.log_files['Key'].str.contains('associated.txt|also-capture.txt'), 'Key'])

        for x in scopes:
            x = gwa_aws.open_file(bucket=secrets.data_bucket, path=x, aws_session=self.aws_session).decode()
            x = x.split('\n')
            self.scope += x

        self.scope = tuple(self.scope)

        return self.scope
