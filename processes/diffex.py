import os
import sys
import datetime
import time
from urllib.parse import unquote, quote
from io import StringIO
import re

import arrow
import requests
import pandas as pd
import concurrent.futures
from ratelimit import limits, sleep_and_retry

from objects import Crawl
from helpers import logg
from env import secrets
import gwa_qa
import settings


process = __file__.rsplit('/', 1)[-1].replace('.py', '')  # NAME OF PROCESS
date = datetime.datetime.now().strftime('%Y%m%d')

###################################################################################################
#### NAME FUNCTION TO MATCH MODULE NAME e.g. template.py module contains tempalte() function ####
###################################################################################################


def diffex(crawl: Crawl.Crawl):
    '''Runs diffex sub-process on a crawl object'''

    #Sets up process sub-cirectory of crawl directory
    process_folder = os.path.join(crawl.directory, f'{date}-{process}/')
    os.system(f'mkdir -p {process_folder}')

#### CONFIGURES LOGGERS
    processlogger = crawl.logger.getChild(process)
    log_folder = f'{process_folder}logs/'
    processlogger = logg.configure_handlers(processlogger, log_folder, maxBytes=200*1024*1024, backupCount=5)
    sys.stdout = open(f'{log_folder}std.out', 'a')
    sys.stderr = open(f'{log_folder}err.log', 'a')

    try:
        url_clean_regex = re.compile('^(?:.?https?:\/\/)?(?:www\.)?(.*?)/?$')
        def clean_url(url: str):
            '''removes URL protocol (https://www.) if there and unquotes and quotes a url'''
            return quote(unquote(url_clean_regex.sub(r'\1', url)))

        # Set Rate limit for CDX API calls.
        MAX_CALLS_PER_SECOND = 5

        @sleep_and_retry
        @limits(calls=MAX_CALLS_PER_SECOND, period=1)
        def url_in_cdx_index(url, cdx_root='https://tnaqa.mirrorweb.com/ukgwa/cdx'):
            '''Checks the presence of a URL in the UKGWA QA CDX index. Returning True or False'''
            try:
                while True:
                    processlogger.debug(f'Checking {url} in QA index.')
                    response = requests.get(cdx_root, params={'url': url, 'limit': 1},
                                            headers=settings.default_headers, auth=(secrets.NOTDuser, secrets.NOTDpassword)) #auth is for if using notd, does not affect other indexes
                    processlogger.debug(f'{response.url} [{response.status_code}] {response.text}')
                    if response.status_code == 200:
                        return True if response.text else False
                    else:
                        processlogger.error(f'CDX API call failed. Trying again in 10 seconds')
                        time.sleep(10)
                        continue
            except:
                raise

        #If there are two files with the same name, JIRA handle it by adding a guid to the filename. As such the below regex macthes ' (<guid>)' to replace it with '' when checking for diffex.csv files
        guid_regex = re.compile(' \([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\)')
        ### GET DIFFEX FILE
        files = [file for file in crawl.issue.data['attachment'] if guid_regex.sub('', file['filename']).lower() == settings.diffex_filename]
        files.sort(key=lambda x: arrow.get(x['created']).datetime, reverse=True)
        file = files[0]

        ### GET FILE CONTENT
        content_response = requests.get(file['content'], auth=crawl.issue.auth, headers={'Accept': 'application/json'})

        try:
            assert content_response.status_code == 200
            ### REMOVE BOM STRING IF ITS THERE
            content = re.sub('^ï»¿', '', content_response.text)
        except Exception as e:
            exception_message = f'Request for {settings.diffex_filename} from crawl {crawl.id} failed. '\
                                f'[{content_response.status_code}] {content_response.text}'
            processlogger.exception(exception_message)
            raise

        # Saves a copy of diffex.csv in the process folder
        with open(os.path.join(process_folder, 'diffex.csv'), 'w') as dest:
            dest.write(content)

        ### Open CSV as DataFrame
        df = pd.read_csv(StringIO(content))

        # Filters the dataframe for successful requests (between 200 and 399)
        df = df[df['Status Code'].between(200, 399)].copy()

        ### Uses URL Encoded Address, cleaned and duplicates dropped
        df.loc[:, 'clean_url'] =  df['URL Encoded Address'].apply(clean_url)
        df.drop_duplicates('clean_url', inplace=True)

        # Saves clean, deduped SF URLs locally
        with open(os.path.join(process_folder, 'screaming-frog-urls.txt'), 'w') as dest:
            dest.write('\n'.join(df.clean_url))

        # Generates RUD for crawl log
        processlogger.info('Generating RUD.')
        crawl.rud = gwa_qa.crawl_log_rud(crawl.crawl_log)
        processlogger.info('RUD Generated.')

        ### Cleans all URLs in Crawl log (quote and remove protocol)
        processlogger.info('Cleaning crawl log URLs')
        crawl.urls = set(map(clean_url, crawl.rud.get_urls(crawl.rud.present)))
        with open(os.path.join(process_folder, 'crawl-log-urls.txt'), 'w') as dest:
            dest.write('\n'.join(crawl.urls))

        ### Return URLs missing from crawl logs
        processlogger.info('Checking SF URLs against crawl log URLs')
        diff = df[~df.clean_url.isin(crawl.urls)].copy()

        # Checks missing URLs against QA index (multithreaded process)
        processlogger.info(f'Checking {len(diff)} missing URLs against QA index.')
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(url_in_cdx_index, diff['URL Encoded Address'].values)

        # Diff DataFrame filtered to removed URLs which are present in QA index
        diff['in_qa'] = pd.Series(results).values
        diff = diff[~diff.in_qa].copy()

        processlogger.info(f'{len(diff)} URLs missing from crawl log and QA index ')

        comment = f'''Diffex:
    {settings.diffex_filename} contains {len(df)} URL(s) 
    crawl log contains {len(crawl.urls)} URL(s)
    {len(diff)} URL(s) in screaming-frog log but not in crawl log or QA index.'''

        if not diff.empty:
            # Saving list of missing URLs to prcoess-folder
            undiscovered_urls_path = os.path.join(process_folder, 'undiscovered-urls.txt')
            diff['URL Encoded Address'].to_csv(undiscovered_urls_path, index=False)  # no status code

            try:
                # Uploaded missing URL list to Jira issue
                crawl.issue.attach_doc(f'diffex-undiscovered-urls-{crawl.id}.txt',
                                       undiscovered_urls_path)
                processlogger.info('Diffex List Attached')
                comment += f'\nAdded to "diffex-undiscovered-urls-{crawl.id}.txt"'
            except:
                processlogger.exception('Diffex List Upload Failed')
                comment += '\nMissing URLs list Upload Failed.'

        try:
            # Adding comment and label to JIRA issue
            crawl.issue.add_comment(comment)
            processlogger.info('Comment Posted')
        except:
            processlogger.exception('Comment/Label Update Failed')

        processlogger.info(f'Process {process} Finished.')
    except:
        raise
    finally:
        crawl.issue.add_label(f'{process}-complete')        #Ensures process won't run and fail over and over
        processlogger.info('Label Added.')