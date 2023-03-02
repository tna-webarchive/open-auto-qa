import os
import sys
import datetime
import re
from io import BytesIO
import time
from urllib.parse import urlparse

from tqdm import tqdm
import requests
import PyPDF2
from ratelimit import limits, sleep_and_retry

import settings
from env import secrets
from helpers import logg
import gwa_qa
import gwa_aws

process = __file__.rsplit('/', 1)[-1].replace('.py', '')  # NAME OF PROCESS
date = datetime.datetime.now().strftime('%Y%m%d')

###################################################################################################
#### NAME FUNCTION TO MATCH MODULE NAME e.g. template.py module contains tempalte() function ####
###################################################################################################

def PDFflash(crawl):
    process_folder = os.path.join(crawl.directory, f'{date}-{process}/')

    os.system(f'  mkdir -p {process_folder}')

    ## CONFIGURE LOGGERS ##
    processlogger = crawl.logger.getChild(process)
    log_folder = f'{process_folder}logs/'
    processlogger = logg.configure_handlers(processlogger, log_folder, maxBytes=200*1024*1024, backupCount=5)
    sys.stdout = open(f'{log_folder}std.out', 'a')
    sys.stderr = open(f'{log_folder}err.log', 'a')

    try:
        url_clean_regex = re.compile('^(?:.?https?:\/\/)?(?:www\.)?(.*?)/?$')
        def clean_url(url: str):
            '''removes URL protocol (https://www.) if there and unquotes and quotes a url'''
            return url_clean_regex.sub(r'\1', url)

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

        url_regex = re.compile('^https?://\S+')
        def get_urls_from_pdf(url: str):
            '''Harvests URLs from a PDF at a given url'''
            def get_key(object, key, otherwise=None):
                '''x.get(a) not performing like x[a] with PyPDF2 dictionaries so naming a
                function which replicates that behaviour'''
                try:
                    return object[key]
                except:
                    return otherwise

            def get_urls_from_page(page):
                '''Harvests URLs from a PDF page. taking a PyPDF2 page object.'''
                annots_tag, uri_tag, ank_tag  = '/Annots', '/URI', '/A'
                pageObject = pdfReader.getPage(page)
                text = pageObject.extractText()
                urls = url_regex.findall(text)

                annots = get_key(pageObject, annots_tag, [])
                for annot in annots:
                    try:
                        annot = annot.getObject()
                        anks = get_key(annot, ank_tag, dict())
                        url = get_key(anks, uri_tag, otherwise='')
                        urls.append(url) if url_regex.match(url) else None
                    except Exception as e:
                        processlogger.error(e)
                return urls

            r = requests.get(url, headers=settings.default_headers)
            pdfFileObject = BytesIO(r.content)
            pdfReader = PyPDF2.PdfFileReader(pdfFileObject)

            urls = []
            for page in range(pdfReader.numPages):
                urls += get_urls_from_page(page)

            return urls

        def open_scope_list(filename: str):
            '''Opens a scope list located at s3://ENTER-YOUR-DIRECTORY/<filename>'''
            scope = gwa_aws.open_file(settings.scope_list_bucket,
                                              os.path.join(settings.scope_list_prefix, filename),
                                              crawl.aws_session)
            scope = [x.decode() for x in scope.splitlines() if x and not x.strip().startswith(b'#')]
            return scope

        # Create RUD
        processlogger.info('Generating RUD using PDF lines.')
        pdf_crawl_log = '\n'.join([line for line in crawl.crawl_log.split('\n') if 'application/pdf' in line])
        crawl.pdf_rud = gwa_qa.crawl_log_rud(pdf_crawl_log)
        processlogger.info('PDF RUD Generated.')

        # Filter all URLs in crawl log for 200-399 status codes & .pdf suffix
        pdfs = set(crawl.pdf_rud.get_urls(range(200, 400)))

        processlogger.info(f'Extracting URLs from {len(pdfs)} PDFs')
        # Loop to extract URLs from all PDFs
        urls = set()
        for pdf in tqdm(pdfs):
            try:
                processlogger.debug(f'Extracting URLs from {pdf}')
                urls |= set(get_urls_from_pdf(pdf))
            except Exception:
                processlogger.exception(f'Failed to extract URLs from {pdf}')

        processlogger.info(f'Writing PDFs and URLs to crawl dir')

        # Writes checked PDFs to a file
        checked_pdf_path = os.path.join(process_folder, 'pdfs-checked.txt')
        with open(checked_pdf_path, 'w') as dest:
            dest.write('\n'.join(pdfs))

        # Writes discovered urls to a file
        pdf_urls_path = os.path.join(process_folder, 'discovered-urls.txt')
        with open(pdf_urls_path, 'w') as dest:
            dest.write('\n'.join(urls))

        # Opens the three scope lists
        scope_slds = open_scope_list('scope_allow-list_slds.txt')
        scope_domains = open_scope_list('scope_allow-list_domains.txt')
        scope_paths = open_scope_list('scope_allow-list_paths.txt')

        processlogger.info(f'Checking {len(urls)} urls against scope and QA index.')
        patchlist = set()
        # Runs through each URL discovered in the PDFs, checking for redirects, if they are in scope and if they are already captured
        # (currently does not check ukgwa live collection (commented out) but checks QA index)
        # Potential speed improvments could be made here by threading the requests.get() line and cdx index checks.
        for url in tqdm(urls):
            try:
                r = requests.get(url, headers=settings.default_headers)
                if 200 <= r.status_code < 400:
                    url = r.url
                    processlogger.debug(f'Checking {url} against scope')
                    if any([domain in urlparse(url).netloc for domain in scope_slds]):
                        pass
                    elif any([clean_url(url).startswith(pattern) for pattern in scope_domains+scope_paths]):
                        pass
                    else:
                        continue
                    processlogger.debug(f'{url} in scope, checking QA index.')
                    if not url_in_cdx_index(url, 'https://tnaqa.mirrorweb.com/ukgwa/cdx'):#and not url_in_cdx_index(url, 'https://webarchive.nationalarchives.gov.uk/ukgwa/cdx'):
                        processlogger.debug(f'Adding {url} to patchlist')
                        patchlist.add(url)
            except Exception:
                processlogger.exception(f'Failed checks on {url}')

        processlogger.info(f'PDFflash complete. Configuring comments and attachments.')
        # Create comment and post + attachment.
        comment = 'PDF-flash:'
        comment += f'\n{len(pdfs)} PDFs checked.'
        comment += f'\n{len(urls)} URLs discovered.'
        comment += f'\n{len(patchlist)} in scope and not in QA index'# or LIVE index.'

        ### PATCHLIST UPLOAD ###
        if len(patchlist) > 0:
            pdf_flash_patchlist = os.path.join(process_folder, 'pdfflash-patchlist.txt')
            with open(pdf_flash_patchlist, 'w') as dest:
                dest.write('\n'.join(patchlist))
            try:
                crawl.issue.attach_doc(f'pdfflash-patchlist-{crawl.id}.txt', pdf_flash_patchlist)
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
    except Exception as e:
        raise
    finally:
        crawl.issue.add_label(f'{process}-complete')        #Ensures process won't run and fail over and over