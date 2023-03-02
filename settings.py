import os
from env import secrets

home = os.path.expanduser('~')
root = os.path.dirname(os.path.realpath(__file__))
aqa_dir = os.path.join(home, 'aqa-crawls/')
aqa_queue = os.path.join(aqa_dir, 'queue/')
aqa_running = os.path.join(aqa_dir, 'running/')

os.system(f'mkdir -p {aqa_dir} {aqa_queue}')# {aqa_running}')

diffex_filename = 'diffex.csv'

scope_list_bucket = 'tna-ukgwa-sharing'
scope_list_prefix = 'scope-lists/'

default_headers = {'user-agent': 'UKGWA autoQA bot:www.nationalarchives.gov.uk/webarchive/; webarchive@nationalarchives.gov.uk'}

