# AutoQA
Application for automating QA tasks. 

This process is specific to UKGWA work flow and connects to sources and platforms such as JIRA and AWS.
It can be run by any member of UKGWA team or on any EC2 instance.
To configure, a env/ dir must be set up holding private information. deatils of the structure of this directory can be found on Sharepoint (link).

**Structure**
- main.py configures the job and collects a list of JIRA issues
- A sub-task (autoQA.py) is created for each issue. 
  - This sets up a crawl folder and checks which processes need to be run.
- The correct processes from the 'processes' directory are run for the crawl.

**Proccesses**
1. Crawl Log Analysis - Combines the Crawl Logs of a crawl and checks errors against the live site. Produces a list of URLs which are errors in the crawl log but available on the live site.
2. Diffex - Compares an output of URLs discovered by Screaming Frog with those in the craw log
3. PDF Flash - Runs through the PDFs in the crawl log, parsing them for URLs. Any which are not captured are added to a patchlist.

# Set Up

Set-up from scratch

To set up autoQA from scratch, e.g. on a new EC2.

set up an EC2 instance

log in & configure environment (e.g. setting up users/downloading git and python if necessary)

Choose a location for autoQA (the root directory ~ will do) run the following from that directory

```
git clone https://github.com/tna-webarchive/autoQA.git
cd autoQA
python3.9 -m venv ./venv
source venv/bin/activate
```
> You may have to run `sudo apt install python3.9-venv` or equivalent to insatll venv 

the terminal line should now have (venv) indicating you are in the virtual environment:

```
pip install -r requirements.txt
```
>NOTE: If you see an error while running the above like `error: invalid command 'bdist_wheel'` it can be ignored.

You will then have to configure the secret values which are not part of the repository

```
mkdir env
nano env/secrets.py
```


paste the following with all <placeholders> swapped out for the actual values

```
from requests.auth import HTTPBasicAuth

### JIRA ###
JIRAauth = HTTPBasicAuth('<aqa-bot email>', '<aqa-bot API token>')
report_to_account_ID = 'enter account id'
aqa_bot_account_ID = 'enter account id'

### S3 ###
S3ACCESS_KEY = <S3 Access Key>   #You can generate this from the AWS console
S3SECRET_KEY = <S3 Access Key>   #You can generate this from the AWS console
data_bucket = <S3 Bucket>

### NOTD ###
NOTDuser = '<NOTD username>'
NOTDpassword = <NOTD password>
```
All variables within <> should be changed. (dropping the < and > characters)


once you’ve done this, the program is ready. You can  run it by:
```
python3 main.py
```

or to set up the full service follow the instructions on the wiki regarding cronjobs etc.

## Installing Requirements requirements.txt
To install all requirements run `pip install - r requirements.txt` 

If you find that there are missign dependencies please let me know or make a pull request.
