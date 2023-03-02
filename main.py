import os
import time
import pickle
import multiprocessing
from multiprocessing_logging import install_mp_handler
import boto3

import autoQA
from jira_listener import jira_listener
import settings
from env import secrets
from helpers import logg


class ProcessHandler:
        '''Process handler class monitors and maintains available workers to run the autoQA application'''
        listener_worker = 'listener'
        logger = logg.default_logger(__name__)

        def __init__(self):
            self.MAXWORKERS = os.cpu_count() - 2      # Maximum available workers = no. cpus of machine - 2
            self.running = set()

        def process_running(self, name: str) -> multiprocessing.Process:
            '''Checks if a named process is running.
            param name is the name of a process to find.
            If it is running, it is returned, otherwise None is returned'''
            self.logger.debug(f'Checking running processes for a process named {name}')
            process = next((process for process in self.running if process.name == name), None)
            return process

        def ensure_listener(self):
            '''Ensures that the JIRA listener  worker is running. If it is not, it is recreated and started.'''
            self.logger.debug('Ensuring listener worker is running')
            if not self.process_running(self.listener_worker):
                listener = multiprocessing.Process(target=jira_listener.listen_populate_queue, args=(secrets.JIRAauth,),
                                      name=self.listener_worker, daemon=False)
                self.launch_process(listener)

        def prune_processes(self):
            '''Dead processes do not close themsleves so it is necessary to prune often.
            If a running process is found to be dead, it is closed, remvoed from the running set
            and if a corresponding crawl is in the queue with the same update time, it is removed.'''
            self.logger.debug(f'Pruning running processes. {len(self.running)} running.')
            for process in self.running.copy():                 ## copy so not to remove from actual set.
                if not process.is_alive():
                    self.logger.info(f'Process {process.name} dead. Closing and removing.')
                    process.close()
                    self.running.remove(process)
                    if process.name in os.listdir(settings.aqa_queue):
                        with open(process.queue_path, 'rb') as source:
                            queued_issue = pickle.load(source)
                        if queued_issue.data['updated'] == process.updated:
                            os.system(f'rm {process.queue_path}')
                            self.logger.info(f'Process {process.name} removed from queue.')
            self.ensure_listener()

        def launch_process(self, process: multiprocessing.Process):
            '''Takes a multiprocessing Process object and laucnhing it in the context of the application.
            If there is no free worker, it will wait and prune every 5 seconds until one becomes available.
            The process is started and added to the running set.'''
            while len(self.running) >= self.MAXWORKERS:
                self.logger.debug('Too many processes running to add another. Pruning and trying again')
                time.sleep(5)
                self.prune_processes()
            self.logger.info(f'Launching process {process}')
            process.start()
            self.running.add(process)

        def get_crawl_from_queue(self):
            '''Checks the queue for a new crawl. If the queue is empty, it will wait, prune and recheck every five seconds.
            When there is a crawl it will return the crawl ID'''
            queue = os.listdir(settings.aqa_queue)
            self.logger.info(f'{len(queue)} crawls in the queue')
            self.logger.info('Getting next crawl from queue.')
            crawl_id = next((crawl_id for crawl_id in queue if not self.process_running(crawl_id)), None)
            while not crawl_id:
                self.logger.debug('No crawls in the queue. Waiting and retrying')
                time.sleep(5)
                self.prune_processes()
                crawl_id = next((crawl_id for crawl_id in os.listdir(settings.aqa_queue) if not self.process_running(crawl_id)), None)
            return crawl_id


if __name__ == '__main__':
    directory = settings.aqa_dir
    os.system(f'mkdir -p {directory}')    # p flag makes if doesnt exist

    aws_session = boto3.Session(
            aws_access_key_id=secrets.S3ACCESS_KEY,
            aws_secret_access_key=secrets.S3SECRET_KEY)

    #Configures Root Logger
    rootlogger = logg.default_logger()
    logg.configure_handlers(rootlogger, os.path.join(directory, 'logs/'),
                            maxBytes=200*1024*1024, backupCount=5)

    #Configures App Logger
    logger = logg.default_logger(__name__)
    logger = logg.configure_handlers(logger, os.path.join(directory, 'logs/app/'),
                                     maxBytes=200*1024*1024, backupCount=5)

    # Handles multiprocess logging: https://pypi.org/project/multiprocessing-logging/
    install_mp_handler()

    logger.info('Initiating autoQA')
    # Initiates Process Handler
    handler = ProcessHandler()

    logger.info('Handler Configured')
    # Loops forever
    while True:
        # Set up includes ensuring listener process is running, running processes are pruned
        try:
            handler.ensure_listener()
            handler.prune_processes()

            crawl_id = handler.get_crawl_from_queue()
            queue_path = os.path.join(settings.aqa_queue, crawl_id)

            # The queued issue is loaded in order to get the update time and associate it with the process.
            # This allows the prune function to remove the crawl from the queue if it refers to the same update
            with open(queue_path, 'rb') as source:
                issue = pickle.load(source)

            logger.info(f'Configuring autoQA process for {crawl_id}')
            process = multiprocessing.Process(target=autoQA.autoQA, args=(crawl_id,),
                                             name=crawl_id, daemon=False)
            process.updated = issue.data['updated']
            process.queue_path = queue_path
            handler.logger.info(f'Launching autoQA process for {process.name}')
            handler.launch_process(process)
        except:
            continue