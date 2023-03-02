import logging
import logging.handlers
import os

def default_logger(logger_name: str = ''):
    '''Defaults to return root logger'''
    logger = logging.getLogger(logger_name)
    logger.setLevel(10)
    return logger

def configure_handlers(logger, log_dir, maxBytes=10*1024*1024, backupCount=3, format = '%(asctime)s\t%(name)s\t%(message)s'):
    '''Configures handlers for logger. Defualt max is 10MB'''
    logger.setLevel(10)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    full_handler = create_FileHandler(os.path.join(log_dir, 'full.log'),
                                      level=10, maxBytes=maxBytes,
                                      backupCount=backupCount, format=format)

    act_handler = create_FileHandler(os.path.join(log_dir, 'info.log'),
                                     level=20, maxBytes=maxBytes,
                                     backupCount=backupCount, format=format)

    err_handler = create_FileHandler(os.path.join(log_dir, 'err.log'),
                                     level=40, maxBytes=maxBytes,
                                     backupCount=backupCount, format=format)

    for handler in [full_handler, err_handler, act_handler]:
        logger.addHandler(handler)

    return logger

def create_FileHandler(filepath, level=10, maxBytes=10*1024*1024, backupCount=3, format='%(asctime)s\t%(name)s\t%(message)s'):
    '''Creates a file handler to add to a logger. Default max bytes is 10MB'''
    handler = logging.handlers.RotatingFileHandler(filepath, maxBytes=maxBytes, backupCount=backupCount)
    handler.setLevel(level)
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)

    return handler

def create_StreamHandler(output, level, format='%(asctime)s\t%(name)s\t%(message)s'):
    '''Creates a Stream Handler to add to a logger'''
    handler = logging.StreamHandler(output)
    handler.setLevel(level)
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)

    return handler