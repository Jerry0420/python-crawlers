import logging
import os
import platform
from datetime import datetime
from logging.handlers import RotatingFileHandler
import inspect

class LoggerToQueue:
    
    def __init__(self, logger_queue) -> None:
        self.logger_queue = logger_queue

    def info(self, message, *args, **kwargs):
        current_stack = inspect.stack()[1]
        self.logger_queue.put([logging.INFO, os.getpid(), current_stack[1], current_stack[2], message, args, kwargs])
        
    def error(self, message, *args, **kwargs):
        current_stack = inspect.stack()[1]
        self.logger_queue.put([logging.ERROR, os.getpid(), current_stack[1], current_stack[2], message, args, kwargs])
        
    def warning(self, message, *args, **kwargs):
        current_stack = inspect.stack()[1]
        self.logger_queue.put([logging.WARNING, os.getpid(), current_stack[1], current_stack[2], message, args, kwargs])
        
    def critical(self, message, *args, **kwargs):
        current_stack = inspect.stack()[1]
        self.logger_queue.put([logging.CRITICAL, os.getpid(), current_stack[1], current_stack[2], message, args, kwargs])
        
def creation_date(file_path):
    if platform.system() == 'Windows':
        return os.path.getctime(file_path)
    else:
        stat = os.stat(file_path)
        try:
            return stat.st_birthtime
        except AttributeError:
            return stat.st_ctime

def init_log(path=os.getcwd(), site_name=''):
    logging.captureWarnings(True)
    
    logger = logging.getLogger(site_name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(levelname)s | %(asctime)s | %(message)s', "%Y-%m-%d %H:%M:%S")

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    if not os.path.exists(path + '/logs'):
        os.makedirs(path + '/logs')
    
    str_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    logger_file_path = path + '/logs/' + site_name + "_{}".format(str_time) + ".log"

    file_handler = RotatingFileHandler(logger_file_path, mode='a', maxBytes=1024*1024, backupCount=1, encoding='utf-8', delay=0)

    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger