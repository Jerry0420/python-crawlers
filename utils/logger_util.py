import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
import inspect
from multiprocessing.queues import Queue

class LoggerToQueue:
    
    def __init__(self, logger_queue: Queue) -> None:
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

def init_log(path: str=os.getcwd(), site_name: str='') -> logging.Logger:
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

def init_logger_process(site_name: str, logger_queue: Queue):
    logger = init_log(site_name=site_name)
    while True:
        while not logger_queue.empty():
            message_info = logger_queue.get()
            log_type = message_info[0]
            log_pid = message_info[1]

            log_filename = message_info[2]
            log_linenum = message_info[3]
            
            log_message = message_info[4]
            log_args = message_info[5]
            log_kargs = message_info[6]

            before_message = f'{log_pid} | {log_filename}:{log_linenum} | '
            
            log_func = None
            if log_type == logging.INFO:
                log_func = logger.info
            if log_type == logging.ERROR:
                log_func = logger.error
            if log_type == logging.WARNING:
                log_func = logger.warning
            if log_type == logging.CRITICAL:
                log_func = logger.critical
            log_func(f'{before_message}{log_message}', *log_args, **log_kargs)