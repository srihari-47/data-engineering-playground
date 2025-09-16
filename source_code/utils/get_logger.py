import logging
import os

def get_logger(name: str) -> logging.Logger:
    """
    This is a simple function that instantize a logger object  and
    configures the same. It adds file handler and stream handler to the
    logger object and sets the logger level as INFO.

    :param    name: Name of the logger
    :rtype    name: string
    :return logger: logger instance
    :rtype  logger: logging.Logger
    """
    # We are sending logs to console as well as writing to a log file for persistence
    logger = logging.getLogger(name)
    # Setting the log level to INFO
    logger.setLevel(logging.INFO)
    # Formatting the loger message verbiage and attaching the same to a formatter
    format_ = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(format_)
    # Now let's initialize handlers and attach formatter to them
    # console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    # file handler
    os.makedirs("../logs", exist_ok=True)
    file_handler = logging.FileHandler('../logs/task.log',encoding='utf-8')
    file_handler.setFormatter(formatter)
    # Adding handlers to our logger instance
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    return logger



