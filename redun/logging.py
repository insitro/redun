import logging

log_formatter = logging.Formatter("[redun] %(message)s")
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)

logger = logging.getLogger("redun")
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)

log_levels = {
    "ERROR": logging.ERROR,
    "WARN": logging.WARN,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}
