import logging

log_formatter = logging.Formatter("[redun] %(message)s")
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)

logger = logging.getLogger("redun")
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)
# Don't propagate to the root logger. Redun installs its own handler, so
# propagation causes duplicate output whenever user code (or an imported
# module) configures root logging, e.g. via logging.basicConfig().
logger.propagate = False

log_levels = {
    "ERROR": logging.ERROR,
    "WARN": logging.WARN,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}
