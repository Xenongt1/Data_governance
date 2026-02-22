import logging
import os
from pathlib import Path

def setup_logger(name, log_file="outputs/pipeline.log"):
    """Sets up a logger that writes to a file and optionally avoids console output."""
    
    # Ensure directory exists
    log_path = Path(log_file)
    log_path.parent.mkdir(exist_ok=True, parents=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Prevent duplicate handlers if called multiple times
    if logger.hasHandlers():
        logger.handlers.clear()
        
    # File Handler
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    ))
    logger.addHandler(file_handler)
    
    # We explicitly do NOT add a StreamHandler (console) to fulfill 
    # the user's request to "not print into the console".
    
    # Optional: If you WANT a minimal console output, you could add one back,
    # but the user said "I don't want you to print into the console".
    
    return logger

def get_logger(name):
    return logging.getLogger(name)
