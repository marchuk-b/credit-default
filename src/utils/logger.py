import logging
import os

def setup_logger(name: str, log_file: str) -> logging.Logger:
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Create a custom logger
    logger = logging.getLogger(name)
    
    # Check if handlers already exist to avoid duplicate logs
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Create handlers
        c_handler = logging.StreamHandler()      # Console (Terminal) handler
        f_handler = logging.FileHandler(f"logs/{log_file}", encoding='utf-8') # File handler

        # Create formatting and add it to handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        c_handler.setFormatter(formatter)
        f_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)

    return logger