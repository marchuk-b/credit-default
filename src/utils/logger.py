import logging
import os

def setup_logger() -> logging.Logger:
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Create a custom logger
    logger = logging.getLogger("CreditPipeline")
    
    # Check if handlers already exist to avoid duplicate logs
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # Create handlers
        c_handler = logging.StreamHandler()      # Console (Terminal) handler
        f_handler = logging.FileHandler("logs/conveyor.log") # File handler

        # Create formatting and add it to handlers
        c_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        c_handler.setFormatter(c_format)
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        logger.addHandler(c_handler)
        logger.addHandler(f_handler)

    return logger