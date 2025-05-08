import logging

# Handler info
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler("logs\etl.log"),
        logging.StreamHandler()  # También lo imprime en consola
    ]
)