from config import get_settings
from config.logging import setup_logging, get_logger

def main():
    setup_logging()
    logger = get_logger(module=__name__)
    logger.info("Starting the application")
    settings = get_settings()
    logger.info(f"Settings: {settings.model_dump_json(indent=4)}")

if __name__ == "__main__":
    main()
