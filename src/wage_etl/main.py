"""
ETL Pipeline - extract -> transform -> load.
"""
from wage_etl.config.logging import get_logger, setup_logging
from wage_etl.config.settings import get_settings
from wage_etl.pipeline import build_pipeline


def main() -> None:
    settings = get_settings()
    settings.ensure_dirs()
    setup_logging(settings.logging, settings.paths)
    logger = get_logger(module=__name__)
    logger.info("Starting ETL pipeline")

    pipeline = build_pipeline(settings)

    if not pipeline.db.test():
        logger.error("Database connection failed")
        return

    target_states = settings.pipeline.target_states
    logger.info(f"Processing {len(target_states)} states: {', '.join(target_states)}")

    for target_state in target_states:
        try:
            logger.info(f"Starting ETL for state: {target_state}")
            pipeline.run_state(target_state)
            logger.info(f"Completed ETL for state: {target_state}")
        except Exception as exc:
            logger.error(f"Failed to process state {target_state}: {exc}")
            continue

    logger.info("ETL pipeline completed for all states")


if __name__ == "__main__":
    main()
