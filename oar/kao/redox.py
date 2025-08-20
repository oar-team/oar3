from oar.lib.configuration import Configuration
from oar.lib.globals import get_logger

logger = get_logger("oar.kao.redox")


def can_use_redox_scheduler(config: Configuration) -> bool:
    return (
        ("REDOX_SCHEDULER" in config)
        and (config["REDOX_SCHEDULER"] == "yes")
        and is_redox_scheduler_available()
    )


def is_redox_scheduler_available():
    try:
        import importlib

        importlib.import_module("oar_scheduler_redox")
        return True
    except ImportError:
        logger.error(
            "You specified to use the rust scheduler with REDOX_SCHEDULER in config, "
            "but the library is not installed (module oar_scheduler_redox not reachable)."
            "Falling back to python scheduler."
        )
        return False
