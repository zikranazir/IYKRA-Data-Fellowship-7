import logging
import pygogo


formatter = logging.Formatter("%(asctime)-4s %(name)s [%(levelname)s] - %(message)s")


def get_logger(name, custom_formatter=None) -> pygogo.Gogo.logger:
    global formatter
    _formatter = formatter
    if custom_formatter:
        _formatter = logging.Formatter(custom_formatter)
    logger = pygogo.Gogo(name,
                         low_formatter=_formatter,
                         high_level="error",
                         high_formatter=formatter).logger
    return logger
