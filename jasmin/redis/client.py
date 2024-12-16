import sys
import logging
from logging.handlers import TimedRotatingFileHandler

import txredisapi as redis
from twisted.internet import reactor
from jasmin.redis.configs import RedisForJasminConfig

LOG_CATEGORY = "jasmin-redis-client"


class RedisForJasminProtocol(redis.RedisProtocol):
    """
    Custom Redis protocol class for Jasmin, extending txredisapi's RedisProtocol.
    Logs connection events and executed commands for better debugging.
    """

    def connectionMade(self):
        super().connectionMade()
        self.factory.log.info("Connection made")

    def execute_command(self, *args, **kwargs):
        self.factory.log.debug('Executing redis command: %s', args)
        return super().execute_command(*args, **kwargs)


class RedisForJasminFactory(redis.RedisFactory):
    """
    Factory for creating Redis connections tailored for Jasmin.
    Includes logging setup and optional password authentication.

    Attributes:
        log (logging.Logger): Logger instance for the factory.
    """
    protocol = RedisForJasminProtocol

    def startedConnecting(self, connector):
        super().startedConnecting(connector)
        self.log.info('Connecting ...')

    def clientConnectionLost(self, connector, reason):
        super().clientConnectionLost(connector, reason)
        self.log.info('Lost connection. Reason: %s', reason)

    def clientConnectionFailed(self, connector, reason):
        super().clientConnectionFailed(connector, reason)
        self.log.info('Connection failed. Reason: %s', reason)

    def __init__(self, uuid, dbid, poolsize, isLazy=True,
                 handler=redis.ConnectionHandler, config=None):
        """
        Initialize the RedisForJasminFactory.

        :param uuid: Unique identifier for the connection.
        :param dbid: Redis DB ID.
        :param poolsize: Number of connections in the pool.
        :param isLazy: If True, return a lazy connection handler.
        :param handler: Connection handler class.
        :param config: Instance of RedisForJasminConfig or None.
        """
        if isinstance(config, RedisForJasminConfig) and config.password is not None:
            super().__init__(uuid, dbid, poolsize, isLazy, handler, password=config.password)
        else:
            super().__init__(uuid, dbid, poolsize, isLazy, handler)

        # Set up a dedicated logger
        self.log = logging.getLogger(LOG_CATEGORY)
        if config is not None:
            self.log.setLevel(config.log_level)
            if 'stdout' in config.log_file:
                log_handler = logging.StreamHandler(sys.stdout)
            else:
                log_handler = TimedRotatingFileHandler(filename=config.log_file, when=config.log_rotate)
            formatter = logging.Formatter(config.log_format, config.log_date_format)
            log_handler.setFormatter(formatter)
        else:
            log_handler = logging.NullHandler()

        # Ensure only one handler is added
        if len(self.log.handlers) != 1:
            self.log.addHandler(log_handler)
            self.log.propagate = False


def makeConnection(host, port, dbid, poolsize, reconnect, isLazy, _RedisForJasminConfig=None):
    """
    Create a Redis connection using RedisForJasminFactory and connect to the given host and port.

    :param host: Redis server host.
    :param port: Redis server port.
    :param dbid: DB ID to select upon connection.
    :param poolsize: Number of connections in the pool.
    :param reconnect: Whether to attempt reconnection on connection loss/failure.
    :param isLazy: If True, return a lazy connection handler.
    :param _RedisForJasminConfig: Configuration instance.
    :return: Deferred or connection handler depending on isLazy.
    """
    uuid = f"{host}:{port}"
    factory = RedisForJasminFactory(uuid, None, poolsize, isLazy, redis.ConnectionHandler, _RedisForJasminConfig)
    factory.continueTrying = reconnect
    for _ in range(poolsize):
        reactor.connectTCP(host, int(port), factory)

    if isLazy:
        return factory.handler
    else:
        return factory.deferred


def SimpleConnection(host="127.0.0.1", port=6379, dbid=None, reconnect=True):
    """
    Create a simple non-lazy Redis connection to the given host and port.

    :param host: Redis host.
    :param port: Redis port.
    :param dbid: Redis DB ID.
    :param reconnect: Whether to reconnect on loss.
    :return: Deferred that fires once connected.
    """
    return makeConnection(host, port, dbid, 1, reconnect, False)


def ConnectionWithConfiguration(_RedisForJasminConfig):
    """
    Create a Redis connection using a RedisForJasminConfig object.

    If password is set in the config, DB selection is done after auth.

    :param _RedisForJasminConfig: Config object.
    :return: Deferred that fires once connected.
    """
    if _RedisForJasminConfig.password is not None:
        dbid = None
    else:
        dbid = _RedisForJasminConfig.dbid

    return makeConnection(_RedisForJasminConfig.host, _RedisForJasminConfig.port, dbid,
                          _RedisForJasminConfig.poolsize, True, False, _RedisForJasminConfig)
