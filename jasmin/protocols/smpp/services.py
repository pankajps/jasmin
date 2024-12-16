import sys
import logging
from logging.handlers import TimedRotatingFileHandler

from twisted.application import service
from jasmin.protocols.smpp.factory import SMPPClientFactory
from .configs import SMPPClientServiceConfig

LOG_CATEGORY = "jasmin-service-smpp"


class SMPPClientService(service.Service):
    """
    A Twisted Service for managing the lifecycle of an SMPP client connection.
    This service starts, stops, and monitors the SMPP client connection and binding.
    """

    def __init__(self, SMPPClientConfig, config):
        """
        Initialize the SMPPClientService.

        :param SMPPClientConfig: Configuration instance for the SMPP client.
        :param config: Main configuration instance, used to load SMPPClientServiceConfig.
        """
        self.startCounter = 0
        self.stopCounter = 0
        self.config = config
        self.SMPPClientConfig = SMPPClientConfig

        # Load client service configuration
        self.SMPPClientServiceConfig = SMPPClientServiceConfig(self.config.getConfigFile())

        # Initialize the SMPP Client Factory
        self.SMPPClientFactory = SMPPClientFactory(self.SMPPClientConfig)

        # Set up logging
        self.log = self._setup_logger()

        self.log.info('Initialized SMPPClientService for [%s]', self.SMPPClientConfig.id)

    def _setup_logger(self):
        """Set up the dedicated logger for SMPPClientService."""
        logger = logging.getLogger(LOG_CATEGORY)
        if not logger.handlers:
            logger.setLevel(self.SMPPClientServiceConfig.log_level)

            if 'stdout' in self.SMPPClientServiceConfig.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(
                    filename=self.SMPPClientServiceConfig.log_file,
                    when=self.SMPPClientServiceConfig.log_rotate
                )

            formatter = logging.Formatter(
                self.SMPPClientServiceConfig.log_format,
                self.SMPPClientServiceConfig.log_date_format
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False

        return logger

    def startService(self):
        """
        Start the SMPP client service.

        Attempts to connect and bind the SMPP client. If the binding fails,
        the service will be stopped.
        """
        self.startCounter += 1
        service.Service.startService(self)
        self.log.info('Starting SMPPClientService for [%s]', self.SMPPClientConfig.id)

        d = self.SMPPClientFactory.connectAndBind()
        d.addErrback(self._startServiceErr)
        return d

    def stopService(self):
        """
        Stop the SMPP client service and disconnect the SMPP client,
        ensuring it will not attempt to reconnect.
        """
        self.stopCounter += 1
        service.Service.stopService(self)
        self.log.info('Stopping SMPPClientService for [%s]', self.SMPPClientConfig.id)
        return self.SMPPClientFactory.disconnectAndDontRetryToConnect()

    def _startServiceErr(self, failure):
        """
        Handle errors that occur during the startup of the SMPP service.

        :param failure: A Twisted Failure object indicating what went wrong.
        """
        self.log.error('Failed to start SMPPClientService for [%s]: %s',
                       self.SMPPClientConfig.id, failure.getErrorMessage())
        # Log detailed traceback if available
        self.log.debug(failure.getTraceback())
        return self.stopService()
