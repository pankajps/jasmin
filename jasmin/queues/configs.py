"""
Config file handler for 'amqp-broker' section in jasmin.cfg
"""

import logging
import os
import re

import txamqp

from jasmin.config import ConfigFile, ROOT_PATH, LOG_PATH

CONFIG_PATH = os.getenv('CONFIG_PATH', f'{ROOT_PATH}/etc/jasmin/')
RESOURCE_PATH = os.getenv('RESOURCE_PATH', f'{CONFIG_PATH}resource/')
CLOUDAMQP_URL = os.getenv('CLOUDAMQP_URL', None)


class AmqpConfig(ConfigFile):
    """
    Configuration handler for the 'amqp-broker' section of Jasmin's configuration.

    This class loads AMQP broker connection details (host, port, credentials, vhost), heartbeat,
    and logging configuration. It also handles optional environment-based configuration (CLOUDAMQP_URL)
    for platforms like Heroku.

    Attributes:
        host (str): AMQP broker hostname or IP address.
        port (int): AMQP broker port.
        username (str): AMQP broker username.
        password (str): AMQP broker password.
        vhost (str): AMQP virtual host.
        spec (str): Path to the AMQP 0.9.1 XML spec file.
        heartbeat (int): AMQP connection heartbeat interval.
        log_level (int): Logging level.
        log_file (str): Path to AMQP client log file.
        log_rotate (str): Log rotation strategy.
        log_format (str): Log record format.
        log_date_format (str): Log date format.
        reconnectOnConnectionLoss (bool): Whether to reconnect on connection loss.
        reconnectOnConnectionFailure (bool): Whether to reconnect on connection failure.
        reconnectOnConnectionLossDelay (int): Delay before reconnecting after loss.
        reconnectOnConnectionFailureDelay (int): Delay before reconnecting after failure.
    """

    def __init__(self, config_file=None):
        super().__init__(config_file)

        if CLOUDAMQP_URL is not None:
            # Parse AMQP config from CLOUDAMQP_URL (commonly used on Heroku)
            m = re.search(
                r"^amqps\:\/\/([a-z]+)\:([A-Za-z0-9_-]+)@((?!-)[-a-zA-Z0-9.]{1,63}(?<!-))\/([a-z]+)$",
                CLOUDAMQP_URL
            )
            if m:
                self.username, self.password, self.host, self.vhost = m.groups()
            else:
                raise ValueError(f'Invalid CLOUDAMQP_URL format: {CLOUDAMQP_URL}')
        else:
            # Load configuration from config file
            self.host = self._get('amqp-broker', 'host', '127.0.0.1')
            self.username = self._get('amqp-broker', 'username', 'guest')
            self.password = self._get('amqp-broker', 'password', 'guest')
            self.vhost = self._get('amqp-broker', 'vhost', '/')

        self.port = self._getint('amqp-broker', 'port', 5672)
        self.spec = self._get('amqp-broker', 'spec', f'{RESOURCE_PATH}/amqp0-9-1.xml')
        self.heartbeat = self._getint('amqp-broker', 'heartbeat', 0)

        # Logging configuration
        self.log_level = logging.getLevelName(self._get('amqp-broker', 'log_level', 'INFO'))
        self.log_file = self._get('amqp-broker', 'log_file', f'{LOG_PATH}/amqp-client.log')
        self.log_rotate = self._get('amqp-broker', 'log_rotate', 'W6')
        self.log_format = self._get(
            'amqp-broker', 'log_format', '%(asctime)s %(levelname)-8s %(process)d %(message)s')
        self.log_date_format = self._get('amqp-broker', 'log_date_format', '%Y-%m-%d %H:%M:%S')

        # Reconnection behavior
        self.reconnectOnConnectionLoss = self._getbool('amqp-broker', 'connection_loss_retry', True)
        self.reconnectOnConnectionFailure = self._getbool('amqp-broker', 'connection_failure_retry', True)
        self.reconnectOnConnectionLossDelay = self._getint('amqp-broker', 'connection_loss_retry_delay', 10)
        self.reconnectOnConnectionFailureDelay = self._getint(
            'amqp-broker', 'connection_failure_retry_delay', 10
        )

    def getSpec(self):
        """
        Load and return the AMQP protocol specifications from the XML spec file.

        :return: A loaded AMQP spec object.
        """
        return txamqp.spec.load(self.spec)
