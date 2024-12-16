"""
Config file handlers for 'client-management', 'sm-listener', and 'dlr' sections in jasmin.cfg
"""

import binascii
import ast
import logging
import os

from jasmin.config import ConfigFile, ROOT_PATH, LOG_PATH

DEFAULT_LOGFORMAT = '%(asctime)s %(levelname)-8s %(process)d %(message)s'
DEFAULT_LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

# Base configuration paths
CONFIG_PATH = os.getenv('CONFIG_PATH', f'{ROOT_PATH}/etc/jasmin/')
STORE_PATH = os.getenv('STORE_PATH', f'{CONFIG_PATH}store/')

class SMPPClientPBConfig(ConfigFile):
    """
    Configuration handler for the 'client-management' section in jasmin.cfg.

    This configuration deals with the SMPP Client Management PB server, including:
    - Network binding
    - Authentication details
    - Storage paths
    - Logging configuration
    """

    def __init__(self, config_file=None):
        super().__init__(config_file)

        # Store path for persistent data
        self.store_path = self._get('client-management', 'store_path', STORE_PATH)

        # Binding and port for the PB server
        self.bind = self._get('client-management', 'bind', '0.0.0.0')
        self.port = self._getint('client-management', 'port', 8989)

        # Authentication details
        self.authentication = self._getbool('client-management', 'authentication', True)
        self.admin_username = self._get('client-management', 'admin_username', 'cmadmin')
        # Default admin_password is a hex string that must be unhexlified
        self.admin_password = binascii.unhexlify(
            self._get('client-management', 'admin_password', "e1c5136acafb7016bc965597c992eb82")
        )

        # Logging configuration
        self.log_level = logging.getLevelName(self._get('client-management', 'log_level', 'INFO'))
        self.log_file = self._get('client-management', 'log_file', f'{LOG_PATH}/smppclient-manager.log')
        self.log_rotate = self._get('client-management', 'log_rotate', 'W6')
        self.log_format = self._get('client-management', 'log_format', DEFAULT_LOGFORMAT)
        self.log_date_format = self._get('client-management', 'log_date_format', DEFAULT_LOG_DATE_FORMAT)

        # Pickle protocol version for object serialization
        self.pickle_protocol = self._getint('client-management', 'pickle_protocol', 2)


class SMPPClientSMListenerConfig(ConfigFile):
    """
    Configuration handler for the 'sm-listener' section in jasmin.cfg.

    Handles configuration related to SMPP client's SMS listener, including:
    - Retrials for submit_sm errors
    - Max age of messages if smpp client is not ready
    - DLR lookup timings
    - Logging configuration
    """

    def __init__(self, config_file=None):
        super().__init__(config_file)

        # Whether to publish submit_sm_resp back
        self.publish_submit_sm_resp = self._getbool('sm-listener', 'publish_submit_sm_resp', False)

        # Submit error retrials: a dictionary of error_code: {count, delay}
        self.submit_error_retrial = ast.literal_eval(
            self._get(
                'sm-listener',
                'submit_error_retrial',
                """{
                    'ESME_RSYSERR':    {'count': 2,  'delay': 30},
                    'ESME_RTHROTTLED': {'count': 20, 'delay': 30},
                    'ESME_RMSGQFUL':   {'count': 2,  'delay': 180},
                    'ESME_RINVSCHED':  {'count': 2,  'delay': 300}
                }"""
            )
        )

        # Maximum allowed age of a message in the queue if the SMPP client is not ready
        self.submit_max_age_smppc_not_ready = self._getint('sm-listener', 'submit_max_age_smppc_not_ready', 1200)

        # Delay before retrying submit if SMPP client is not ready
        self.submit_retrial_delay_smppc_not_ready = self._getint(
            'sm-listener', 'submit_retrial_delay_smppc_not_ready', 30
        )

        # DLR lookup timings
        self.dlr_lookup_retry_delay = self._getint('sm-listener', 'dlr_lookup_retry_delay', 10)
        self.dlr_lookup_max_retries = self._getint('sm-listener', 'dlr_lookup_max_retries', 2)

        # Logging configuration
        self.log_level = logging.getLevelName(self._get('sm-listener', 'log_level', 'INFO'))
        self.log_file = self._get('sm-listener', 'log_file', f'{LOG_PATH}/messages.log')
        self.log_rotate = self._get('sm-listener', 'log_rotate', 'midnight')
        self.log_format = self._get('sm-listener', 'log_format', DEFAULT_LOGFORMAT)
        self.log_date_format = self._get('sm-listener', 'log_date_format', DEFAULT_LOG_DATE_FORMAT)
        self.log_privacy = self._getbool('sm-listener', 'log_privacy', False)


class DLRLookupConfig(ConfigFile):
    """
    Configuration handler for the 'dlr' section in jasmin.cfg.

    Deals with DLR lookup and SMPP receipt handling, including:
    - DLR lookup retries and delays
    - SMPP receipts handling
    - Logging configuration
    """

    def __init__(self, config_file=None):
        super().__init__(config_file)

        # PID for DLR processes (helps distinguish logs from different processes)
        self.pid = self._get('dlr', 'pid', 'main')

        # DLR lookup retry config
        self.dlr_lookup_retry_delay = self._getint('dlr', 'dlr_lookup_retry_delay', 10)
        self.dlr_lookup_max_retries = self._getint('dlr', 'dlr_lookup_max_retries', 2)

        # SMPP receipt handling when submit_sm_resp is successful
        self.smpp_receipt_on_success_submit_sm_resp = self._getbool(
            'dlr', 'smpp_receipt_on_success_submit_sm_resp', False
        )

        # Logging configuration
        self.log_level = logging.getLevelName(self._get('dlr', 'log_level', 'INFO'))
        self.log_file = self._get('dlr', 'log_file', f'{LOG_PATH}/messages.log')
        self.log_rotate = self._get('dlr', 'log_rotate', 'midnight')
        self.log_format = self._get('dlr', 'log_format', DEFAULT_LOGFORMAT)
        self.log_date_format = self._get('dlr', 'log_date_format', DEFAULT_LOG_DATE_FORMAT)
        self.log_privacy = self._getbool('dlr', 'log_privacy', False)
