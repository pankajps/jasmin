import logging
import re

from jasmin.config import LOG_PATH, ConfigFile
from smpp.pdu.pdu_types import (
    EsmClass, EsmClassMode, EsmClassType,
    RegisteredDelivery, RegisteredDeliveryReceipt,
    AddrTon, AddrNpi, PriorityFlag, ReplaceIfPresentFlag
)


# Default logging format constants
DEFAULT_LOGFORMAT = '%(asctime)s %(levelname)-8s %(process)d %(message)s'
DEFAULT_LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


class ConfigUndefinedIdError(Exception):
    """Raised when a Config class is initialized without an 'id' parameter."""
    pass


class ConfigInvalidIdError(Exception):
    """Raised when a Config class is initialized with an invalid ID syntax."""
    pass


class TypeMismatch(Exception):
    """Raised when a configuration element has an invalid type."""
    pass


class UnknownValue(Exception):
    """Raised when a configuration element has a valid type but an inappropriate or unsupported value."""
    pass


class SMPPClientConfig:
    """
    SMPPClientConfig holds the configuration parameters needed for establishing and maintaining
    an SMPP client connection, as well as message handling and logging.

    Attributes:
        id (str): SMPP client identifier.
        port (int): SMPP server port to connect to.
        log_file (str): Path to the log file.
        log_rotate (str): Log rotation schedule.
        log_level (int): Logging level.
        log_format (str): Format for log messages.
        log_date_format (str): Date format for log messages.
        log_privacy (bool): If True, message content is obfuscated in logs.
        sessionInitTimerSecs (float): Time allowed for the initial bind request to respond.
        enquireLinkTimerSecs (float): Interval between enquire_link requests.
        inactivityTimerSecs (float): Maximum allowed inactivity time before reconnect.
        responseTimerSecs (float): Timeout for receiving a response to any PDU request.
        pduReadTimerSecs (float): Timeout for reading a single PDU completely.
        dlr_expiry (float): TTL for messages awaiting DLR receipt.
        host (str): SMPP server host.
        username (str): System ID used for binding.
        password (str): Password used for binding.
        systemType (str): System type to be used for binding.
        reconnectOnConnectionLoss (bool): Reconnect when connection is lost.
        reconnectOnConnectionFailure (bool): Reconnect on initial connection failure.
        reconnectOnConnectionLossDelay (float): Delay before reconnecting after a lost connection.
        reconnectOnConnectionFailureDelay (float): Delay before reconnecting after a failed connection attempt.
        useSSL (bool): If True, use SSL for connections.
        SSLCertificateFile (str): Path to SSL certificate file.
        bindOperation (str): Type of bind: 'transceiver', 'transmitter', or 'receiver'.
        service_type (str): SMPP service_type parameter.
        addressTon (AddrTon): TON for the address range.
        addressNpi (AddrNpi): NPI for the address range.
        source_addr_ton (AddrTon): TON for the source address.
        source_addr_npi (AddrNpi): NPI for the source address.
        dest_addr_ton (AddrTon): TON for the destination address.
        dest_addr_npi (AddrNpi): NPI for the destination address.
        addressRange (str): Address range.
        source_addr (str): Fixed source address if set.
        esm_class (EsmClass): ESM class settings.
        protocol_id (int): Protocol ID.
        priority_flag (PriorityFlag): Priority of the message.
        schedule_delivery_time (str): Scheduled delivery time.
        validity_period (str): Validity period for the message.
        registered_delivery (RegisteredDelivery): Requested delivery receipt settings.
        replace_if_present_flag (ReplaceIfPresentFlag): Replace if present flag.
        sm_default_msg_id (int): Default message ID.
        data_coding (int): Data coding scheme for messages.
        requeue_delay (float): Delay before requeuing rejected messages.
        submit_sm_throughput (float): Maximum SUBMIT_SM messages per second.
        dlr_msg_id_bases (int): DLR message ID base conversions (0,1,2).
    """

    def __init__(self, **kwargs):
        # Validate and set ID
        if 'id' not in kwargs or kwargs['id'] is None:
            raise ConfigUndefinedIdError('SMPPConfig must have an id.')
        idcheck = re.compile(r'^[A-Za-z0-9_-]{3,25}$')
        if idcheck.match(str(kwargs['id'])) is None:
            raise ConfigInvalidIdError('SMPPConfig id syntax is invalid. It must be 3-25 chars [A-Za-z0-9_-].')
        self.id = str(kwargs['id'])

        # Basic settings
        self.port = kwargs.get('port', 2775)
        if not isinstance(self.port, int):
            raise TypeMismatch('port must be an integer.')

        # Logging
        self.log_file = kwargs.get('log_file', f'{LOG_PATH}/default-{self.id}.log')
        self.log_rotate = kwargs.get('log_rotate', 'midnight')
        self.log_level = kwargs.get('log_level', logging.INFO)
        self.log_format = kwargs.get('log_format', DEFAULT_LOGFORMAT)
        self.log_date_format = kwargs.get('log_dateformat', DEFAULT_LOG_DATE_FORMAT)
        self.log_privacy = kwargs.get('log_privacy', False)
        if not isinstance(self.log_privacy, bool):
            raise TypeMismatch('log_privacy must be a boolean.')

        # Timers
        self.sessionInitTimerSecs = kwargs.get('sessionInitTimerSecs', 30)
        if not isinstance(self.sessionInitTimerSecs, (int, float)):
            raise TypeMismatch('sessionInitTimerSecs must be an integer or float.')

        self.enquireLinkTimerSecs = kwargs.get('enquireLinkTimerSecs', 30)
        if not isinstance(self.enquireLinkTimerSecs, (int, float)):
            raise TypeMismatch('enquireLinkTimerSecs must be an integer or float.')

        self.inactivityTimerSecs = kwargs.get('inactivityTimerSecs', 300)
        if not isinstance(self.inactivityTimerSecs, (int, float)):
            raise TypeMismatch('inactivityTimerSecs must be an integer or float.')

        self.responseTimerSecs = kwargs.get('responseTimerSecs', 120)
        if not isinstance(self.responseTimerSecs, (int, float)):
            raise TypeMismatch('responseTimerSecs must be an integer or float.')

        self.pduReadTimerSecs = kwargs.get('pduReadTimerSecs', 10)
        if not isinstance(self.pduReadTimerSecs, (int, float)):
            raise TypeMismatch('pduReadTimerSecs must be an integer or float.')

        # DLR
        self.dlr_expiry = kwargs.get('dlr_expiry', 86400)
        if not isinstance(self.dlr_expiry, (int, float)):
            raise TypeMismatch('dlr_expiry must be an integer or float.')

        # SMPP Client specifics
        self.host = kwargs.get('host', '127.0.0.1')
        if not isinstance(self.host, str):
            raise TypeMismatch('host must be a string.')

        self.username = kwargs.get('username', 'smppclient')
        if len(self.username) > 15:
            raise TypeMismatch('username length must not exceed 15 chars.')

        self.password = kwargs.get('password', 'password')
        if len(self.password) > 16:
            raise TypeMismatch('password length must not exceed 16 chars.')

        self.systemType = kwargs.get('systemType', '')

        # Reconnection logic
        self.reconnectOnConnectionLoss = kwargs.get('reconnectOnConnectionLoss', True)
        if not isinstance(self.reconnectOnConnectionLoss, bool):
            raise TypeMismatch('reconnectOnConnectionLoss must be a boolean.')

        self.reconnectOnConnectionFailure = kwargs.get('reconnectOnConnectionFailure', True)
        if not isinstance(self.reconnectOnConnectionFailure, bool):
            raise TypeMismatch('reconnectOnConnectionFailure must be a boolean.')

        self.reconnectOnConnectionLossDelay = kwargs.get('reconnectOnConnectionLossDelay', 10)
        if not isinstance(self.reconnectOnConnectionLossDelay, (int, float)):
            raise TypeMismatch('reconnectOnConnectionLossDelay must be an integer or float.')

        self.reconnectOnConnectionFailureDelay = kwargs.get('reconnectOnConnectionFailureDelay', 10)
        if not isinstance(self.reconnectOnConnectionFailureDelay, (int, float)):
            raise TypeMismatch('reconnectOnConnectionFailureDelay must be an integer or float.')

        self.useSSL = kwargs.get('useSSL', False)
        if not isinstance(self.useSSL, bool):
            raise TypeMismatch('useSSL must be a boolean.')
        self.SSLCertificateFile = kwargs.get('SSLCertificateFile', None)

        # Bind operation
        self.bindOperation = kwargs.get('bindOperation', 'transceiver')
        if self.bindOperation not in ['transceiver', 'transmitter', 'receiver']:
            raise UnknownValue(f'Invalid bindOperation: {self.bindOperation}')

        # Default PDU parameters
        self.service_type = kwargs.get('service_type', None)
        self.addressTon = kwargs.get('addressTon', AddrTon.UNKNOWN)
        self.addressNpi = kwargs.get('addressNpi', AddrNpi.UNKNOWN)
        self.source_addr_ton = kwargs.get('source_addr_ton', AddrTon.NATIONAL)
        self.source_addr_npi = kwargs.get('source_addr_npi', AddrNpi.ISDN)
        self.dest_addr_ton = kwargs.get('dest_addr_ton', AddrTon.INTERNATIONAL)
        self.dest_addr_npi = kwargs.get('dest_addr_npi', AddrNpi.ISDN)
        self.addressRange = kwargs.get('addressRange', None)
        self.source_addr = kwargs.get('source_addr', None)
        self.esm_class = kwargs.get(
            'esm_class',
            EsmClass(EsmClassMode.STORE_AND_FORWARD, EsmClassType.DEFAULT)
        )
        self.protocol_id = kwargs.get('protocol_id', None)
        self.priority_flag = kwargs.get('priority_flag', PriorityFlag.LEVEL_0)
        self.schedule_delivery_time = kwargs.get('schedule_delivery_time', None)
        self.validity_period = kwargs.get('validity_period', None)
        self.registered_delivery = kwargs.get(
            'registered_delivery',
            RegisteredDelivery(RegisteredDeliveryReceipt.NO_SMSC_DELIVERY_RECEIPT_REQUESTED)
        )
        self.replace_if_present_flag = kwargs.get('replace_if_present_flag', ReplaceIfPresentFlag.DO_NOT_REPLACE)
        self.sm_default_msg_id = kwargs.get('sm_default_msg_id', 0)

        self.data_coding = kwargs.get('data_coding', 0)
        valid_data_codings = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 13, 14]
        if self.data_coding not in valid_data_codings:
            raise UnknownValue(f'Invalid data_coding: {self.data_coding}')

        # QoS
        self.requeue_delay = kwargs.get('requeue_delay', 120)
        if not isinstance(self.requeue_delay, (int, float)):
            raise TypeMismatch('requeue_delay must be an integer or float.')

        self.submit_sm_throughput = kwargs.get('submit_sm_throughput', 1)
        if not isinstance(self.submit_sm_throughput, (int, float)):
            raise TypeMismatch('submit_sm_throughput must be an integer or float.')

        self.dlr_msg_id_bases = kwargs.get('dlr_msg_id_bases', 0)
        if self.dlr_msg_id_bases not in [0, 1, 2]:
            raise UnknownValue(f'Invalid dlr_msg_id_bases: {self.dlr_msg_id_bases}')


class SMPPClientServiceConfig(ConfigFile):
    """
    Configuration for the SMPPClientService. Defines logging settings for the SMPP client service.
    """

    def __init__(self, config_file):
        super().__init__(config_file)

        self.log_level = logging.getLevelName(self._get('service-smppclient', 'log_level', 'INFO'))
        self.log_file = self._get('service-smppclient', 'log_file', f'{LOG_PATH}/service-smppclient.log')
        self.log_rotate = self._get('service-smppclient', 'log_rotate', 'W6')
        self.log_format = self._get('service-smppclient', 'log_format', DEFAULT_LOGFORMAT)
        self.log_date_format = self._get('service-smppclient', 'log_date_format', DEFAULT_LOG_DATE_FORMAT)


class SMPPServerConfig(ConfigFile):
    """
    SMPPServerConfig holds settings for an SMPP server instance including:
    - Binding interface and port
    - Billing features
    - Logging configuration
    - Timers and DLR expiry settings
    """

    def __init__(self, config_file=None):
        super().__init__(config_file)

        self.id = self._get('smpp-server', 'id', 'smpps_01')
        self.bind = self._get('smpp-server', 'bind', '0.0.0.0')
        self.port = self._getint('smpp-server', 'port', 2775)

        self.billing_feature = self._getbool('smpp-server', 'billing_feature', True)

        # Logging
        self.log_level = logging.getLevelName(self._get('smpp-server', 'log_level', 'INFO'))
        self.log_file = self._get('smpp-server', 'log_file', f'{LOG_PATH}/default-{self.id}.log')
        self.log_rotate = self._get('smpp-server', 'log_rotate', 'midnight')
        self.log_format = self._get('smpp-server', 'log_format', DEFAULT_LOGFORMAT)
        self.log_date_format = self._get('smpp-server', 'log_date_format', DEFAULT_LOG_DATE_FORMAT)
        self.log_privacy = self._getbool('smpp-server', 'log_privacy', False)

        # Timers
        self.sessionInitTimerSecs = self._getint('smpp-server', 'sessionInitTimerSecs', 30)
        self.enquireLinkTimerSecs = self._getint('smpp-server', 'enquireLinkTimerSecs', 30)
        self.inactivityTimerSecs = self._getint('smpp-server', 'inactivityTimerSecs', 300)
        self.responseTimerSecs = self._getint('smpp-server', 'responseTimerSecs', 60)
        self.pduReadTimerSecs = self._getint('smpp-server', 'pduReadTimerSecs', 10)

        # DLR expiry
        self.dlr_expiry = self._getint('smpp-server', 'dlr_expiry', 86400)


class SMPPServerPBConfig(ConfigFile):
    """
    SMPPServerPBConfig configures the PB interface for SMPP server management.
    It includes network details, authentication, and logging.
    """

    def __init__(self, config_file=None):
        super().__init__(config_file)

        self.bind = self._get('smpp-server-pb', 'bind', '0.0.0.0')
        self.port = self._getint('smpp-server-pb', 'port', 14000)

        self.authentication = self._getbool('smpp-server-pb', 'authentication', True)
        self.admin_username = self._get('smpp-server-pb', 'admin_username', 'smppsadmin')
        self.admin_password = bytes.fromhex(
            self._get('smpp-server-pb', 'admin_password', "e97ab122faa16beea8682d84f3d2eea4")
        )

        # Logging
        self.log_level = logging.getLevelName(self._get('smpp-server-pb', 'log_level', 'INFO'))
        self.log_rotate = self._get('smpp-server-pb', 'log_rotate', 'W6')
        self.log_file = self._get('smpp-server-pb', 'log_file', f'{LOG_PATH}/smpp-server-pb.log')
        self.log_format = self._get('smpp-server-pb', 'log_format', DEFAULT_LOGFORMAT)
        self.log_date_format = self._get('smpp-server-pb', 'log_date_format', DEFAULT_LOG_DATE_FORMAT)


class SMPPServerPBClientConfig(ConfigFile):
    """
    SMPPServerPBClientConfig configures the client part for connecting to the SMPPServerPB.
    """

    def __init__(self, config_file=None):
        super().__init__(config_file)

        self.host = self._get('smpp-server-pb-client', 'host', '127.0.0.1')
        self.port = self._getint('smpp-server-pb-client', 'port', 14000)

        self.username = self._get('smpp-server-pb-client', 'username', 'smppsadmin')
        self.password = self._get('smpp-server-pb-client', 'password', 'smppspwd')
