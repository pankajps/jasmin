"""
A set of objects used by Jasmin to manage users, groups, and connectors in memory (no database storage).
"""

import re
from hashlib import md5
from typing import Optional, Dict, Any, Union, Pattern

from jasmin.tools.singleton import Singleton


class jasminApiInvalidParamError(Exception):
    """Raised when attempting to instantiate a jasminApi object with invalid parameters."""
    pass


class jasminApiCredentialError(Exception):
    """Raised for any credential-related error in jasminApi objects."""
    pass


class jasminApiGeneric:
    """A generic base class for Jasmin API objects."""
    pass


class CredentialGeneric(jasminApiGeneric):
    """
    A generic credential object holding authorizations, value filters, defaults, and quotas.

    Attributes:
        authorizations: Dict of authorizations (bool flags).
        value_filters: Dict of regex patterns for filtering parameter values.
        defaults: Dict of default values for parameters.
        quotas: Dict of quotas (limits) on certain actions.
        quotas_updated: Indicates whether any quotas have been updated.
    """
    authorizations: Dict[str, bool] = {}
    value_filters: Dict[str, Pattern[bytes]] = {}
    defaults: Dict[str, Any] = {}
    quotas: Dict[str, Optional[Union[int, float]]] = {}
    quotas_updated: bool = False

    def setAuthorization(self, key: str, value: bool):
        """
        Set an authorization value.

        :param key: The authorization key to set.
        :param value: The boolean value for this authorization.
        :raises jasminApiCredentialError: If key is invalid or value is not boolean.
        """
        if key not in self.authorizations:
            raise jasminApiCredentialError(f'{key} is not a valid Authorization')
        if not isinstance(value, bool):
            raise jasminApiCredentialError(f'{key} is not a boolean value: {value}')

        self.authorizations[key] = value

    def getAuthorization(self, key: str) -> bool:
        """
        Get an authorization value.

        :param key: The authorization key.
        :return: Boolean authorization value.
        :raises jasminApiCredentialError: If key is invalid.
        """
        if key not in self.authorizations:
            raise jasminApiCredentialError(f'{key} is not a valid Authorization')

        return self.authorizations[key]

    def setValueFilter(self, key: str, value: str):
        """
        Set a value filter as a regex pattern.

        :param key: The filter key.
        :param value: The regex pattern for this filter.
        :raises jasminApiCredentialError: If key is invalid or pattern is invalid.
        """
        if key not in self.value_filters:
            raise jasminApiCredentialError(f'{key} is not a valid Filter')

        try:
            self.value_filters[key] = re.compile(value.encode())
        except TypeError:
            raise jasminApiCredentialError(f'{key} is not a regex pattern: {value}')

    def getValueFilter(self, key: str) -> Pattern[bytes]:
        """
        Get a value filter pattern.

        :param key: The filter key.
        :return: Compiled regex pattern.
        :raises jasminApiCredentialError: If key is invalid.
        """
        if key not in self.value_filters:
            raise jasminApiCredentialError(f'{key} is not a valid Filter')

        return self.value_filters[key]

    def setDefaultValue(self, key: str, value: Any):
        """
        Set a default value for a parameter.

        :param key: The default value key.
        :param value: The default value.
        :raises jasminApiCredentialError: If key is invalid.
        """
        if key not in self.defaults:
            raise jasminApiCredentialError(f'{key} is not a valid Default value')

        self.defaults[key] = value

    def getDefaultValue(self, key: str) -> Any:
        """
        Get a default value.

        :param key: The default value key.
        :return: The default value.
        :raises jasminApiCredentialError: If key is invalid.
        """
        if key not in self.defaults:
            raise jasminApiCredentialError(f'{key} is not a valid Default value')

        return self.defaults[key]

    def setQuota(self, key: str, value: Optional[Union[int, float]]):
        """
        Set a quota value.

        :param key: The quota key.
        :param value: The quota value (None or numeric).
        :raises jasminApiCredentialError: If key is invalid.
        """
        if key not in self.quotas:
            raise jasminApiCredentialError(f'{key} is not a valid Quota key')

        self.quotas[key] = value

    def updateQuota(self, key: str, difference: Union[int, float]):
        """
        Update a quota by a certain difference (increment/decrement).

        :param key: The quota key.
        :param difference: The increment/decrement value.
        :raises jasminApiCredentialError: If key is invalid or difference type is invalid.
        """
        if key not in self.quotas:
            raise jasminApiCredentialError(f'{key} is not a valid Quota key')

        if not isinstance(difference, (int, float)):
            raise jasminApiCredentialError(f'Invalid type for difference ({difference}), must be int or float')

        if isinstance(self.quotas[key], int) and isinstance(difference, float):
            raise jasminApiCredentialError('Type mismatch, cannot update an int with a float value')

        # If quota is unlimited, initialize to zero before update
        if self.quotas[key] is None:
            self.quotas[key] = 0

        self.quotas[key] += difference
        self.quotas_updated = True

    def getQuota(self, key: str) -> Optional[Union[int, float]]:
        """
        Get a quota value.

        :param key: The quota key.
        :return: The quota value or None if unlimited.
        :raises jasminApiCredentialError: If key is invalid.
        """
        if key not in self.quotas:
            raise jasminApiCredentialError(f'{key} is not a valid Quota key')

        return self.quotas[key]


class MtMessagingCredential(CredentialGeneric):
    """
    Credential set for sending MT (Mobile Terminated) messages.
    Controls authorizations, filters, defaults, and quotas related to message sending.
    """

    def __init__(self, default_authorizations: bool = True):
        if not isinstance(default_authorizations, bool):
            default_authorizations = False

        self.authorizations = {
            'http_send': default_authorizations,
            'http_bulk': False,
            'http_balance': default_authorizations,
            'http_rate': default_authorizations,
            'smpps_send': default_authorizations,
            'http_long_content': default_authorizations,
            'set_dlr_level': default_authorizations,
            'http_set_dlr_method': default_authorizations,
            'set_source_address': default_authorizations,
            'set_priority': default_authorizations,
            'set_validity_period': default_authorizations,
            'set_hex_content': default_authorizations,
            'set_schedule_delivery_time': default_authorizations,
        }

        self.value_filters = {
            'destination_address': re.compile(b'.*'),
            'source_address': re.compile(b'.*'),
            'priority': re.compile(b'^[0-3]$'),
            'validity_period': re.compile(b'^\d+$'),
            'content': re.compile(b'.*'),
        }

        self.defaults = {'source_address': None}

        self.quotas = {
            'balance': None,
            'early_decrement_balance_percent': None,
            'submit_sm_count': None,
            'http_throughput': None,
            'smpps_throughput': None,
        }

    def setQuota(self, key: str, value: Optional[Union[int, float]]):
        """
        Additional validation for MT Messaging quotas before setting them.

        :param key: Quota key.
        :param value: Quota value.
        :raises jasminApiCredentialError: If value is invalid for the given key.
        """
        if key == 'balance' and value is not None and value < 0:
            raise jasminApiCredentialError(
                f'{key} is invalid ({value}), must be None or a positive number')
        elif key == 'early_decrement_balance_percent' and value is not None and (value < 1 or value > 100):
            raise jasminApiCredentialError(
                f'{key} is invalid ({value}), must be None or in range [1..100]')
        elif key == 'submit_sm_count' and value is not None and (value < 0 or not isinstance(value, int)):
            raise jasminApiCredentialError(
                f'{key} is invalid ({value}), must be a positive int')
        elif key in ['http_throughput', 'smpps_throughput'] and value is not None and value < 0:
            raise jasminApiCredentialError(
                f'{key} is invalid ({value}), must be None or a positive number')

        super().setQuota(key, value)


class SmppsCredential(CredentialGeneric):
    """
    Credential set for SMPP Server connections.
    Controls whether a user can bind and how many bindings are allowed.
    """

    def __init__(self, default_authorizations: bool = True):
        if not isinstance(default_authorizations, bool):
            default_authorizations = False

        self.authorizations = {'bind': default_authorizations}
        self.quotas = {'max_bindings': None}

    def setQuota(self, key: str, value: Optional[int]):
        """
        Additional validation for SMPPs quotas before setting them.

        :param key: Quota key.
        :param value: Quota value.
        :raises jasminApiCredentialError: If value is invalid.
        """
        if key == 'max_bindings' and value is not None and (value < 0 or not isinstance(value, int)):
            raise jasminApiCredentialError(
                f'{key} is invalid ({value}), must be a positive int')

        super().setQuota(key, value)


class Group(jasminApiGeneric):
    """
    Represents a user group. Each user must belong to a group.

    Attributes:
        gid: Group ID (alphanumeric, underscores, hyphens).
        enabled: Indicates if the group is active.
    """

    def __init__(self, gid: str):
        # Validate gid
        if re.match(r'^[A-Za-z0-9_-]{1,16}$', gid) is None:
            raise jasminApiInvalidParamError('Group gid syntax is invalid')

        self.gid = gid
        self.enabled = True

    def disable(self):
        """Disable the group, indicating all sub-users are effectively disabled."""
        self.enabled = False

    def enable(self):
        """Enable the group, allowing sub-users to be active."""
        self.enabled = True

    def __str__(self):
        return str(self.gid)


class CnxStatus(jasminApiGeneric):
    """
    Connection status information holder.

    Holds statistics and counters related to SMPP and HTTP API usage.
    """

    def __init__(self):
        self.smpps = {
            'bind_count': 0,
            'unbind_count': 0,
            'bound_connections_count': {
                'bind_receiver': 0,
                'bind_transceiver': 0,
                'bind_transmitter': 0,
            },
            'submit_sm_request_count': 0,
            'last_activity_at': 0,
            'qos_last_submit_sm_at': 0,
            'submit_sm_count': 0,
            'deliver_sm_count': 0,
            'data_sm_count': 0,
            'elink_count': 0,
            'throttling_error_count': 0,
            'other_submit_error_count': 0,
        }

        self.httpapi = {
            'connects_count': 0,
            'last_activity_at': 0,
            'submit_sm_request_count': 0,
            'balance_request_count': 0,
            'rate_request_count': 0,
            'qos_last_submit_sm_at': 0,
        }


class UserStats(metaclass=Singleton):
    """
    A singleton to hold user statistics in-memory.

    No persistence is done. This is used for tracking usage and connection details for users.
    """
    users: Dict[str, Dict[str, CnxStatus]] = {}

    def get(self, uid: str) -> Dict[str, CnxStatus]:
        """
        Return a user's stats dictionary or create a new one if not present.

        :param uid: User ID.
        """
        if uid not in self.users:
            self.users[uid] = {'cnx': CnxStatus()}
        return self.users[uid]

    def set(self, uid: str, stats: Dict[str, CnxStatus]):
        """Set (or overwrite) a user's stats."""
        self.users[uid] = stats


class User(jasminApiGeneric):
    """
    Represents a Jasmin user.

    Attributes:
        uid: User ID.
        group: Group the user belongs to.
        username: Username (alphanumeric, underscores, hyphens).
        password: MD5 hashed password.
        mt_credential: MT Messaging credentials.
        smpps_credential: SMPP server credentials.
        enabled: Indicates if the user is active.
    """

    def __init__(
            self,
            uid: str,
            group: Group,
            username: Optional[str],
            password: Optional[str],
            mt_credential: Optional[MtMessagingCredential] = None,
            smpps_credential: Optional[SmppsCredential] = None,
            password_crypted: bool = False
    ):
        # Validate uid
        if re.match(r'^[A-Za-z0-9_-]{1,16}$', uid) is None:
            raise jasminApiInvalidParamError('User uid syntax is invalid')

        self.uid = uid
        self.group = group
        self.enabled = True

        # Validate username if provided
        if username is not None and re.match(r'^[A-Za-z0-9_-]{1,15}$', username) is None:
            raise jasminApiInvalidParamError('User username syntax is invalid')
        self.username = username

        # Password hashing if not crypted
        if password is not None and not password_crypted:
            if len(password) == 0 or len(password) > 16:
                raise jasminApiInvalidParamError('Invalid password length!')
            self.password = md5(password.encode('ascii')).digest()
        else:
            # Password is already encrypted
            self.password = password

        self.mt_credential = mt_credential if mt_credential is not None else MtMessagingCredential()
        self.smpps_credential = smpps_credential if smpps_credential is not None else SmppsCredential()

    def getCnxStatus(self) -> CnxStatus:
        """Get the user's connection status from the UserStats singleton."""
        return UserStats().get(self.uid)['cnx']

    def setCnxStatus(self, status: CnxStatus):
        """Set the user's connection status."""
        UserStats().set(self.uid, {'cnx': status})

    def disable(self):
        """Disable this user."""
        self.enabled = False

    def enable(self):
        """Enable this user."""
        self.enabled = True

    def __str__(self):
        return str(self.username)


class Connector(jasminApiGeneric):
    """
    A generic connector object. Connectors are used to route messages.

    Attributes:
        cid: Connector ID.
    """
    _type = 'generic'

    def __init__(self, cid: str):
        self.cid = cid
        self._str = f'{self._type.capitalize()} Connector'
        self._repr = f'<{self._type.capitalize()} Connector>'

    def __repr__(self):
        return self._repr

    def __str__(self):
        return self._str


class HttpConnector(Connector):
    """
    HTTP Client connector used to forward MO messages via HTTP calls.
    """
    _type = 'http'

    def __init__(self, cid: str, baseurl: str, method: str = 'GET'):
        # Validate cid
        if re.match(r'^[A-Za-z0-9_-]{3,25}$', cid) is None:
            raise jasminApiInvalidParamError('HttpConnector cid syntax is invalid')

        # Validate method
        if method.lower() not in ['get', 'post']:
            raise jasminApiInvalidParamError('HttpConnector method must be GET or POST')

        # Validate URL
        url_pattern = re.compile(
            r'^(?:http)s?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain
            r'localhost|'  # localhost
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|'  # ipv4
            r'\[?[A-F0-9]*:[A-F0-9:]+\]?)'  # ipv6
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        if url_pattern.match(baseurl) is None:
            raise jasminApiInvalidParamError('HttpConnector url syntax is invalid')

        super().__init__(cid)
        self.baseurl = baseurl
        self.method = method

        self._repr = f'<HttpConnector (cid={self.cid}, baseurl={self.baseurl}, method={self.method})>'
        self._str = (
            f'HttpConnector:\n'
            f'cid = {self.cid}\n'
            f'baseurl = {self.baseurl}\n'
            f'method = {self.method}'
        )


class SmppClientConnector(Connector):
    """SMPP Client connector."""
    _type = 'smppc'

    def __init__(self, cid: str):
        super().__init__(cid)


class SmppServerSystemIdConnector(Connector):
    """
    SMPP Server connector mapped to a system_id. Used to deliver messages through
    the SMPP server to a bound system_id (receiver or transceiver).
    """
    _type = 'smpps'

    def __init__(self, system_id: str):
        super().__init__(system_id)
        self.system_id = system_id


class InterceptorScript(jasminApiGeneric):
    """
    Represents a generic Python script for message interception.

    :param pyCode: The Python code of the script.
    """
    _type = 'generic'

    def __init__(self, pyCode: str):
        self.pyCode = pyCode
        short_pyCode = pyCode[:30].replace('\n', "")
        self._repr = f'<IS (pyCode={short_pyCode} ..)>'
        self._str = f'{self.__class__.__name__}:\n{pyCode}'

    def __repr__(self):
        return self._repr

    def __str__(self):
        return self._str


class MOInterceptorScript(InterceptorScript):
    """
    A script for MO (Mobile Originated) message interception.
    """
    _type = 'moi'

    def __init__(self, pyCode: str):
        super().__init__(pyCode)
        short_pyCode = pyCode[:30].replace('\n', "")
        self._repr = f'<MOIS (pyCode={short_pyCode} ..)>'
        self._str = f'{self.__class__.__name__}:\n{pyCode}'


class MTInterceptorScript(InterceptorScript):
    """
    A script for MT (Mobile Terminated) message interception.
    """
    _type = 'mti'

    def __init__(self, pyCode: str):
        super().__init__(pyCode)
        short_pyCode = pyCode[:30].replace('\n', '')
        self._repr = f'<MTIS (pyCode={short_pyCode} ..)>'
        self._str = f'{self.__class__.__name__}:\n{pyCode}'
