"""
SMPP validators
"""

import re
from enum import Enum

from jasmin.protocols.validation import AbstractCredentialValidator
from jasmin.protocols.smpp.error import (
    AuthorizationError, FilterError, CredentialValidationError
)
from smpp.pdu.constants import priority_flag_value_map, priority_flag_name_map
from smpp.pdu.pdu_types import RegisteredDeliveryReceipt, RegisteredDelivery


class SmppsCredentialValidator(AbstractCredentialValidator):
    """
    Validates SMPP credentials for a given user and submit_sm PDU.

    The validator checks authorizations (such as sending MT messages, setting DLR levels,
    source addresses, or priority) and applies filters to ensure message parameters (destination,
    source, priority, content) match user-defined filters.

    Attributes:
        action (str): The action to validate, such as 'Send'.
        user: The user object holding credentials and authorizations.
        submit_sm: The SubmitSM PDU to be validated.
    """

    def __init__(self, action, user, submit_sm):
        super().__init__(action, user)
        self.submit_sm = submit_sm

    def _checkSendAuthorizations(self):
        """
        Check MT authorizations:

        - smpps_send: User must be authorized to send MT messages.
        - set_dlr_level: User must be authorized if a DLR level other than NO_SMSC_DELIVERY_RECEIPT_REQUESTED is used.
        - set_source_address: User must be authorized if a non-empty source address is used.
        - set_priority: User must be authorized if a priority other than the default is used.
        """
        if not self.user.mt_credential.getAuthorization('smpps_send'):
            raise AuthorizationError(
                f'Authorization failed for username [{self.user}] (Cannot send MT messages).')

        if (not self.user.mt_credential.getAuthorization('set_dlr_level') and
                self.submit_sm.params['registered_delivery'] != RegisteredDelivery(
                    RegisteredDeliveryReceipt.NO_SMSC_DELIVERY_RECEIPT_REQUESTED)):
            raise AuthorizationError(
                f'Authorization failed for username [{self.user}] (Setting dlr level not authorized).')

        if (not self.user.mt_credential.getAuthorization('set_source_address') and
                len(self.submit_sm.params['source_addr']) > 0):
            raise AuthorizationError(
                f'Authorization failed for username [{self.user}] (Setting source address not authorized).')

        if (not self.user.mt_credential.getAuthorization('set_priority') and
                self.submit_sm.params['priority_flag'] != priority_flag_value_map[0]):
            raise AuthorizationError(
                f'Authorization failed for username [{self.user}] (Setting priority not authorized).')

    def _get_binary_r(self, key, credential=None):
        """
        Retrieve a binary regex pattern from the user's MT credentials.

        :param key: The key in the credentials to retrieve the regex pattern for.
        :param credential: The credential object, defaults to user's mt_credential if not provided.
        :return: A compiled regex object for the given key.
        """
        if credential is None:
            credential = self.user.mt_credential

        r = credential.getValueFilter(key)
        # Ensure pattern is in binary form for matching
        if isinstance(r.pattern, str):
            r = re.compile(r.pattern.encode())
        return r

    def _checkSendFilters(self):
        """
        Apply user-defined value filters to destination_address, source_address, priority, and content.
        Raises FilterError if any filter does not match the given message parameter.
        """
        # Check destination_address filter
        _value = self.submit_sm.params['destination_addr']
        _r = self._get_binary_r('destination_address')
        if _r is None or (_r.pattern != b'.*' and not _r.match(_value)):
            raise FilterError(
                f'Value filter failed for username [{self.user}] (destination_address filter mismatch).',
                'destination_address')

        # Check source_address filter
        _value = self.submit_sm.params['source_addr']
        _r = self._get_binary_r('source_address')
        if _r is None or (_r.pattern != b'.*' and not _r.match(_value)):
            raise FilterError(
                f'Value filter failed for username [{self.user}] (source_address filter mismatch).',
                'source_address')

        # Check priority_flag filter
        _value = f"{priority_flag_name_map[self.submit_sm.params['priority_flag'].name]}".encode()
        _r = self._get_binary_r('priority')
        if _r is None or (_r.pattern != b'^[0-3]$' and not _r.match(_value)):
            raise FilterError(
                f'Value filter failed for username [{self.user}] (priority filter mismatch).',
                'priority')

        # Check content filter
        _value = self.submit_sm.params['short_message']
        _r = self._get_binary_r('content')
        if _r is None or (_r.pattern != b'.*' and not _r.match(_value)):
            raise FilterError(
                f'Value filter failed for username [{self.user}] (content filter mismatch).',
                'content')

    def updatePDUWithUserDefaults(self, PDU):
        """
        Update the SubmitSmPDU parameters with user default values, if not already set.

        For example, if the user specifies a default source_address and the PDU lacks one,
        it will be applied here.

        :param PDU: The SubmitSmPDU object to update.
        :return: Updated PDU object.
        """
        if (self.user.mt_credential.getDefaultValue('source_address') is not None and
                (PDU.params['source_addr'] is None or len(PDU.params['source_addr']) == 0)):
            PDU.params['source_addr'] = self.user.mt_credential.getDefaultValue('source_address')

        return PDU

    def validate(self):
        """
        Perform credential validation based on the action.

        Currently supported actions:
        - 'Send': Checks authorizations and applies value filters.
        """
        if self.action == 'Send':
            self._checkSendAuthorizations()
            self._checkSendFilters()
        else:
            raise CredentialValidationError(f'Unknown action [{self.action}].')
