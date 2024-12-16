"""
Multiple classes extending txamqp.content.Content for handling various SMPP and DLR message payloads.
"""

import pickle
import datetime
import uuid
from enum import Enum
from typing import Any, Dict, Optional, Callable

from txamqp.content import Content
from smpp.pdu.pdu_types import CommandId, CommandStatus
from pkg_resources import iter_entry_points


class InvalidParameterError(Exception):
    """Raised when a given parameter is invalid."""


# Default msgid generator function
def default_msgid_generator(pdu_type: str, uid: Optional[str], source_cid: Optional[str], destination_cid: Optional[str]) -> str:
    return str(uuid.uuid4())


# Attempt to load a msgid generator plugin if available
randomUniqueId: Callable[[str, Optional[str], Optional[str], Optional[str]], str] = default_msgid_generator
for entry_point in iter_entry_points(group='jasmin.content', name='msgid'):
    print(f"Hooking randomUniqueId() from {entry_point.dist}")
    randomUniqueId = entry_point.load()
    # Break after loading the first found plugin
    break


class PDU(Content):
    """A generic SMPP PDU Content object that encapsulates a PDU payload."""

    def __init__(self,
                 body: Any = "",
                 children: Any = None,
                 properties: Optional[Dict[str, Any]] = None,
                 pickleProtocol: int = pickle.HIGHEST_PROTOCOL,
                 prePickle: bool = False):
        """
        :param body: Body of the message.
        :param children: Children content elements (unused).
        :param properties: Additional properties to store in the Content.
        :param pickleProtocol: The pickle protocol to use if prePickle is True.
        :param prePickle: If True, the body will be pickled.
        """
        if properties is None:
            properties = {}

        self.pickleProtocol = pickleProtocol

        if prePickle:
            body = pickle.dumps(body, self.pickleProtocol)

        # Add creation date in header
        if 'headers' not in properties:
            properties['headers'] = {}
        properties['headers']['created_at'] = str(datetime.datetime.now())

        super().__init__(body, children, properties)


class DLR(Content):
    """
    A DLR Content used for DLRLookup. It can represent either submit_sm_resp (ESME_ROK),
    deliver_sm, or data_sm PDUs with varying headers.
    """

    def __init__(self,
                 pdu_type: CommandId,
                 msgid: str,
                 status: Any,
                 smpp_msgid: Optional[bytes] = None,
                 cid: Optional[str] = None,
                 dlr_details: Optional[Dict[str, Any]] = None):
        """
        :param pdu_type: Type of the PDU (CommandId).
        :param msgid: The message ID associated with this DLR.
        :param status: The status of the DLR (CommandStatus enum or a valid string).
        :param smpp_msgid: SMPP message ID used in case of submit_sm_resp.
        :param cid: Connector ID (for deliver_sm or data_sm).
        :param dlr_details: Additional DLR details (for deliver_sm or data_sm).
        """

        # Validate pdu_type
        if pdu_type not in (CommandId.deliver_sm, CommandId.data_sm, CommandId.submit_sm_resp):
            raise InvalidParameterError(f"Invalid pdu_type: {pdu_type.name}")

        # Validation depending on pdu_type
        if pdu_type == CommandId.submit_sm_resp and status == CommandStatus.ESME_ROK and smpp_msgid is None:
            raise InvalidParameterError("submit_sm_resp with ESME_ROK requires smpp_msgid.")
        elif pdu_type in (CommandId.deliver_sm, CommandId.data_sm) and (cid is None or dlr_details is None):
            raise InvalidParameterError("deliver_sm/data_sm dlr requires cid and dlr_details.")

        # Build properties
        properties = {'message-id': str(msgid), 'headers': {'type': pdu_type.name}}

        if pdu_type == CommandId.submit_sm_resp and smpp_msgid is not None:
            # Map msgid to smpp_msgid
            properties['headers']['smpp_msgid'] = smpp_msgid.decode().upper().lstrip('0')
        elif pdu_type in (CommandId.deliver_sm, CommandId.data_sm):
            properties['headers']['cid'] = cid
            for k, v in dlr_details.items():
                # Decode bytes into string if needed
                properties['headers'][f'dlr_{k}'] = v.decode() if isinstance(v, bytes) else v

        # Convert Enum status to name if needed
        if isinstance(status, Enum):
            status = status.name

        super().__init__(status, properties=properties)


class DLRContentForHttpapi(Content):
    """
    A DLR Content carrying information about the origin SubmitSm sent from httpapi
    and the subsequent receipt acknowledgment details.
    """

    VALID_METHODS = ['POST', 'GET']
    VALID_STATUSES = ['DELIVRD', 'EXPIRED', 'DELETED', 'UNDELIV', 'ACCEPTD', 'UNKNOWN', 'REJECTD']

    def __init__(self,
                 message_status: str,
                 msgid: str,
                 dlr_url: str,
                 dlr_level: int,
                 dlr_connector: str = 'unknown',
                 id_smsc: str = '',
                 sub: str = '',
                 dlvrd: str = '',
                 subdate: str = '',
                 donedate: str = '',
                 err: str = '',
                 text: str = '',
                 method: str = 'POST',
                 trycount: int = 0):
        """
        :param message_status: The message status (ESME_* or one of the DLR statuses defined in SMPP spec).
        :param msgid: The message ID.
        :param dlr_url: Callback URL for DLR notifications.
        :param dlr_level: DLR level (1, 2, or 3).
        :param dlr_connector: The connector name.
        :param id_smsc: SMSC ID.
        :param sub: Number of short messages originally submitted.
        :param dlvrd: Number of short messages delivered.
        :param subdate: Submit date.
        :param donedate: Done date.
        :param err: Error code.
        :param text: The short message text.
        :param method: HTTP method for callback (POST or GET).
        :param trycount: Retry count for callback attempts.
        """

        if not (message_status.startswith('ESME_') or message_status in self.VALID_STATUSES):
            raise InvalidParameterError(f"Invalid message_status: {message_status}")

        if dlr_level not in [1, 2, 3]:
            raise InvalidParameterError(f"Invalid dlr_level: {dlr_level}")

        if method not in self.VALID_METHODS:
            raise InvalidParameterError(f"Invalid method: {method}")

        properties = {
            'message-id': msgid,
            'headers': {
                'try-count': trycount,
                'url': dlr_url,
                'method': method,
                'message_status': message_status,
                'level': dlr_level,
                'id_smsc': id_smsc,
                'sub': sub,
                'dlvrd': dlvrd,
                'subdate': subdate,
                'donedate': donedate,
                'err': err,
                'connector': dlr_connector,
                'text': text
            }
        }

        super().__init__(msgid, properties=properties)


class DLRContentForSmpps(Content):
    """
    A DLR Content holding information about the origin SubmitSm sent from smpps and
    the corresponding receipt acknowledgment details.
    """

    VALID_STATUSES = ['DELIVRD', 'EXPIRED', 'DELETED', 'UNDELIV', 'ACCEPTD', 'UNKNOWN', 'REJECTD']

    def __init__(self,
                 message_status: str,
                 msgid: str,
                 system_id: str,
                 source_addr: str,
                 destination_addr: str,
                 sub_date: datetime.datetime,
                 source_addr_ton: int,
                 source_addr_npi: int,
                 dest_addr_ton: int,
                 dest_addr_npi: int,
                 err: int = 99):
        """
        :param message_status: The message status (ESME_* or one of the DLR statuses).
        :param msgid: The message ID.
        :param system_id: The system ID.
        :param source_addr: The source address.
        :param destination_addr: The destination address.
        :param sub_date: The submit date and time.
        :param source_addr_ton: Type of number for source address.
        :param source_addr_npi: Numbering plan indicator for source address.
        :param dest_addr_ton: Type of number for destination address.
        :param dest_addr_npi: Numbering plan indicator for destination address.
        :param err: Error code.
        """

        if not (message_status.startswith('ESME_') or message_status in self.VALID_STATUSES):
            raise InvalidParameterError(f"Invalid message_status: {message_status}")

        properties = {
            'message-id': msgid,
            'headers': {
                'try-count': 0,
                'message_status': message_status,
                'err': err,
                'system_id': system_id,
                'source_addr': source_addr,
                'destination_addr': destination_addr,
                'sub_date': str(sub_date),
                'source_addr_ton': str(source_addr_ton),
                'source_addr_npi': str(source_addr_npi),
                'dest_addr_ton': str(dest_addr_ton),
                'dest_addr_npi': str(dest_addr_npi)
            }
        }

        super().__init__(msgid, properties=properties)


class SubmitSmContent(PDU):
    """
    A SMPP SubmitSm Content.
    """

    def __init__(self,
                 uid: str,
                 body: Any,
                 replyto: str,
                 submit_sm_bill: Optional[Any] = None,
                 priority: int = 1,
                 expiration: Optional[str] = None,
                 msgid: Optional[str] = None,
                 source_connector: str = 'httpapi',
                 destination_cid: Optional[str] = None):
        """
        :param uid: User ID.
        :param body: The message body.
        :param replyto: Queue or mechanism to send replies to.
        :param submit_sm_bill: Billing information.
        :param priority: Message priority (0 to 3).
        :param expiration: Expiration time (if any).
        :param msgid: Message ID. If None, a new one is generated.
        :param source_connector: Connector source type ('httpapi' or 'smppsapi').
        :param destination_cid: Destination connector ID.
        """

        if not isinstance(priority, int) or priority < 0 or priority > 3:
            raise InvalidParameterError(f"Priority must be between 0 and 3, got: {priority}")

        if source_connector not in ['httpapi', 'smppsapi']:
            raise InvalidParameterError(f"Invalid source_connector: {source_connector}")

        if msgid is None:
            msgid = randomUniqueId('submit_sm', uid, source_connector, destination_cid)

        props = {
            'priority': priority,
            'message-id': msgid,
            'reply-to': replyto,
            'headers': {'source_connector': source_connector}
        }

        if submit_sm_bill is not None:
            props['headers']['submit_sm_bill'] = submit_sm_bill
        if expiration is not None:
            props['headers']['expiration'] = expiration

        super().__init__(body, properties=props)


class SubmitSmRespContent(PDU):
    """
    A SMPP SubmitSmResp Content.
    """

    def __init__(self,
                 body: Any,
                 msgid: str,
                 pickleProtocol: int = pickle.HIGHEST_PROTOCOL,
                 prePickle: bool = True):
        """
        :param body: The body containing the response info (e.g. CommandStatus).
        :param msgid: The related message ID.
        :param pickleProtocol: The pickle protocol used if prePickle is True.
        :param prePickle: If True, pickle the body.
        """
        props = {'message-id': msgid}
        super().__init__(body, properties=props, pickleProtocol=pickleProtocol, prePickle=prePickle)


class DeliverSmContent(PDU):
    """
    A SMPP DeliverSm Content.
    """

    def __init__(self,
                 body: Any,
                 sourceCid: str,
                 pickleProtocol: int = pickle.HIGHEST_PROTOCOL,
                 prePickle: bool = True,
                 concatenated: bool = False,
                 will_be_concatenated: bool = False):
        """
        :param body: The message body.
        :param sourceCid: Source connector ID.
        :param pickleProtocol: The pickle protocol used if prePickle is True.
        :param prePickle: If True, pickle the body.
        :param concatenated: Indicates if the message is part of a concatenated message.
        :param will_be_concatenated: Indicates if the message will be concatenated later.
        """

        props = {
            'message-id': randomUniqueId('deliver_sm', None, sourceCid, None),
            'headers': {
                'try-count': 0,
                'connector-id': sourceCid,
                'concatenated': concatenated,
                'will_be_concatenated': will_be_concatenated
            }
        }

        super().__init__(body, properties=props, pickleProtocol=pickleProtocol, prePickle=prePickle)


class SubmitSmRespBillContent(Content):
    """
    A Bill Content holding the amount to be charged to a user (uid) after a SubmitSmResp.
    """

    def __init__(self, bid: str, uid: str, amount: float):
        """
        :param bid: Bill ID.
        :param uid: User ID.
        :param amount: Amount to charge. Must be non-negative.
        """
        if not isinstance(amount, (float, int)):
            raise InvalidParameterError(f"Amount must be a float or int, got: {amount}")

        if amount < 0:
            raise InvalidParameterError(f"Amount cannot be negative, got: {amount}")

        properties = {'message-id': bid, 'headers': {'user-id': uid, 'amount': str(amount)}}
        super().__init__(bid, properties=properties)
