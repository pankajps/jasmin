import datetime
import math
import re
import struct
from enum import Enum

import dateutil.parser as parser

from jasmin.protocols.smpp.configs import SMPPClientConfig
from smpp.pdu.operations import SubmitSM, DataSM, DeliverSM
from smpp.pdu.pdu_types import (
    EsmClass, EsmClassMode, EsmClassType, EsmClassGsmFeatures,
    MoreMessagesToSend, MessageState, AddrTon, AddrNpi
)

message_state_map = {
    MessageState.ACCEPTED: 'ACCEPTD',
    MessageState.UNDELIVERABLE: 'UNDELIV',
    MessageState.REJECTED: 'REJECTD',
    MessageState.DELIVERED: 'DELIVRD',
    MessageState.EXPIRED: 'EXPIRED',
    MessageState.DELETED: 'DELETED',
    MessageState.UNKNOWN: 'UNKNOWN',
}


class UnknownMessageStatusError(Exception):
    """Raised when the message_status is not recognized as a known state."""
    pass


class SMPPOperationFactory:
    """
    A factory for creating SMPP PDUs, handling message segmentation (for long messages),
    and constructing delivery receipts.

    Attributes:
        config (SMPPClientConfig): SMPP configuration.
        long_content_max_parts (int): Maximum number of parts allowed for long messages.
        long_content_split (str): Method of splitting long messages ('sar' or 'udh').
        lastLongMsgRefNum (int): Tracks the reference number used for segmented messages.
    """

    lastLongMsgRefNum = 0

    def __init__(self, config=None, long_content_max_parts=5, long_content_split='sar'):
        if config is not None:
            self.config = config
        else:
            # Fallback to a default config if none provided
            self.config = SMPPClientConfig(**{'id': 'anyid'})

        self.long_content_max_parts = int(long_content_max_parts)
        if isinstance(long_content_split, bytes):
            long_content_split = long_content_split.decode()
        self.long_content_split = long_content_split

    def _setConfigParamsInPDU(self, pdu, kwargs):
        """
        Ensure that all mandatory PDU parameters are set, using the config defaults if not provided.

        :param pdu: The PDU object to configure.
        :param kwargs: Keyword arguments possibly containing PDU parameters.
        :return: The updated PDU object.
        """
        for param in pdu.mandatoryParams:
            if param not in kwargs:
                try:
                    pdu.params[param] = getattr(self.config, param)
                except AttributeError:
                    pdu.params[param] = None
        return pdu

    def isDeliveryReceipt(self, pdu):
        """
        Determines if the given pdu is a delivery receipt. If so, returns a dictionary of DLR elements.

        Process:
        1. Checks optional parameters (receipted_message_id and message_state).
        2. If available, attempts to parse additional fields from the short_message content.

        :param pdu: The PDU to analyze (DeliverSM or DataSM).
        :return: Dict containing DLR fields if it's a DLR, None otherwise.
        """
        if not isinstance(pdu, (DeliverSM, DataSM)):
            return None

        # Prepare default return structure
        ret = {'dlvrd': 'ND', 'sub': 'ND', 'sdate': 'ND', 'ddate': 'ND', 'err': 'ND', 'text': ''}

        # Step 1: Check optional parameters
        if 'receipted_message_id' in pdu.params and 'message_state' in pdu.params:
            ret['id'] = pdu.params['receipted_message_id']
            msg_state = pdu.params['message_state']
            ret['stat'] = message_state_map.get(msg_state, 'UNKNOWN')

        # Step 2: Parse short_message if present
        # Sample format:
        #   "id:IIIIIIIIII sub:SSS dlvrd:DDD submit date:YYMMDDhhmm done date:YYMMDDhhmm stat:XXXXXXX err:EEE text:..."
        if 'short_message' in pdu.params and pdu.params['short_message'] is not None:
            sm = pdu.params['short_message']
            sm_str = sm.decode('utf-8', 'ignore') if isinstance(sm, bytes) else sm
            patterns = [
                r"id:(?P<id>[\dA-Za-z-_]+)",
                r"sub:(?P<sub>\d{1,3})",
                r"dlvrd:(?P<dlvrd>\d{1,3})",
                r"submit date:(?P<sdate>\d+)",
                r"done date:(?P<ddate>\d+)",
                r"stat:(?P<stat>\w{7})",
                r"err:(?P<err>\w{1,3})",
                r"[tT]ext:(?P<text>.*)",
            ]

            for pattern in patterns:
                m = re.search(pattern, sm_str)
                if m:
                    k = list(m.groupdict())[0]
                    # Update only if key not already set from optional params or if id/stat not previously set
                    if (k not in ['id', 'stat']) or (k == 'id' and 'id' not in ret) or (k == 'stat' and 'stat' not in ret):
                        ret.update(m.groupdict())

        # Normalize lengths for sub/dlvrd/err if they are not 'ND'
        for field in ['sub', 'dlvrd', 'err']:
            if ret[field] != 'ND' and len(ret[field]) < 3:
                ret[field] = f'{int(ret[field]):03}'

        # Validate if this can be considered a DLR
        if 'id' in ret and 'stat' in ret:
            return ret
        else:
            return None

    def claimLongMsgRefNum(self):
        """
        Returns a unique reference number for long (segmented) messages.
        Cycles through 1 to 255.
        """
        if self.lastLongMsgRefNum >= 255:
            self.lastLongMsgRefNum = 0
        self.lastLongMsgRefNum += 1
        return self.lastLongMsgRefNum

    def SubmitSM(self, short_message, data_coding=0, **kwargs):
        """
        Creates a SubmitSM PDU. If the message is longer than allowed, splits it into multiple parts,
        linked using SAR or UDH headers.

        :param short_message: The message content.
        :param data_coding: Data coding scheme.
        :param kwargs: Additional PDU parameters.
        :return: A single SubmitSM or the first SubmitSM of a linked chain.
        """

        kwargs['short_message'] = short_message
        kwargs['data_coding'] = data_coding

        # Determine message length constraints based on data_coding
        if data_coding in [3, 6, 7, 10]:
            # 8 bit coding
            bits = 8
            maxSmLength = 140
            slicedMaxSmLength = maxSmLength - 6
        elif data_coding in [2, 4, 5, 8, 9, 13, 14]:
            # 16 bit coding
            bits = 16
            maxSmLength = 70
            slicedMaxSmLength = maxSmLength - 3
        else:
            # Default to 7 bit coding
            bits = 7
            maxSmLength = 160
            slicedMaxSmLength = 153

        longMessage = kwargs['short_message']
        smLength = len(longMessage) / 2 if bits == 16 else len(longMessage)

        # Handle long messages
        if smLength > maxSmLength:
            total_segments = int(math.ceil(smLength / float(slicedMaxSmLength)))
            # Obey configured maximum parts
            if total_segments > self.long_content_max_parts:
                total_segments = self.long_content_max_parts

            msg_ref_num = self.claimLongMsgRefNum()

            for i in range(total_segments):
                segment_seqnum = i + 1
                try:
                    previousPdu  # Check existence
                    previousPdu = tmpPdu
                except NameError:
                    previousPdu = None

                # Slice message
                if bits == 16:
                    kwargs['short_message'] = longMessage[slicedMaxSmLength * i * 2:slicedMaxSmLength * (i + 1) * 2]
                else:
                    kwargs['short_message'] = longMessage[slicedMaxSmLength * i:slicedMaxSmLength * (i + 1)]

                tmpPdu = self._setConfigParamsInPDU(SubmitSM(**kwargs), kwargs)

                if self.long_content_split == 'sar':
                    # SAR options
                    tmpPdu.params['sar_total_segments'] = total_segments
                    tmpPdu.params['sar_segment_seqnum'] = segment_seqnum
                    tmpPdu.params['sar_msg_ref_num'] = msg_ref_num
                elif self.long_content_split == 'udh':
                    # UDH header
                    tmpPdu.params['esm_class'] = EsmClass(
                        EsmClassMode.DEFAULT, EsmClassType.DEFAULT, [EsmClassGsmFeatures.UDHI_INDICATOR_SET])
                    tmpPdu.params['more_messages_to_send'] = (
                        MoreMessagesToSend.MORE_MESSAGES if segment_seqnum < total_segments else MoreMessagesToSend.NO_MORE_MESSAGES
                    )

                    # UDH construction
                    udh = [
                        struct.pack('!B', 5),  # Length of UDH
                        struct.pack('!B', 0),  # IEI for concatenated messages, 8-bit reference
                        struct.pack('!B', 3),  # Length of the header, excluding IEI and length fields
                        struct.pack('!B', msg_ref_num),
                        struct.pack('!B', total_segments),
                        struct.pack('!B', segment_seqnum)
                    ]

                    if isinstance(kwargs['short_message'], str):
                        tmpPdu.params['short_message'] = b''.join(udh) + kwargs['short_message'].encode()
                    else:
                        tmpPdu.params['short_message'] = b''.join(udh) + kwargs['short_message']

                if i == 0:
                    pdu = tmpPdu
                if previousPdu is not None:
                    previousPdu.nextPdu = tmpPdu
        else:
            pdu = self._setConfigParamsInPDU(SubmitSM(**kwargs), kwargs)

        return pdu

    def getReceipt(self, dlr_pdu, msgid, source_addr, destination_addr, message_status, err, sub_date,
                   source_addr_ton, source_addr_npi, dest_addr_ton, dest_addr_npi):
        """
        Constructs a DataSM or DeliverSM as a delivery receipt, including message status.

        :param dlr_pdu: 'deliver_sm' or 'data_sm'
        :param msgid: Message ID
        :param source_addr: Source address
        :param destination_addr: Destination address
        :param message_status: Status of the message (e.g. DELIVRD, UNDELIV)
        :param err: Error code
        :param sub_date: Submission date
        :param source_addr_ton: Source address TON
        :param source_addr_npi: Source address NPI
        :param dest_addr_ton: Destination address TON
        :param dest_addr_npi: Destination address NPI
        :return: A DataSM or DeliverSM PDU containing the receipt.
        """
        if isinstance(message_status, bytes):
            message_status = message_status.decode()
        if isinstance(msgid, bytes):
            msgid = msgid.decode()
        if isinstance(err, bytes):
            err = err.decode()

        # Map message_status to SMPP message_state
        if message_status.startswith('ESME_'):
            if message_status == 'ESME_ROK':
                message_state = MessageState.ACCEPTED
                sm_message_stat = 'ACCEPTD'
            else:
                message_state = MessageState.UNDELIVERABLE
                sm_message_stat = 'UNDELIV'
        else:
            # Attempt direct mapping
            sm_message_stat = message_status
            if message_status == 'UNDELIV':
                message_state = MessageState.UNDELIVERABLE
            elif message_status == 'REJECTD':
                message_state = MessageState.REJECTED
            elif message_status == 'DELIVRD':
                message_state = MessageState.DELIVERED
            elif message_status == 'EXPIRED':
                message_state = MessageState.EXPIRED
            elif message_status == 'DELETED':
                message_state = MessageState.DELETED
            elif message_status == 'ACCEPTD':
                message_state = MessageState.ACCEPTED
            elif message_status == 'ENROUTE':
                message_state = MessageState.ENROUTE
            elif message_status == 'UNKNOWN':
                message_state = MessageState.UNKNOWN
            else:
                raise UnknownMessageStatusError(f'Unknown message_status: {message_status}')

        # Construct the receipt PDU
        if dlr_pdu == 'deliver_sm':
            short_message = (
                f"id:{msgid} submit date:{parser.parse(sub_date).strftime('%y%m%d%H%M')} "
                f"done date:{datetime.datetime.now().strftime('%y%m%d%H%M')} "
                f"stat:{sm_message_stat} err:{err}"
            )

            pdu = DeliverSM(
                source_addr=destination_addr,
                destination_addr=source_addr,
                esm_class=EsmClass(EsmClassMode.DEFAULT, EsmClassType.SMSC_DELIVERY_RECEIPT),
                receipted_message_id=msgid,
                short_message=short_message,
                message_state=message_state,
                source_addr_ton=self.get_enum(AddrTon, dest_addr_ton),
                source_addr_npi=self.get_enum(AddrNpi, dest_addr_npi),
                dest_addr_ton=self.get_enum(AddrTon, source_addr_ton),
                dest_addr_npi=self.get_enum(AddrNpi, source_addr_npi),
            )
        else:
            # DataSM
            pdu = DataSM(
                source_addr=destination_addr,
                destination_addr=source_addr,
                esm_class=EsmClass(EsmClassMode.DEFAULT, EsmClassType.SMSC_DELIVERY_RECEIPT),
                receipted_message_id=msgid,
                message_state=message_state,
                source_addr_ton=self.get_enum(AddrTon, dest_addr_ton),
                source_addr_npi=self.get_enum(AddrNpi, dest_addr_npi),
                dest_addr_ton=self.get_enum(AddrTon, source_addr_ton),
                dest_addr_npi=self.get_enum(AddrNpi, source_addr_npi),
            )

        return pdu

    def get_enum(self, enum_type, value):
        """
        Safely retrieve an enum value from a string, bytes, or Enum.

        :param enum_type: The enum class to retrieve values from.
        :param value: The enum member or its name as a string.
        :return: The enum member corresponding to the given value.
        """
        if isinstance(value, Enum):
            return value

        val_parts = value.split('.')
        if len(val_parts) == 2:
            return getattr(enum_type, val_parts[1])
        else:
            return getattr(enum_type, value)
