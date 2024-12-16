# pylint: disable=W0401,W0611
import logging
import re
import struct
import uuid
from datetime import datetime

from twisted.cred import error
from twisted.internet import defer, reactor

from smpp.pdu.constants import data_coding_default_value_map
from smpp.pdu.error import (
    SMPPClientConnectionCorruptedError, SMPPRequestTimoutError,
    SMPPSessionInitTimoutError, SMPPProtocolError,
    SMPPGenericNackTransactionError, SMPPTransactionError,
    SMPPClientError, SessionStateError
)
from smpp.pdu.operations import SubmitSM, GenericNack
from smpp.pdu.pdu_types import (
    CommandId, CommandStatus, DataCoding,
    DataCodingDefault, PDURequest, PDUResponse, EsmClassGsmFeatures
)
from smpp.twisted.protocol import (
    SMPPClientProtocol as twistedSMPPClientProtocol,
    SMPPServerProtocol as twistedSMPPServerProtocol,
    SMPPSessionStates, SMPPOutboundTxn, SMPPOutboundTxnResult
)
from .error import (
    LongSubmitSmTransactionError
)

# LOG_CATEGORY seems unused, but kept here for reference
LOG_CATEGORY = "smpp.twisted.protocol"


class SMPPClientProtocol(twistedSMPPClientProtocol):
    """
    Overridden SMPP client protocol with additional logging, error handling,
    and statistics tracking.
    """

    def __init__(self):
        super().__init__()
        self.longSubmitSmTxns = {}

    def PDUReceived(self, pdu):
        """
        Handler for received PDUs. Logs the incoming PDU and handles it
        according to whether it is a request or response.
        """
        self.log.debug(
            "SMPP Client received PDU [command: %s, seq_number: %s, command_status: %s]",
            pdu.commandId, pdu.seqNum, pdu.status
        )
        self.log.debug("Complete PDU dump: %s", pdu)
        self.factory.stats.set('last_received_pdu_at', datetime.now())

        # Signal SMPP operation
        self.onSMPPOperation()

        if isinstance(pdu, PDURequest):
            self.PDURequestReceived(pdu)
        elif isinstance(pdu, PDUResponse):
            self.PDUResponseReceived(pdu)
        else:
            getattr(self, f"onPDU_{pdu.commandId.name}")(pdu)

    def connectionMade(self):
        """
        Called when the connection is established.
        """
        super().connectionMade()
        self.factory.stats.set('connected_at', datetime.now())
        self.factory.stats.inc('connected_count')
        self.log.info("Connection made to %s:%s", self.config().host, self.config().port)
        self.factory.connectDeferred.callback(self)

    def connectionLost(self, reason):
        """
        Called when the connection is lost.
        """
        super().connectionLost(reason)
        self.factory.stats.set('disconnected_at', datetime.now())
        self.factory.stats.inc('disconnected_count')

    def doPDURequest(self, reqPDU, handler):
        """
        Handle a PDU request and update statistics.
        """
        super().doPDURequest(reqPDU, handler)
        if reqPDU.commandId == CommandId.enquire_link:
            self.factory.stats.set('last_received_elink_at', datetime.now())
        elif reqPDU.commandId == CommandId.deliver_sm:
            self.factory.stats.inc('deliver_sm_count')
        elif reqPDU.commandId == CommandId.data_sm:
            self.factory.stats.inc('data_sm_count')

    def PDUResponseReceived(self, pdu):
        """
        Handle responses and update statistics for submit_sm_resp PDUs.
        """
        super().PDUResponseReceived(pdu)
        if pdu.commandId == CommandId.submit_sm_resp:
            if pdu.status == CommandStatus.ESME_RTHROTTLED:
                self.factory.stats.inc('throttling_error_count')
            elif pdu.status != CommandStatus.ESME_ROK:
                self.factory.stats.inc('other_submit_error_count')
            else:
                self.factory.stats.inc('submit_sm_count')

    def sendPDU(self, pdu):
        """
        Sends a PDU and updates relevant statistics.
        """
        super().sendPDU(pdu)
        self.factory.stats.set('last_sent_pdu_at', datetime.now())
        if pdu.commandId == CommandId.enquire_link:
            self.factory.stats.set('last_sent_elink_at', datetime.now())
            self.factory.stats.inc('elink_count')
        elif pdu.commandId == CommandId.submit_sm:
            self.factory.stats.inc('submit_sm_request_count')

    def claimSeqNum(self):
        seqNum = super().claimSeqNum()
        self.factory.stats.set('last_seqNum_at', datetime.now())
        self.factory.stats.set('last_seqNum', seqNum)
        return seqNum

    def bindSucceeded(self, result, nextState):
        self.factory.stats.set('bound_at', datetime.now())
        self.factory.stats.inc('bound_count')
        return super().bindSucceeded(result, nextState)

    def bindAsReceiver(self):
        """
        Bind as receiver using the msgHandler from the factory.
        """
        return super().bindAsReceiver(self.factory.msgHandler)

    def bindAsTransceiver(self):
        """
        Bind as transceiver using the msgHandler from the factory.
        """
        return super().bindAsTransceiver(self.factory.msgHandler)

    def bindFailed(self, reason):
        """
        Called if binding fails.
        """
        self.log.error("Bind failed [%s]. Disconnecting...", reason)
        self.disconnect()
        if reason.check(SMPPRequestTimoutError):
            raise SMPPSessionInitTimoutError(str(reason))

    def endOutboundTransaction(self, respPDU):
        """
        Close outbound transactions and handle submit_sm responses as expected.
        """
        txn = self.closeOutboundTransaction(respPDU.seqNum)
        if txn is None:
            return

        if isinstance(txn.request, SubmitSM) or respPDU.status == CommandStatus.ESME_ROK:
            # Normal status handling for SubmitSMResp
            if not isinstance(respPDU, txn.request.requireAck):
                txn.ackDeferred.errback(
                    SMPPProtocolError(f"Invalid PDU response type [{type(respPDU)}] for [{type(txn.request)}]")
                )
                return
            txn.ackDeferred.callback(SMPPOutboundTxnResult(self, txn.request, respPDU))
            return

        if isinstance(respPDU, GenericNack):
            txn.ackDeferred.errback(SMPPGenericNackTransactionError(respPDU, txn.request))
            return

        txn.ackDeferred.errback(SMPPTransactionError(respPDU, txn.request))

    def cancelOutboundTransactions(self, err):
        """
        Cancel outbound transactions including long submit_sm transactions.
        """
        super().cancelOutboundTransactions(err)
        self.cancelLongSubmitSmTransactions(err)

    def cancelLongSubmitSmTransactions(self, err):
        """
        Cancel all long submit_sm transactions due to an error.
        """
        for item in list(self.longSubmitSmTxns.values()):
            reqPDU = item['txn'].request
            self.log.exception(err)
            txn = self.closeLongSubmitSmTransaction(reqPDU.LongSubmitSm['msg_ref_num'])
            txn.ackDeferred.errback(err)

    def startLongSubmitSmTransaction(self, reqPDU, timeout):
        """
        Start a long submit_sm transaction for multipart messages.
        """
        msg_ref_num = reqPDU.LongSubmitSm['msg_ref_num']
        if msg_ref_num in self.longSubmitSmTxns:
            self.log.error(
                'Transaction with msg_ref_num [%s] already in progress. Currently open: %s',
                msg_ref_num, len(self.longSubmitSmTxns)
            )
            raise LongSubmitSmTransactionError(
                f'Transaction with msg_ref_num [{msg_ref_num}] already in progress.'
            )

        ackDeferred = defer.Deferred()
        timer = reactor.callLater(timeout, self.onResponseTimeout, reqPDU, timeout)
        self.longSubmitSmTxns[msg_ref_num] = {
            'txn': SMPPOutboundTxn(reqPDU, timer, ackDeferred),
            'nack_count': reqPDU.LongSubmitSm['total_segments']
        }
        self.log.debug("Long submit_sm transaction started with msg_ref_num %s", msg_ref_num)
        return ackDeferred

    def closeLongSubmitSmTransaction(self, msg_ref_num):
        """
        Close a long submit_sm transaction and return its txn.
        """
        self.log.debug("Long submit_sm transaction finished with msg_ref_num %s", msg_ref_num)
        txn = self.longSubmitSmTxns[msg_ref_num]['txn']
        del self.longSubmitSmTxns[msg_ref_num]
        if txn.timer.active():
            txn.timer.cancel()
        return txn

    def endLongSubmitSmTransaction(self, result):
        """
        Handle completion of a long submit_sm transaction after all parts have been acknowledged.
        """
        reqPDU = result.request
        respPDU = result.response
        msg_ref_num = reqPDU.LongSubmitSm['msg_ref_num']

        if msg_ref_num not in self.longSubmitSmTxns:
            self.log.error(
                'Received submit_sm_resp for unknown transaction with msg_ref_num [%s]. Open count: %s',
                msg_ref_num, len(self.longSubmitSmTxns)
            )
            raise LongSubmitSmTransactionError(
                f'Unknown transaction with msg_ref_num [{msg_ref_num}].'
            )

        self.longSubmitSmTxns[msg_ref_num]['nack_count'] -= 1
        self.log.debug(
            "Long submit_sm transaction msg_ref_num %s updated, nack_count: %s",
            msg_ref_num, self.longSubmitSmTxns[msg_ref_num]['nack_count']
        )

        if self.longSubmitSmTxns[msg_ref_num]['nack_count'] == 0:
            txn = self.closeLongSubmitSmTransaction(msg_ref_num)
            txn.ackDeferred.callback(SMPPOutboundTxnResult(self, txn.request, respPDU))

    def endLongSubmitSmTransactionErr(self, failure):
        """
        Handle generic NACK errors or others for long submit_sm transactions.
        """
        try:
            failure.raiseException()
        except SMPPClientConnectionCorruptedError:
            return

    def preSubmitSm(self, pdu):
        """
        Preprocessing before sending a SubmitSM PDU.
        - Convert data_coding from int to DataCoding object if necessary.
        - Apply default source_addr from config if not defined.
        """
        if 'data_coding' in pdu.params and isinstance(pdu.params['data_coding'], int):
            intVal = pdu.params['data_coding']
            if intVal in data_coding_default_value_map:
                name = data_coding_default_value_map[intVal]
                pdu.params['data_coding'] = DataCoding(schemeData=getattr(DataCodingDefault, name))
            else:
                pdu.params['data_coding'] = None

        if pdu.params['source_addr'] is None and self.config().source_addr is not None:
            pdu.params['source_addr'] = self.config().source_addr

    def doSendRequest(self, pdu, timeout):
        """
        Override default to handle long messages.
        If the PDU is a long submit_sm, it will split the message into multiple parts and create a
        LongSubmitSmTransaction.
        """
        if self.connectionCorrupted:
            raise SMPPClientConnectionCorruptedError()
        if not isinstance(pdu, PDURequest) or pdu.requireAck is None:
            raise SMPPClientError(f"Invalid PDU to send: {pdu}")

        if pdu.commandId == CommandId.submit_sm:
            # Check if it is a long message
            UDHI_INDICATOR_SET = False
            if hasattr(pdu.params['esm_class'], 'gsmFeatures'):
                UDHI_INDICATOR_SET = EsmClassGsmFeatures.UDHI_INDICATOR_SET in pdu.params['esm_class'].gsmFeatures

            if (
                    (('sar_msg_ref_num' in pdu.params) or UDHI_INDICATOR_SET) and
                    hasattr(pdu, 'nextPdu')
            ):
                # Long message with multiple PDUs
                partedSmPdu = pdu
                first = True
                while True:
                    partedSmPdu.seqNum = self.claimSeqNum()
                    partedSmPdu.LongSubmitSm = {
                        'msg_ref_num': (partedSmPdu.params['sar_msg_ref_num'] if 'sar_msg_ref_num' in partedSmPdu.params else partedSmPdu.params['short_message'][3]),
                        'total_segments': (partedSmPdu.params['sar_total_segments'] if 'sar_total_segments' in partedSmPdu.params else partedSmPdu.params['short_message'][4]),
                        'segment_seqnum': (partedSmPdu.params['sar_segment_seqnum'] if 'sar_segment_seqnum' in partedSmPdu.params else partedSmPdu.params['short_message'][5])
                    }

                    self.preSubmitSm(partedSmPdu)
                    self.sendPDU(partedSmPdu)
                    self.startOutboundTransaction(partedSmPdu, timeout).addCallbacks(
                        self.endLongSubmitSmTransaction, self.endLongSubmitSmTransactionErr
                    )

                    if first:
                        txn = self.startLongSubmitSmTransaction(partedSmPdu, timeout)
                        first = False

                    try:
                        partedSmPdu = partedSmPdu.nextPdu
                    except AttributeError:
                        break

                return txn
            else:
                self.preSubmitSm(pdu)

        return super().doSendRequest(pdu, timeout)

    def sendDataRequest(self, pdu):
        """
        Workaround for vendor TLVs: remove 'vendor_specific_bypass' before sending.
        """
        if pdu.commandId == CommandId.submit_sm and 'vendor_specific_bypass' in pdu.params:
            del pdu.params['vendor_specific_bypass']

        return super().sendDataRequest(pdu)


class SMPPServerProtocol(twistedSMPPServerProtocol):
    """
    Overridden SMPP server protocol with additional logging, stats, and authentication checks.
    """

    def __init__(self):
        super().__init__()
        self.dataRequestHandler = lambda *args: self.factory.msgHandler(self.system_id, *args)
        self.system_id = None
        self.user = None
        self.bind_type = None
        self.session_id = str(uuid.uuid4())
        self.log = logging.getLogger(LOG_CATEGORY)

    def PDUReceived(self, pdu):
        """
        Handle received PDUs. Logs and processes them as request or response.
        """
        self.log.debug(
            "SMPP Server received PDU from system '%s' [command: %s, seq_number: %s, command_status: %s]",
            self.system_id, pdu.commandId, pdu.seqNum, pdu.status
        )
        self.log.debug("Complete PDU dump: %s", pdu)
        self.factory.stats.set('last_received_pdu_at', datetime.now())

        self.onSMPPOperation()

        if isinstance(pdu, PDURequest):
            self.PDURequestReceived(pdu)
        elif isinstance(pdu, PDUResponse):
            self.PDUResponseReceived(pdu)
        else:
            getattr(self, f"onPDU_{pdu.commandId.name}")(pdu)

    def connectionMade(self):
        super().connectionMade()
        self.factory.stats.inc('connect_count')
        self.factory.stats.inc('connected_count')

    def connectionLost(self, reason):
        super().connectionLost(reason)
        self.factory.stats.inc('disconnect_count')
        self.factory.stats.dec('connected_count')

        if self.sessionState in [SMPPSessionStates.BOUND_RX, SMPPSessionStates.BOUND_TX, SMPPSessionStates.BOUND_TRX]:
            if self.bind_type == CommandId.bind_transceiver:
                self.factory.stats.dec('bound_trx_count')
            elif self.bind_type == CommandId.bind_receiver:
                self.factory.stats.dec('bound_rx_count')
            elif self.bind_type == CommandId.bind_transmitter:
                self.factory.stats.dec('bound_tx_count')

    def onPDURequest_enquire_link(self, reqPDU):
        super().onPDURequest_enquire_link(reqPDU)
        self.factory.stats.set('last_received_elink_at', datetime.now())
        self.factory.stats.inc('elink_count')
        if self.user is not None:
            self.user.getCnxStatus().smpps['elink_count'] += 1

    def doPDURequest(self, reqPDU, handler):
        super().doPDURequest(reqPDU, handler)
        if reqPDU.commandId == CommandId.enquire_link:
            self.factory.stats.set('last_received_elink_at', datetime.now())
        elif reqPDU.commandId == CommandId.submit_sm:
            self.factory.stats.inc('submit_sm_request_count')

    def sendPDU(self, pdu):
        super().sendPDU(pdu)
        self.factory.stats.set('last_sent_pdu_at', datetime.now())

        if pdu.commandId in [CommandId.deliver_sm, CommandId.data_sm]:
            message_content = pdu.params.get('short_message', None)
            if message_content is None:
                message_content = pdu.params.get('message_payload', b'')

            if self.config().log_privacy:
                logged_content = f'** {len(message_content)} byte content **'
            else:
                logged_content = repr(re.sub(rb'[^\x20-\x7E]+', b'.', message_content))

        if pdu.commandId == CommandId.deliver_sm:
            self.factory.stats.inc('deliver_sm_count')
            if self.user is not None:
                self.log.info(
                    "DELIVER_SM [uid:%s] [from:%s] [to:%s] [content:%s]",
                    self.user.uid, pdu.params['source_addr'], pdu.params['destination_addr'], logged_content
                )
                self.user.getCnxStatus().smpps['deliver_sm_count'] += 1
        elif pdu.commandId == CommandId.data_sm:
            self.factory.stats.inc('data_sm_count')
            if self.user is not None:
                self.log.info(
                    "DATA_SM [uid:%s] [from:%s] [to:%s] [content:%s]",
                    self.user.uid, pdu.params['source_addr'], pdu.params['destination_addr'], logged_content
                )
                self.user.getCnxStatus().smpps['data_sm_count'] += 1
        elif pdu.commandId == CommandId.submit_sm_resp:
            if pdu.status == CommandStatus.ESME_RTHROTTLED:
                self.factory.stats.inc('throttling_error_count')
                if self.user is not None:
                    self.user.getCnxStatus().smpps['throttling_error_count'] += 1
            elif pdu.status != CommandStatus.ESME_ROK:
                self.factory.stats.inc('other_submit_error_count')
                if self.user is not None:
                    self.user.getCnxStatus().smpps['other_submit_error_count'] += 1
            else:
                self.factory.stats.inc('submit_sm_count')
                if self.user is not None:
                    self.user.getCnxStatus().smpps['submit_sm_count'] += 1

    def onPDURequest_unbind(self, reqPDU):
        super().onPDURequest_unbind(reqPDU)
        self.factory.stats.inc('unbind_count')
        if self.bind_type == CommandId.bind_transceiver:
            self.factory.stats.dec('bound_trx_count')
        elif self.bind_type == CommandId.bind_receiver:
            self.factory.stats.dec('bound_rx_count')
        elif self.bind_type == CommandId.bind_transmitter:
            self.factory.stats.dec('bound_tx_count')

    def PDUDataRequestReceived(self, reqPDU):
        if self.sessionState == SMPPSessionStates.BOUND_RX and reqPDU.commandId == CommandId.submit_sm:
            errMsg = f'Received submit_sm when BOUND_RX {reqPDU}'
            self.cancelOutboundTransactions(SessionStateError(errMsg, CommandStatus.ESME_RINVBNDSTS))
            return self.fatalErrorOnRequest(reqPDU, errMsg, CommandStatus.ESME_RINVBNDSTS)
        return super().PDUDataRequestReceived(reqPDU)

    def PDURequestReceived(self, reqPDU):
        acceptedPDUs = [
            CommandId.submit_sm, CommandId.bind_transmitter,
            CommandId.bind_receiver, CommandId.bind_transceiver,
            CommandId.unbind, CommandId.unbind_resp,
            CommandId.enquire_link, CommandId.data_sm
        ]
        if reqPDU.commandId not in acceptedPDUs:
            errMsg = f'Received unsupported pdu type: {reqPDU.commandId}'
            self.cancelOutboundTransactions(SessionStateError(errMsg, CommandStatus.ESME_RSYSERR))
            return self.fatalErrorOnRequest(reqPDU, errMsg, CommandStatus.ESME_RSYSERR)

        super().PDURequestReceived(reqPDU)
        if self.user is not None:
            self.user.getCnxStatus().smpps['last_activity_at'] = datetime.now()

    @defer.inlineCallbacks
    def doBindRequest(self, reqPDU, sessionState):
        bind_type = reqPDU.commandId

        if bind_type == CommandId.bind_transceiver:
            self.factory.stats.inc('bind_trx_count')
        elif bind_type == CommandId.bind_receiver:
            self.factory.stats.inc('bind_rx_count')
        elif bind_type == CommandId.bind_transmitter:
            self.factory.stats.inc('bind_tx_count')

        username = reqPDU.params['system_id'].decode()
        password = reqPDU.params['password'].decode()

        try:
            iface, auth_avatar, logout = yield self.factory.login(
                username, password, self.transport.getPeer().host
            )
        except error.UnauthorizedLogin as e:
            self.log.debug('From host %s and using password: %s', self.transport.getPeer().host, password)
            self.log.warning('SMPP Bind request failed for username: "%s", reason: %s', username, str(e))
            self.sendErrorResponse(reqPDU, CommandStatus.ESME_RINVPASWD, username)
            return

        if self.sessionState != SMPPSessionStates.OPEN:
            self.log.warning('Duplicate SMPP bind request from: %s', username)
            self.sendErrorResponse(reqPDU, CommandStatus.ESME_RALYBND, username)
            return

        if not self.factory.canOpenNewConnection(auth_avatar, bind_type):
            self.log.warning('SMPP System %s exceeded max bindings for %s', username, bind_type)
            self.sendErrorResponse(reqPDU, CommandStatus.ESME_RBINDFAIL, username)
            return

        self.user = auth_avatar
        self.system_id = username
        self.sessionState = sessionState
        self.bind_type = bind_type

        self.factory.addBoundConnection(self, self.user)
        bound_cnxns = self.factory.getBoundConnections(self.system_id)
        self.log.debug(
            'Bind request succeeded for %s in session [%s]. %d active binds',
            username, self.session_id, bound_cnxns.getBindingCount() if bound_cnxns else 0
        )
        self.sendResponse(reqPDU, system_id=self.system_id)

        if bind_type == CommandId.bind_transceiver:
            self.factory.stats.inc('bound_trx_count')
        elif bind_type == CommandId.bind_receiver:
            self.factory.stats.inc('bound_rx_count')
        elif bind_type == CommandId.bind_transmitter:
            self.factory.stats.inc('bound_tx_count')

    def sendDataRequest(self, pdu):
        """
        Remove 'vendor_specific_bypass' if present before sending the PDU.
        """
        if pdu.commandId == CommandId.deliver_sm and 'vendor_specific_bypass' in pdu.params:
            del pdu.params['vendor_specific_bypass']
        return super().sendDataRequest(pdu)
