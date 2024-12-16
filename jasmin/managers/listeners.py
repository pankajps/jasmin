# pylint: disable=W0401,W0611
import pickle
import sys
import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

from dateutil import parser
from jasmin.tools import qos
from twisted.internet import defer, reactor
from txamqp.queue import Closed
from smpp.pdu.operations import SubmitSM, DeliverSM
from smpp.pdu.pdu_types import CommandStatus, EsmClassGsmFeatures, DataCodingScheme, DataCodingGsmMsgClass
from smpp.twisted.protocol import DataHandlerResponse
from smpp.pdu.error import SMPPRequestTimoutError

from jasmin.managers.configs import SMPPClientPBConfig
from jasmin.managers.content import SubmitSmRespContent, DeliverSmContent, SubmitSmRespBillContent, DLR
from jasmin.protocols.smpp.error import (
    InterceptorNotSetError, InterceptorNotConnectedError,
    InterceptorRunError, DeliverSmInterceptionError,
    LongSubmitSmTransactionError
)
from jasmin.protocols.smpp.operations import SMPPOperationFactory
from jasmin.routing.Routables import RoutableDeliverSm
from jasmin.routing.jasminApi import Connector


LOG_CATEGORY = "jasmin-sm-listener"


class SMPPClientSMListener:
    """
    Listener object instantiated for every SMPP connection.
    Responsible for handling SubmitSm, DeliverSm and related PDUs for a given SMPP connection.
    """

    def __init__(self, config, SMPPClientFactory, amqpBroker, redisClient, RouterPB=None, interceptorpb_client=None):
        self.config = config
        self.SMPPClientFactory = SMPPClientFactory
        self.SMPPOperationFactory = SMPPOperationFactory(self.SMPPClientFactory.config)
        self.amqpBroker = amqpBroker
        self.redisClient = redisClient
        self.RouterPB = RouterPB
        self.interceptorpb_client = interceptorpb_client

        self.submit_sm_q = None
        self.qos_last_submit_sm_at = None
        self.rejectTimers = {}
        self.submit_retrials = {}
        self.qosTimer = None

        # Set pickleProtocol
        self.pickleProtocol = SMPPClientPBConfig(self.config.config_file).pickle_protocol

        # Set up a dedicated logger
        self.log = logging.getLogger(LOG_CATEGORY)
        if len(self.log.handlers) != 1:
            self.log.setLevel(self.config.log_level)
            if 'stdout' in self.config.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(filename=self.config.log_file,
                                                   when=self.config.log_rotate)
            formatter = logging.Formatter(self.config.log_format, self.config.log_date_format)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

    def setSubmitSmQ(self, queue):
        """
        Set the queue from which SUBMIT_SM messages are consumed.
        """
        self.log.debug(f'Setting a new submit_sm_q: {queue}')
        self.submit_sm_q = queue

    def clearRejectTimer(self, msgid):
        """
        Clear a single message's reject timer if it exists.
        """
        if msgid in self.rejectTimers:
            timer = self.rejectTimers[msgid]
            if timer.active():
                timer.cancel()
            del self.rejectTimers[msgid]

    def clearRejectTimers(self):
        """
        Clear all reject timers for all messages.
        """
        for msgid, timer in list(self.rejectTimers.items()):
            if timer.active():
                timer.cancel()
            del self.rejectTimers[msgid]

    def clearQosTimer(self):
        """
        Clear QoS timer if active.
        """
        if self.qosTimer is not None and not self.qosTimer.called:
            self.qosTimer.cancel()
            self.qosTimer = None

    def clearAllTimers(self):
        """
        Clear all timers (QoS and reject timers).
        """
        self.clearQosTimer()
        self.clearRejectTimers()

    @defer.inlineCallbacks
    def rejectAndRequeueMessage(self, message, delay=True):
        """
        Reject and optionally requeue a message with a delay.
        If delay is True, use the configured requeue_delay.
        If delay is a numeric value, use that as the requeue delay.
        """
        msgid = message.content.properties['message-id']

        if delay:
            requeue_delay = delay if not isinstance(delay, bool) else self.SMPPClientFactory.config.requeue_delay
            self.log.debug(f"Requeuing SubmitSmPDU[{msgid}] in {requeue_delay} seconds")

            # Requeue the message with a delay
            self.clearRejectTimer(msgid)
            timer = reactor.callLater(requeue_delay, self.rejectMessage, message=message, requeue=1)
            self.rejectTimers[msgid] = timer
            defer.returnValue(timer)
        else:
            self.log.debug(f"Requeuing SubmitSmPDU[{msgid}] without delay")
            yield self.rejectMessage(message, requeue=1)

    @defer.inlineCallbacks
    def rejectMessage(self, message, requeue=0):
        """
        Reject (NACK) the message, optionally requeueing it.
        """
        yield self.amqpBroker.chan.basic_reject(delivery_tag=message.delivery_tag, requeue=requeue)

    @defer.inlineCallbacks
    def ackMessage(self, message):
        """
        ACK the message, removing it from the queue.
        """
        yield self.amqpBroker.chan.basic_ack(message.delivery_tag)

    def _isExpired(self, message):
        """
        Check if the message is expired based on 'expiration' header.
        """
        if 'headers' in message.content.properties and 'expiration' in message.content.properties['headers']:
            expiration_datetime = parser.parse(message.content.properties['headers']['expiration'])
            if expiration_datetime < datetime.now():
                return True
        return False

    @defer.inlineCallbacks
    def submit_sm_callback(self, message):
        """
        Callback for SUBMIT_SM messages. Called whenever a SUBMIT_SM is consumed from the queue.
        """
        msgid = None
        try:
            msgid = message.content.properties['message-id']
            SubmitSmPDU = pickle.loads(message.content.body)

            # Re-register callback for next message
            self.submit_sm_q.get().addCallback(self.submit_sm_callback).addErrback(self.submit_sm_errback)

            self.log.debug(f"Callbacked a submit_sm with a SubmitSmPDU[{msgid}]: {SubmitSmPDU}")

            # Track retrials
            self.submit_retrials[msgid] = self.submit_retrials.get(msgid, 0) + 1

            # Handle QoS/throughput
            yield self._handle_throughput()

            # Check message type
            if not isinstance(SubmitSmPDU, SubmitSM):
                self.log.error(
                    f"Received object[{msgid}] is not a SubmitSmPDU: discarding this unknown object from queue")
                yield self.rejectMessage(message)
                defer.returnValue(False)

            # Check expiration
            if self._isExpired(message):
                self.log.info(f"Discarding expired message[{msgid}]")
                yield self.rejectMessage(message)
                defer.returnValue(False)

            # Check SMPP connection and binding states
            if not (yield self._check_smpp_readiness(msgid, message)):
                # _check_smpp_readiness handles requeueing or rejection
                defer.returnValue(False)

            # Send the SMS
            self.log.debug(
                f"Sending SubmitSmPDU[{msgid}] through [cid:{self.SMPPClientFactory.config.id}] "
                f"after {self.submit_retrials[msgid]} retrials.")
            d = self.SMPPClientFactory.smpp.sendDataRequest(SubmitSmPDU)
            d.addCallback(self.submit_sm_resp_event, message)
            yield d
        except SMPPRequestTimoutError:
            self.log.error(
                f"SubmitSmPDU[{msgid}] request timed out through [cid:{self.SMPPClientFactory.config.id}], "
                "message requeued.")
            self.rejectAndRequeueMessage(message)
            defer.returnValue(False)
        except LongSubmitSmTransactionError as e:
            self.log.error(
                f"Long SubmitSmPDU[{msgid}] error in [cid:{self.SMPPClientFactory.config.id}], requeued: {e.message}")
            self.rejectAndRequeueMessage(message)
            defer.returnValue(False)
        except Exception as e:
            self.log.critical(
                f"Rejecting SubmitSmPDU[{msgid}] through [cid:{self.SMPPClientFactory.config.id}] "
                f"for unknown error ({type(e)}): {e}")
            self.rejectMessage(message)
            defer.returnValue(False)

    @defer.inlineCallbacks
    def submit_sm_resp_event(self, r, amqpMessage):
        """
        Handle SubmitSmResp events. A SubmitSmResp is received after a SubmitSm request is processed by the SMSC.
        """
        msgid = amqpMessage.content.properties['message-id']
        total_bill_amount = 0.0
        will_be_retried = False

        try:
            # Extract billing information if any
            submit_sm_resp_bill = None
            if 'submit_sm_bill' in amqpMessage.content.properties['headers']:
                submit_sm_resp_bill = pickle.loads(
                    amqpMessage.content.properties['headers']['submit_sm_bill']).getSubmitSmRespBill()

            if r.response.status == CommandStatus.ESME_ROK:
                # No more retrials
                del self.submit_retrials[msgid]
                # Bill calculation if any
                if submit_sm_resp_bill is not None and submit_sm_resp_bill.getTotalAmounts() > 0:
                    total_bill_amount = submit_sm_resp_bill.getTotalAmounts()

                short_message, _pdu = self._handle_concat_response(r.request, submit_sm_resp_bill, total_bill_amount)

                # Log message content
                logged_content = self._loggable_content(short_message)
                self.log.info(
                    f"SMS-MT [cid:{self.SMPPClientFactory.config.id}] [queue-msgid:{msgid}] "
                    f"[smpp-msgid:{r.response.params['message_id']}] [status:{r.response.status}] "
                    f"[prio:{amqpMessage.content.properties['priority']}] "
                    f"[dlr:{_pdu.params['registered_delivery'].receipt}] "
                    f"[validity:{self._validity_info(amqpMessage)}] [from:{_pdu.params['source_addr']}] "
                    f"[to:{_pdu.params['destination_addr']}] [content:{logged_content}]"
                )
            else:
                # Error handling with retrials
                if r.response.status.name in self.config.submit_error_retrial:
                    retrial = self.config.submit_error_retrial[r.response.status.name]

                    if self.submit_retrials[msgid] < retrial['count']:
                        yield self.rejectAndRequeueMessage(amqpMessage, delay=retrial['delay'])
                        will_be_retried = True
                    else:
                        # No more retrials
                        del self.submit_retrials[msgid]

                # Log message content
                logged_content = self._loggable_content(r.request.params['short_message'])
                self.log.info(
                    f"SMS-MT [cid:{self.SMPPClientFactory.config.id}] [queue-msgid:{msgid}] "
                    f"[status:ERROR/{r.response.status}] [retry:{will_be_retried}] "
                    f"[prio:{amqpMessage.content.properties['priority']}] "
                    f"[dlr:{r.request.params['registered_delivery'].receipt}] "
                    f"[validity:{self._validity_info(amqpMessage)}] [from:{r.request.params['source_addr']}] "
                    f"[to:{r.request.params['destination_addr']}] [content:{logged_content}]"
                )

            if not will_be_retried:
                self.clearRejectTimer(msgid)
                self.log.debug(f"ACKing amqpMessage [{msgid}] with routing_key [{amqpMessage.routing_key}]")
                yield self.ackMessage(amqpMessage)

            # Publish DLR
            dlr_status = r.response.status
            dlr_msgid = r.response.params['message_id'] if dlr_status == CommandStatus.ESME_ROK else None
            dlr = DLR(pdu_type=r.response.id, msgid=msgid, status=dlr_status, smpp_msgid=dlr_msgid)
            yield self.amqpBroker.publish(exchange='messaging', routing_key='dlr.submit_sm_resp', content=dlr)

            # Bill accounting
            if total_bill_amount > 0:
                pubQueueName = f"bill_request.submit_sm_resp.{submit_sm_resp_bill.user.uid}"
                content = SubmitSmRespBillContent(
                    submit_sm_resp_bill.bid,
                    submit_sm_resp_bill.user.uid,
                    total_bill_amount)
                self.log.debug(
                    f"Requesting SubmitSmRespBillContent from bill [bid:{submit_sm_resp_bill.bid}] "
                    f"with routing_key[{pubQueueName}]: {total_bill_amount}")
                yield self.amqpBroker.publish(exchange='billing', routing_key=pubQueueName, content=content)

            # Publish submit_sm_resp if configured
            if self.config.publish_submit_sm_resp:
                content = SubmitSmRespContent(r.response, msgid, pickleProtocol=self.pickleProtocol)
                self.log.debug(
                    f"Sending back SubmitSmRespContent[{msgid}] with routing_key[{amqpMessage.content.properties['reply-to']}]"
                )
                yield self.amqpBroker.publish(
                    exchange='messaging',
                    routing_key=amqpMessage.content.properties['reply-to'],
                    content=content)

        except Exception as e:
            self.log.error(f'({type(e)}) while handling submit_sm_resp for msgid:{msgid}: {e}')
        else:
            if will_be_retried:
                defer.returnValue(False)

    def submit_sm_errback(self, error):
        """
        Errback for submit_sm_callback. Usually triggered when closing a queue
        or when some unexpected error occurs.
        """
        if error.check(Closed) is None:
            try:
                error.raiseException()
            except Exception as e:
                self.log.error(f"Error in submit_sm_errback ({type(e)}): {e}")

    @defer.inlineCallbacks
    def concatDeliverSMs(self, HSetReturn, hashKey, splitMethod, total_segments, msg_ref_num, segment_seqnum):
        """
        Concatenate long DeliverSM messages once all parts are received.
        """
        if HSetReturn == 0:
            self.log.warning(f'This hashKey {hashKey} already exists, will not reset it !')
            return

        # Set expiry for the key
        yield self.redisClient.expire(hashKey, 300)

        # If this is the last part, proceed to concatenation
        if segment_seqnum == total_segments:
            hvals = yield self.redisClient.hvals(hashKey)
            if len(hvals) != total_segments:
                self.log.warning(
                    f'Received last part (msg_ref_num:{msg_ref_num}) but not all parts found, data lost !')
                return

            pdus = {}
            for pickledValue in hvals:
                value = pickle.loads(pickledValue)
                pdus[value['segment_seqnum']] = value['pdu']

            # Concatenate message parts
            final_pdu = self._concat_message_parts(pdus, splitMethod, total_segments)

            # Route the final pdu
            routable = RoutableDeliverSm(final_pdu, Connector(self.SMPPClientFactory.config.id))
            yield self.deliver_sm_event_post_interception(routable=routable, smpp=None, concatenated=True)

    def code_dlr_msgid(self, pdu):
        """
        Code the DLR message id according to smppc's dlr_msg_id_bases configuration.
        """
        dlr_id = pdu.dlr['id']
        if isinstance(dlr_id, bytes):
            dlr_id = dlr_id.decode()

        try:
            if self.SMPPClientFactory.config.dlr_msg_id_bases == 1:
                ret = ('%x' % int(dlr_id)).upper().lstrip('0')
            elif self.SMPPClientFactory.config.dlr_msg_id_bases == 2:
                ret = int(str(dlr_id), 16)
            else:
                ret = str(dlr_id).upper().lstrip('0')
        except Exception as e:
            self.log.error(
                f'code_dlr_msgid, cannot code msgid [{dlr_id}] with dlr_msg_id_bases:{self.SMPPClientFactory.config.dlr_msg_id_bases}')
            self.log.error(f'code_dlr_msgid, error details: {e}')
            ret = str(dlr_id).upper().lstrip('0')

        self.log.debug(f'code_dlr_msgid: {dlr_id} coded to {ret}')
        return ret

    def deliver_sm_event_interceptor(self, smpp, pdu):
        """
        Intercept deliver_sm event for MO messages and possibly run interception scripts.
        """
        self.log.debug(f'Intercepting deliver_sm event in smppc {self.SMPPClientFactory.config.id}')

        if self.RouterPB is None:
            self.log.error(
                f'(deliver_sm_event_interceptor/{self.SMPPClientFactory.config.id}) RouterPB not set: cannot route')
            return

        routable = RoutableDeliverSm(pdu, Connector(self.SMPPClientFactory.config.id))

        # Check if there's an interceptor
        interceptor = self.RouterPB.getMOInterceptionTable().getInterceptorFor(routable)
        if interceptor is not None:
            self.log.debug(f"RouterPB selected {interceptor} interceptor for this DeliverSmPDU")
            if self.interceptorpb_client is None:
                smpp.factory.stats.inc('interceptor_error_count')
                self.log.error("InterceptorPB not set !")
                raise InterceptorNotSetError('InterceptorPB not set !')
            if not self.interceptorpb_client.isConnected:
                smpp.factory.stats.inc('interceptor_error_count')
                self.log.error("InterceptorPB not connected !")
                raise InterceptorNotConnectedError('InterceptorPB not connected !')

            script = interceptor.getScript()
            self.log.debug(f"Interceptor script loaded: {script}")
            d = self.interceptorpb_client.run_script(script, routable)
            d.addCallback(self.deliver_sm_event_post_interception, routable=routable, smpp=smpp)
            d.addErrback(self.deliver_sm_event_post_interception)
            return d
        else:
            return self.deliver_sm_event_post_interception(routable=routable, smpp=smpp)

    @defer.inlineCallbacks
    def deliver_sm_event_post_interception(self, *args, **kw):
        """
        Process the DeliverSm event after interception (if any).
        This handles MO messages and DLR messages, routing them to the appropriate queues and handlers.
        """
        try:
            if 'smpp' not in kw or 'routable' not in kw:
                self.log.error(
                    f'deliver_sm_event_post_interception missing arguments: {kw}')
                raise InterceptorRunError('deliver_sm_event_post_interception missing arguments')

            smpp = kw['smpp']
            routable = kw['routable']
            concatenated = kw.get('concatenated', False)

            message_content = self._extract_message_content(routable.pdu)

            # Handle interception results
            if len(args) == 1:
                # Check returned value from interceptor
                if isinstance(args[0], bool) and not args[0]:
                    smpp.factory.stats.inc('interceptor_error_count')
                    self.log.error('Failed running interception script, got a False return.')
                    raise InterceptorRunError('Interception script returned False')
                elif isinstance(args[0], dict) and args[0]['smpp_status'] > 0:
                    smpp.factory.stats.inc('interceptor_error_count')
                    self.log.info(f'Interceptor script returned smpp_status error {args[0]["smpp_status"]}')
                    raise DeliverSmInterceptionError(code=args[0]['smpp_status'])
                elif isinstance(args[0], (str, bytes)):
                    smpp.factory.stats.inc('interceptor_count')
                    routable = pickle.loads(args[0])
                else:
                    smpp.factory.stats.inc('interceptor_error_count')
                    self.log.error(f'Failed running interception script, got return: {args[0]}')
                    raise InterceptorRunError('Interception script returned invalid data')

            self.log.debug(f'Handling deliver_sm_event_post_interception event for smppc: {self.SMPPClientFactory.config.id}')

            routable.pdu.dlr = self.SMPPOperationFactory.isDeliveryReceipt(routable.pdu)
            content = DeliverSmContent(routable,
                                       self.SMPPClientFactory.config.id,
                                       pickleProtocol=self.pickleProtocol,
                                       concatenated=concatenated)
            msgid = content.properties['message-id']

            if routable.pdu.dlr is None:
                # MO message
                yield self._handle_mo_message(routable, msgid, content, message_content)
            else:
                # DLR message
                yield self.amqpBroker.publish(
                    exchange='messaging',
                    routing_key='dlr.deliver_sm',
                    content=DLR(
                        pdu_type=routable.pdu.id,
                        msgid=self.code_dlr_msgid(routable.pdu),
                        status=routable.pdu.dlr['stat'],
                        cid=self.SMPPClientFactory.config.id,
                        dlr_details=routable.pdu.dlr
                    ))
        except (InterceptorRunError, DeliverSmInterceptionError) as e:
            logged_content = self._loggable_content(message_content)
            self.log.info(
                f"SMS-MO [cid:{self.SMPPClientFactory.config.id}] [i-status:{e.status}] "
                f"[from:{routable.pdu.params['source_addr']}] [to:{routable.pdu.params['destination_addr']}] "
                f"[content:{logged_content}]")
            defer.returnValue(DataHandlerResponse(status=e.status))
        except Exception as e:
            self.log.critical(f'Unknown exception ({type(e)}): {e}')
            defer.returnValue(DataHandlerResponse(status=CommandStatus.ESME_RUNKNOWNERR))

    ####################
    # Helper Methods   #
    ####################

    def _extract_message_content(self, pdu):
        """Extract message content from a pdu, checking both short_message and message_payload."""
        if 'short_message' in pdu.params and len(pdu.params['short_message']) > 0:
            return pdu.params['short_message']
        elif 'message_payload' in pdu.params:
            return pdu.params['message_payload']
        elif 'short_message' in pdu.params:
            return pdu.params['short_message']
        return None

    def _validity_info(self, amqpMessage):
        """Get validity information from the message properties if present."""
        if 'headers' not in amqpMessage.content.properties or 'expiration' not in amqpMessage.content.properties['headers']:
            return 'none'
        return amqpMessage.content.properties['headers']['expiration']

    def _loggable_content(self, message_content):
        """Return content safe for logging considering privacy config."""
        if self.config.log_privacy:
            return f'** {len(message_content)} byte content **' if message_content else '** 0 byte content **'
        else:
            return repr(message_content)

    @defer.inlineCallbacks
    def _handle_throughput(self):
        """
        Handle QoS throughput: if submit_sm_throughput > 0, ensure messages are not sent too fast.
        """
        if self.SMPPClientFactory.config.submit_sm_throughput > 0:
            if self.qos_last_submit_sm_at is None:
                self.qos_last_submit_sm_at = datetime(1970, 1, 1)

            qos_throughput_second = 1 / float(self.SMPPClientFactory.config.submit_sm_throughput)
            qos_throughput_ysecond_td = timedelta(microseconds=qos_throughput_second * 1_000_000)
            qos_delay = datetime.now() - self.qos_last_submit_sm_at
            if qos_delay < qos_throughput_ysecond_td:
                qos_slow_down = float((qos_throughput_ysecond_td - qos_delay).microseconds) / 1_000_000
                self.log.debug(
                    f"QoS: submit_sm_callback faster ({qos_delay}) than throughput ({qos_throughput_ysecond_td}), "
                    f"slowing down {qos_slow_down}s (requeuing)."
                )
                yield qos.slow_down(qos_slow_down)

            self.qos_last_submit_sm_at = datetime.now()

    @defer.inlineCallbacks
    def _check_smpp_readiness(self, msgid, message):
        """
        Check if SMPP client is connected and bound. If not ready, handle retrials or discarding.
        """
        if self.SMPPClientFactory.smpp is None or not self.SMPPClientFactory.smpp.isBound():
            created_at = parser.parse(message.content.properties['headers']['created_at'])
            msgAge = datetime.now() - created_at

            not_connected = (self.SMPPClientFactory.smpp is None)
            reason = 'not connected' if not_connected else 'not bound'

            if msgAge.seconds > self.config.submit_max_age_smppc_not_ready:
                self.log.error(
                    f"SMPPC [cid:{self.SMPPClientFactory.config.id}] is {reason}: "
                    f"Discarding #{self.submit_retrials[msgid]} SubmitSmPDU[{msgid}], over-aged {msgAge.seconds} seconds."
                )
                yield self.rejectMessage(message)
                defer.returnValue(False)
            else:
                delay_str = ''
                if self.config.submit_retrial_delay_smppc_not_ready:
                    delay_str = f' with delay {self.config.submit_retrial_delay_smppc_not_ready} seconds'

                self.log.error(
                    f"SMPPC [cid:{self.SMPPClientFactory.config.id}] is {reason}: "
                    f"Requeuing #{self.submit_retrials[msgid]} SubmitSmPDU[{msgid}]{delay_str}, aged {msgAge.seconds} seconds."
                )
                yield self.rejectAndRequeueMessage(
                    message, delay=self.config.submit_retrial_delay_smppc_not_ready
                )
                defer.returnValue(False)

        defer.returnValue(True)

    def _handle_concat_response(self, request_pdu, submit_sm_resp_bill, total_bill_amount):
        """
        Handle message concatenation for SubmitSmResp. Calculate total bill for each part if needed.
        """
        _pdu = request_pdu
        UDHI_INDICATOR_SET = False
        if 'esm_class' in _pdu.params and hasattr(_pdu.params['esm_class'], 'gsmFeatures'):
            for gsmFeature in _pdu.params['esm_class'].gsmFeatures:
                if gsmFeature == EsmClassGsmFeatures.UDHI_INDICATOR_SET:
                    UDHI_INDICATOR_SET = True
                    break

        splitMethod = None
        if 'sar_msg_ref_num' in _pdu.params:
            # SAR splitting
            splitMethod = 'sar'
            short_message = _pdu.params['short_message']
        elif UDHI_INDICATOR_SET and _pdu.params['short_message'][:3] == b'\x05\x00\x03':
            # UDH splitting
            splitMethod = 'udh'
            short_message = _pdu.params['short_message'][6:]
        else:
            # No splitting
            return _pdu.params['short_message'], _pdu

        # Concatenate all parts
        while hasattr(_pdu, 'nextPdu'):
            _pdu = _pdu.nextPdu
            if splitMethod == 'sar':
                short_message += _pdu.params['short_message']
            else:
                short_message += _pdu.params['short_message'][6:]

            # Increase bill amount for each part
            if submit_sm_resp_bill is not None and submit_sm_resp_bill.getTotalAmounts() > 0:
                total_bill_amount += submit_sm_resp_bill.getTotalAmounts()

        return short_message, request_pdu

    def _concat_message_parts(self, pdus, splitMethod, total_segments):
        """
        Concatenate message parts for DeliverSm PDUs and return the final PDU.
        """
        # Identify msg_content field
        first_pdu = pdus[1]
        if 'short_message' in first_pdu.params:
            msg_content_key = 'short_message'
        elif 'message_payload' in first_pdu.params:
            msg_content_key = 'message_payload'
        else:
            self.log.warning(f'Cannot find message content in first pdu params: {first_pdu.params}')
            return first_pdu

        concat_message_content = b''
        for i in range(total_segments):
            pdu = pdus[i + 1]
            if splitMethod == 'sar':
                concat_message_content += pdu.params[msg_content_key]
            else:
                concat_message_content += pdu.params[msg_content_key][6:]

        # Remove splitting info and set the final content
        if splitMethod == 'sar':
            del first_pdu.params['sar_segment_seqnum']
            del first_pdu.params['sar_total_segments']
            del first_pdu.params['sar_msg_ref_num']
        else:
            first_pdu.params['esm_class'] = None

        first_pdu.params[msg_content_key] = concat_message_content
        return first_pdu

    @defer.inlineCallbacks
    def _handle_mo_message(self, routable, msgid, content, message_content):
        """
        Handle a MO message (non-DLR) by publishing it to the correct queue and logging it.
        """
        # Check if UDH is set
        UDHI_INDICATOR_SET = False
        if 'esm_class' in routable.pdu.params and hasattr(routable.pdu.params['esm_class'], 'gsmFeatures'):
            for gsmFeature in routable.pdu.params['esm_class'].gsmFeatures:
                if gsmFeature == EsmClassGsmFeatures.UDHI_INDICATOR_SET:
                    UDHI_INDICATOR_SET = True
                    break

        not_class2 = True
        if 'data_coding' in routable.pdu.params:
            dcs = routable.pdu.params['data_coding']
            if dcs.scheme == DataCodingScheme.GSM_MESSAGE_CLASS and dcs.schemeData is not None:
                not_class2 = (dcs.schemeData.msgClass != DataCodingGsmMsgClass.CLASS_2)

        # Identify splitting method if any
        splitMethod = None
        if 'sar_msg_ref_num' in routable.pdu.params:
            splitMethod = 'sar'
        elif UDHI_INDICATOR_SET and not_class2 and message_content and message_content[:3] == b'\x05\x00\x03':
            splitMethod = 'udh'

        if splitMethod is None:
            # Simple MO message
            routing_key = f'deliver.sm.{self.SMPPClientFactory.config.id}'
            self.log.debug(f"Publishing DeliverSmContent[{msgid}] with routing_key[{routing_key}]")
            yield self.amqpBroker.publish(exchange='messaging', routing_key=routing_key, content=content)

            # Log the message
            logged_content = self._loggable_content(message_content)
            self.log.info(
                f"SMS-MO [cid:{self.SMPPClientFactory.config.id}] [queue-msgid:{msgid}] [status:{routable.pdu.status}] "
                f"[prio:{routable.pdu.params.get('priority_flag')}] [validity:{routable.pdu.params.get('validity_period')}] "
                f"[from:{routable.pdu.params['source_addr']}] [to:{routable.pdu.params['destination_addr']}] [content:{logged_content}]"
            )
        else:
            # Part of a long message
            if self.redisClient is None:
                self.log.critical(
                    f'Invalid RC found while receiving part of long DeliverSm [queue-msgid:{msgid}], MSG LOST !'
                )
            else:
                # Store parts in Redis for later concatenation
                total_segments, segment_seqnum, msg_ref_num = self._extract_long_msg_info(routable.pdu, splitMethod, message_content)
                hashKey = f"longDeliverSm:{self.SMPPClientFactory.config.id}:{msg_ref_num}:{routable.pdu.params['destination_addr']}"
                hashValues = {
                    'pdu': routable.pdu,
                    'total_segments': total_segments,
                    'msg_ref_num': msg_ref_num,
                    'segment_seqnum': segment_seqnum
                }
                yield self.redisClient.hset(hashKey, segment_seqnum, pickle.dumps(hashValues, self.pickleProtocol)).addCallback(
                    self.concatDeliverSMs,
                    hashKey,
                    splitMethod,
                    total_segments,
                    msg_ref_num,
                    segment_seqnum)

                self.log.info(
                    f"DeliverSmContent[{msgid}] is part of a long msg ({total_segments} parts), "
                    "will be enqueued after concatenation."
                )

                # Mark message as will_be_concatenated
                routing_key = f'deliver.sm.{self.SMPPClientFactory.config.id}'
                self.log.debug(
                    f"Publishing DeliverSmContent[{msgid}](flagged:wbc) with routing_key[{routing_key}]")
                content.properties['headers']['will_be_concatenated'] = True
                yield self.amqpBroker.publish(exchange='messaging', routing_key=routing_key, content=content)

    def _extract_long_msg_info(self, pdu, splitMethod, message_content):
        """Extract total segments, segment_seqnum, and msg_ref_num for long messages."""
        if splitMethod == 'sar':
            total_segments = pdu.params['sar_total_segments']
            segment_seqnum = pdu.params['sar_segment_seqnum']
            msg_ref_num = pdu.params['sar_msg_ref_num']
        else:
            # UDH method
            total_segments = message_content[4]
            segment_seqnum = message_content[5]
            msg_ref_num = message_content[3]
        return total_segments, segment_seqnum, msg_ref_num
