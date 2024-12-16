import sys
import logging
from logging.handlers import TimedRotatingFileHandler

from twisted.internet import defer, reactor
from txamqp.queue import Closed
from txredisapi import ConnectionError
from smpp.pdu.pdu_types import RegisteredDeliveryReceipt

from jasmin.managers.content import DLRContentForHttpapi, DLRContentForSmpps
from jasmin.tools.singleton import Singleton
from jasmin.tools import to_enum

LOG_CATEGORY = "dlr"


class RedisError(Exception):
    """Raised for any Redis connectivity problem"""


class DLRMapError(Exception):
    """Raised when receiving an invalid DLR content from Redis"""


class DLRMapNotFound(Exception):
    """Raised if no dlr is found in Redis db"""


class DLRLookup:
    """
    Will consume dlr pdus (submit_sm, deliver_sm or data_sm), lookup for matching dlr maps in redis db
    and publish dlr for later throwing (http or smpp)
    """

    def __init__(self, config, amqpBroker, redisClient):
        self.pid = config.pid
        self.q = None
        self.config = config
        self.amqpBroker = amqpBroker
        self.redisClient = redisClient
        self.requeue_timers = {}
        self.lookup_retrials = {}

        # Set up a dedicated logger
        self.log = logging.getLogger(LOG_CATEGORY)
        if len(self.log.handlers) != 1:
            self.log.setLevel(self.config.log_level)
            if 'stdout' in self.config.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(
                    filename=self.config.log_file,
                    when=self.config.log_rotate
                )
            formatter = logging.Formatter(self.config.log_format, self.config.log_date_format)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

        self.log.info('Started %s #%s.', self.__class__.__name__, self.pid)

    @defer.inlineCallbacks
    def subscribe(self):
        """Subscribe to dlr.* queues"""
        consumerTag = f'DLRLookup-{self.pid}'
        queueName = f'DLRLookup-{self.pid}'
        routing_key = 'dlr.*'
        yield self.amqpBroker.chan.exchange_declare(exchange='messaging', type='topic')
        yield self.amqpBroker.named_queue_declare(queue=queueName)
        yield self.amqpBroker.chan.queue_bind(queue=queueName, exchange="messaging", routing_key=routing_key)
        yield self.amqpBroker.chan.basic_consume(queue=queueName, no_ack=False, consumer_tag=consumerTag)
        self.amqpBroker.client.queue(consumerTag).addCallback(self.setup_callbacks)

    def setup_callbacks(self, q):
        if self.q is None:
            self.q = q
            self.log.info('DLRLookup (%s) is ready.', self.pid)

        q.get().addCallback(self.dlr_callback_dispatcher).addErrback(self.dlr_errback)

    def dlr_errback(self, error):
        """Handle errors from dlr_callback_dispatcher."""
        if error.check(Closed) is None:
            # Unknown error
            self.log.error("Error in dlr_callback_dispatcher: %s", error)

    @staticmethod
    def _decode_if_bytes(value):
        """Decode value if it's bytes, else return as is."""
        return value.decode() if isinstance(value, bytes) else value

    def _increment_retrial(self, msgid):
        """Increment retry count for a given msgid."""
        self.lookup_retrials[msgid] = self.lookup_retrials.get(msgid, 0) + 1

    def _can_retry(self, msgid):
        """Check if we can still retry for a given msgid."""
        return self.lookup_retrials.get(msgid, 0) < self.config.dlr_lookup_max_retries

    @defer.inlineCallbacks
    def dlr_callback_dispatcher(self, message):
        # Re-register callback for next message
        self.setup_callbacks(self.q)

        msgid = message.content.properties['message-id']
        self._increment_retrial(msgid)

        if message.routing_key == 'dlr.submit_sm_resp':
            yield self.submit_sm_resp_dlr_callback(message)
        elif message.routing_key == 'dlr.deliver_sm':
            yield self.deliver_sm_dlr_callback(message)
        else:
            self.log.error('Unknown routing_key in dlr_callback_dispatcher: %s', message.routing_key)
            yield self.rejectMessage(message)

    @defer.inlineCallbacks
    def rejectAndRequeueMessage(self, message, delay=True):
        msgid = message.content.properties['message-id']
        if delay:
            self.log.debug("Requeuing Content[%s] with delay: %s seconds",
                           msgid, self.config.dlr_lookup_retry_delay)

            if msgid in self.requeue_timers:
                timer = self.requeue_timers[msgid]
                if timer.active():
                    timer.cancel()
                del self.requeue_timers[msgid]

            timer = reactor.callLater(
                self.config.dlr_lookup_retry_delay,
                self.rejectMessage,
                message=message,
                requeue=1
            )
            self.requeue_timers[msgid] = timer
            defer.returnValue(timer)
        else:
            self.log.debug("Requeuing Content[%s] without delay", msgid)
            yield self.rejectMessage(message, requeue=1)

    @defer.inlineCallbacks
    def rejectMessage(self, message, requeue=0):
        msgid = message.content.properties['message-id']
        if requeue == 0 and msgid in self.lookup_retrials:
            del self.lookup_retrials[msgid]

        yield self.amqpBroker.chan.basic_reject(delivery_tag=message.delivery_tag, requeue=requeue)

    @defer.inlineCallbacks
    def ackMessage(self, message):
        msgid = message.content.properties['message-id']
        if msgid in self.lookup_retrials:
            del self.lookup_retrials[msgid]

        yield self.amqpBroker.chan.basic_ack(message.delivery_tag)

    def _validate_redis_client(self):
        if self.redisClient is None:
            raise RedisError('RC undefined !')

    def _validate_dlr_map(self, dlr, msgid):
        if not dlr:
            raise DLRMapNotFound(f'No dlr map for msgid[{msgid}]')
        if 'sc' not in dlr or dlr['sc'] not in ['httpapi', 'smppsapi']:
            raise DLRMapError(f'Fetched unknown dlr: {dlr}')

    @defer.inlineCallbacks
    def _handle_redis_error_with_retry(self, message, e, error_type):
        msgid = message.content.properties['message-id']
        retry_count = self.lookup_retrials.get(msgid, 1)
        max_retries = self.config.dlr_lookup_max_retries

        if self._can_retry(msgid):
            self.log.error('[msgid:%s] (retrials: %s/%s) %s: %s',
                           msgid, retry_count, max_retries, error_type, e)
            yield self.rejectAndRequeueMessage(message)
        else:
            self.log.error('[msgid:%s] (final) %s: %s', msgid, error_type, e)
            yield self.rejectMessage(message)

    @defer.inlineCallbacks
    def submit_sm_resp_dlr_callback(self, message):
        msgid = message.content.properties['message-id']
        dlr_status = self._decode_if_bytes(message.content.body)

        try:
            self._validate_redis_client()

            dlr = yield self.redisClient.hgetall("dlr:%s" % msgid)
            self._validate_dlr_map(dlr, msgid)

            if dlr['sc'] == 'httpapi':
                yield self._handle_httpapi_submit_sm_resp(dlr, dlr_status, message)
            elif dlr['sc'] == 'smppsapi':
                yield self._handle_smppsapi_submit_sm_resp(dlr, dlr_status, message)

        except DLRMapError as e:
            self.log.error('[msgid:%s] DLR Content: %s', msgid, e)
            yield self.rejectMessage(message)
        except (RedisError, ConnectionError) as e:
            yield self._handle_redis_error_with_retry(message, e, "RedisError")
        except DLRMapNotFound as e:
            # DLR not found, no retries needed for submit_sm_resp case
            self.log.debug('[msgid:%s] DLRMapNotFound: %s', msgid, e)
            yield self.rejectMessage(message)
        except Exception as e:
            self.log.error('[msgid:%s] Unknown error (%s): %s', msgid, type(e), e)
            yield self.rejectMessage(message)
        else:
            yield self.ackMessage(message)

    @defer.inlineCallbacks
    def _handle_httpapi_submit_sm_resp(self, dlr, dlr_status, message):
        msgid = message.content.properties['message-id']
        dlr_url = dlr['url']
        dlr_level = dlr['level']
        dlr_method = dlr['method']
        dlr_expiry = dlr['expiry']
        dlr_connector = dlr.get('connector', 'unknown')

        # If intermediate receipt is requested (level 1 or 3)
        if dlr_level in [1, 3]:
            self.log.debug('Got DLR info for msgid[%s], url:%s, level:%s, connector:%s',
                           msgid, dlr_url, dlr_level, dlr_connector)

            # Publish intermediate DLR (level 1)
            yield self.amqpBroker.publish(
                exchange='messaging',
                routing_key='dlr_thrower.http',
                content=DLRContentForHttpapi(
                    dlr_status, msgid, dlr_url, dlr_level=1,
                    dlr_connector=dlr_connector, method=dlr_method
                )
            )

            # Remove DLR if level 1 or error in submit_sm_resp
            if dlr_level == 1 or dlr_status != 'ESME_ROK':
                self.log.debug('Removing DLR request for msgid[%s]', msgid)
                yield self.redisClient.delete("dlr:%s" % msgid)

        # If final receipt (level 2 or 3) and success, map smpp_msgid
        if dlr_level in [2, 3] and dlr_status == 'ESME_ROK':
            smpp_msgid = message.content.properties['headers']['smpp_msgid']
            self.log.debug('Mapping smpp msgid: %s to queue msgid: %s, expiring in %s',
                           smpp_msgid, msgid, dlr_expiry)
            hashKey = "queue-msgid:%s" % smpp_msgid
            yield self.redisClient.hmset(hashKey, {'msgid': msgid, 'connector_type': 'httpapi'})
            yield self.redisClient.expire(hashKey, dlr_expiry)

    @defer.inlineCallbacks
    def _handle_smppsapi_submit_sm_resp(self, dlr, dlr_status, message):
        msgid = message.content.properties['message-id']
        headers = message.content.properties.get('headers', {})
        system_id = dlr['system_id']
        source_addr_ton = to_enum(dlr['source_addr_ton'])
        source_addr_npi = to_enum(dlr['source_addr_npi'])
        source_addr = str(dlr['source_addr'])
        dest_addr_ton = to_enum(dlr['dest_addr_ton'])
        dest_addr_npi = to_enum(dlr['dest_addr_npi'])
        destination_addr = str(dlr['destination_addr'])
        sub_date = dlr['sub_date']
        registered_delivery_receipt = to_enum(dlr['rd_receipt'])
        smpps_map_expiry = dlr['expiry']

        need_receipt = (
                (dlr_status != 'ESME_ROK' and registered_delivery_receipt == RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED_FOR_FAILURE) or
                (dlr_status == 'ESME_ROK' and registered_delivery_receipt in [
                    RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED,
                    RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED_FOR_FAILURE
                ])
        )

        if need_receipt and (dlr_status != 'ESME_ROK' or (dlr_status == 'ESME_ROK' and self.config.smpp_receipt_on_success_submit_sm_resp)):
            # Publish intermediate SMPP DLR
            self.log.debug("Publishing DLRContentForSmpps[%s] with routing_key[%s]",
                           msgid, 'dlr_thrower.smpps')
            yield self.amqpBroker.publish(
                exchange='messaging',
                routing_key='dlr_thrower.smpps',
                content=DLRContentForSmpps(
                    dlr_status, msgid, system_id, source_addr, destination_addr, sub_date,
                    source_addr_ton, source_addr_npi, dest_addr_ton, dest_addr_npi
                )
            )

        if dlr_status == 'ESME_ROK':
            smpp_msgid = headers.get('smpp_msgid')
            self.log.debug('Mapping smpp msgid: %s to queue msgid: %s, expiring in %s',
                           smpp_msgid, msgid, smpps_map_expiry)
            hashKey = "queue-msgid:%s" % smpp_msgid
            yield self.redisClient.hmset(hashKey, {'msgid': msgid, 'connector_type': 'smppsapi'})
            yield self.redisClient.expire(hashKey, smpps_map_expiry)

    @defer.inlineCallbacks
    def deliver_sm_dlr_callback(self, message):
        msgid = message.content.properties['message-id']
        headers = message.content.properties.get('headers', {})
        pdu_cid = headers.get('cid')
        pdu_dlr_id = headers.get('dlr_id')
        pdu_dlr_ddate = headers.get('dlr_ddate')
        pdu_dlr_sdate = headers.get('dlr_sdate')
        pdu_dlr_sub = headers.get('dlr_sub')
        pdu_dlr_err = headers.get('dlr_err')
        pdu_dlr_text = headers.get('dlr_text')
        pdu_dlr_dlvrd = headers.get('dlr_dlvrd')

        pdu_dlr_status = self._decode_if_bytes(message.content.body)

        try:
            self._validate_redis_client()

            q = yield self.redisClient.hgetall("queue-msgid:%s" % msgid)
            if len(q) != 2 or 'msgid' not in q or 'connector_type' not in q:
                raise DLRMapNotFound('Got a DLR for an unknown message id: %s (coded:%s)' % (pdu_dlr_id, msgid))

            submit_sm_queue_id = q['msgid']
            connector_type = q['connector_type']

            dlr = yield self.redisClient.hgetall("dlr:%s" % submit_sm_queue_id)
            if not dlr:
                raise DLRMapNotFound('Got a DLR for an unknown message id: %s (coded:%s)' % (pdu_dlr_id, msgid))
            if dlr['sc'] != connector_type:
                raise DLRMapError('Found a dlr for msgid:%s with different sc: %s' % (submit_sm_queue_id, dlr['sc']))

            success_states = ['ACCEPTD', 'DELIVRD']
            final_states = ['DELIVRD', 'EXPIRED', 'DELETED', 'UNDELIV', 'REJECTD']

            if connector_type == 'httpapi':
                yield self._handle_httpapi_deliver_sm(
                    dlr, submit_sm_queue_id, pdu_dlr_status, pdu_dlr_id, msgid,
                    pdu_dlr_sub, pdu_dlr_dlvrd, pdu_dlr_sdate, pdu_dlr_ddate, pdu_dlr_err,
                    pdu_dlr_text, final_states
                )
            elif connector_type == 'smppsapi':
                yield self._handle_smppsapi_deliver_sm(
                    dlr, submit_sm_queue_id, pdu_dlr_status, success_states, final_states,
                    pdu_dlr_err, pdu_dlr_id, msgid, pdu_dlr_sdate, pdu_dlr_ddate,
                    pdu_dlr_sub, pdu_dlr_dlvrd, pdu_dlr_text
                )

        except DLRMapError as e:
            self.log.error('[msgid:%s] DLRMapError: %s', msgid, e)
            yield self.rejectMessage(message)
        except (RedisError, ConnectionError) as e:
            yield self._handle_redis_error_with_retry(message, e, "RedisError")
        except DLRMapNotFound as e:
            yield self._handle_redis_error_with_retry(message, e, "DLRMapNotFound")
        except Exception as e:
            self.log.error('[msgid:%s] Unknown error (%s): %s', msgid, type(e), e)
            yield self.rejectMessage(message)
        else:
            yield self.ackMessage(message)

            # Do not log text for privacy reasons
            if self.config.log_privacy:
                logged_content = '** %s byte content **' % len(pdu_dlr_text)
            else:
                logged_content = '%r' % pdu_dlr_text

            self.log.info(
                "DLR [cid:%s] [smpp-msgid:%s] [status:%s] [submit date:%s] [done date:%s] "
                "[sub/dlvrd messages:%s/%s] [err:%s] [content:%s]",
                pdu_cid,
                msgid,
                pdu_dlr_status,
                pdu_dlr_sdate,
                pdu_dlr_ddate,
                pdu_dlr_sub,
                pdu_dlr_dlvrd,
                pdu_dlr_err,
                logged_content
            )

    @defer.inlineCallbacks
    def _handle_httpapi_deliver_sm(
            self, dlr, submit_sm_queue_id, pdu_dlr_status, pdu_dlr_id, msgid,
            pdu_dlr_sub, pdu_dlr_dlvrd, pdu_dlr_sdate, pdu_dlr_ddate, pdu_dlr_err,
            pdu_dlr_text, final_states
    ):
        dlr_url = dlr['url']
        dlr_level = dlr['level']
        dlr_method = dlr['method']

        if dlr_level in [2, 3]:
            self.log.debug('Got DLR info for msgid[%s], url:%s, level:%s',
                           submit_sm_queue_id, dlr_url, dlr_level)
            yield self.amqpBroker.publish(
                exchange='messaging',
                routing_key='dlr_thrower.http',
                content=DLRContentForHttpapi(
                    pdu_dlr_status,
                    submit_sm_queue_id,
                    dlr_url,
                    dlr_level=2,
                    dlr_connector=pdu_dlr_id,
                    id_smsc=msgid,
                    sub=pdu_dlr_sub,
                    dlvrd=pdu_dlr_dlvrd,
                    subdate=pdu_dlr_sdate,
                    donedate=pdu_dlr_ddate,
                    err=pdu_dlr_err,
                    text=pdu_dlr_text,
                    method=dlr_method
                )
            )

            if pdu_dlr_status in final_states:
                self.log.debug('Removing HTTP dlr map for msgid[%s]', submit_sm_queue_id)
                yield self.redisClient.delete('dlr:%s' % submit_sm_queue_id)

    @defer.inlineCallbacks
    def _handle_smppsapi_deliver_sm(
            self, dlr, submit_sm_queue_id, pdu_dlr_status, success_states, final_states,
            pdu_dlr_err, pdu_dlr_id, msgid, pdu_dlr_sdate, pdu_dlr_ddate,
            pdu_dlr_sub, pdu_dlr_dlvrd, pdu_dlr_text
    ):
        system_id = dlr['system_id']
        source_addr_ton = to_enum(dlr['source_addr_ton'])
        source_addr_npi = to_enum(dlr['source_addr_npi'])
        source_addr = str(dlr['source_addr'])
        dest_addr_ton = to_enum(dlr['dest_addr_ton'])
        dest_addr_npi = to_enum(dlr['dest_addr_npi'])
        destination_addr = str(dlr['destination_addr'])
        sub_date = dlr['sub_date']
        registered_delivery_receipt = to_enum(dlr['rd_receipt'])

        forward_receipt = (
                (pdu_dlr_status in success_states and
                 registered_delivery_receipt == RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED) or
                (pdu_dlr_status not in success_states and
                 registered_delivery_receipt in [
                     RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED,
                     RegisteredDeliveryReceipt.SMSC_DELIVERY_RECEIPT_REQUESTED_FOR_FAILURE
                 ])
        )

        if forward_receipt:
            self.log.debug(
                'Got DLR info for msgid[%s], registered_delivery:%s, system_id:%s',
                submit_sm_queue_id, registered_delivery_receipt, system_id
            )

            yield self.amqpBroker.publish(
                exchange='messaging',
                routing_key='dlr_thrower.smpps',
                content=DLRContentForSmpps(
                    pdu_dlr_status,
                    submit_sm_queue_id,
                    system_id,
                    source_addr,
                    destination_addr,
                    sub_date,
                    source_addr_ton,
                    source_addr_npi,
                    dest_addr_ton,
                    dest_addr_npi,
                    err=pdu_dlr_err
                )
            )

        if pdu_dlr_status in final_states:
            self.log.debug('Removing SMPPs dlr map for msgid[%s]', submit_sm_queue_id)
            yield self.redisClient.delete('dlr:%s' % submit_sm_queue_id)


class DLRLookupSingleton(metaclass=Singleton):
    """Used to launch only one DLRLookup object"""
    objects = {}

    def get(self, config, amqpBroker, redisClient):
        """Return a DLRLookup object or instanciate a new one"""
        name = 'singleton'
        if name not in self.objects:
            self.objects[name] = DLRLookup(config, amqpBroker, redisClient)

        return self.objects[name]
