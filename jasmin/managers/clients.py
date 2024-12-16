import pickle
import datetime
import sys
import time
import logging
from logging.handlers import TimedRotatingFileHandler

from twisted.internet import defer
from twisted.spread import pb

import jasmin
from jasmin.protocols.smpp.protocol import SMPPServerProtocol
from jasmin.protocols.smpp.services import SMPPClientService
from jasmin.tools.migrations.configuration import ConfigurationMigrator
from smpp.pdu.pdu_types import RegisteredDeliveryReceipt
from smpp.twisted.protocol import SMPPSessionStates
from .configs import SMPPClientSMListenerConfig
from .content import SubmitSmContent
from .listeners import SMPPClientSMListener

LOG_CATEGORY = "jasmin-pb-client-mgmt"


class ConfigProfileLoadingError(Exception):
    """Raised for any error occurring while loading a configuration profile."""


class SMPPClientManagerPB(pb.Avatar):
    def __init__(self, SMPPClientPBConfig):
        """
        Initialize the SMPPClientManagerPB.

        :param SMPPClientPBConfig: Configuration object for SMPP Client PB.
        """
        self.config = SMPPClientPBConfig
        self.avatar = None
        self.redisClient = None
        self.amqpBroker = None
        self.interceptorpb_client = None
        self.RouterPB = None
        self.connectors = []
        self.declared_queues = []
        self.pickleProtocol = pickle.HIGHEST_PROTOCOL
        self.persisted = True  # Persistence flag

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

        # Set pickleProtocol from config
        self.pickleProtocol = self.config.pickle_protocol

        self.log.info(f'SMPP Client manager configured and ready.')

    def setAvatar(self, avatar):
        if isinstance(avatar, str):
            self.log.info(f'Authenticated Avatar: {avatar}')
        else:
            self.log.info('Anonymous connection')
        self.avatar = avatar

    def addAmqpBroker(self, amqpBroker):
        self.amqpBroker = amqpBroker
        self.log.info('Added amqpBroker to SMPPClientManagerPB')

    def addRedisClient(self, redisClient):
        self.redisClient = redisClient
        self.log.info('Added Redis Client to SMPPClientManagerPB')

    def addInterceptorPBClient(self, interceptorpb_client):
        self.interceptorpb_client = interceptorpb_client
        self.log.info('Added interceptorpb_client to SMPPClientManagerPB')

    def addRouterPB(self, RouterPB):
        self.RouterPB = RouterPB
        self.log.info('Added RouterPB to SMPPClientManagerPB')

    def getConnector(self, cid):
        """Retrieve connector by cid."""
        for c in self.connectors:
            if str(c['id']) == str(cid):
                self.log.debug(f'getConnector [{cid}] returned a connector')
                return c

        self.log.debug(f'getConnector [{cid}] returned None')
        return None

    def getConnectorDetails(self, cid):
        """Get a dictionary of connector details."""
        c = self.getConnector(cid)
        if c is None:
            self.log.debug(f'getConnectorDetails [{cid}] returned None')
            return None

        details = {
            'id': c['id'],
            'session_state': c['service'].SMPPClientFactory.getSessionState().name,
            'service_status': c['service'].running,
            'start_count': c['service'].startCounter,
            'stop_count': c['service'].stopCounter
        }

        self.log.debug(f'getConnectorDetails [{cid}] returned details')
        return details

    def delConnector(self, cid):
        """Delete a connector by cid."""
        for i, conn in enumerate(self.connectors):
            if str(conn['id']) == str(cid):
                del self.connectors[i]
                self.log.debug(f'Deleted connector [{cid}].')
                return True

        self.log.debug(f'Deleting connector [{cid}] failed.')
        return False

    def perspective_version_release(self):
        return jasmin.get_release()

    def perspective_version(self):
        return jasmin.get_version()

    def perspective_persist(self, profile='jcli-prod'):
        """
        Persist current configuration to a profile file.
        """
        path = f'{self.config.store_path}/{profile}.smppccs'
        self.log.info(f'Persisting current configuration to [{profile}] profile in {path}')

        try:
            connectors_data = []
            for c in self.connectors:
                connectors_data.append({
                    'id': c['id'],
                    'config': c['config'],
                    'service_status': c['service'].running
                })

            with open(path, 'wb') as fh:
                fh.write((f'Persisted on {time.strftime("%c")} [Jasmin {jasmin.get_release()}]\n').encode('ascii'))
                fh.write(pickle.dumps(connectors_data, self.pickleProtocol))

            # Mark state as persisted
            self.persisted = True
        except IOError:
            self.log.error(f'Cannot persist to {path}')
            return False
        except Exception as e:
            self.log.error(f'Unknown error occurred while persisting configuration: {e}')
            return False

        return True

    @defer.inlineCallbacks
    def perspective_load(self, profile='jcli-prod'):
        """
        Load and activate a given configuration profile.
        """
        path = f'{self.config.store_path}/{profile}.smppccs'
        self.log.info(f'Loading/Activating [{profile}] profile configuration from {path}')

        try:
            with open(path, 'rb') as fh:
                lines = fh.readlines()

            # Initialize migrator
            cf = ConfigurationMigrator(context='smppccs', header=lines[0].decode('ascii'),
                                       data=b''.join(lines[1:]))

            # Remove current configuration
            yield self._remove_all_connectors()

            # Apply configuration
            loadedConnectors = cf.getMigratedData()
            for loadedConnector in loadedConnectors:
                addRet = yield self.perspective_connector_add(
                    pickle.dumps(loadedConnector['config'], self.pickleProtocol)
                )
                if not addRet:
                    raise ConfigProfileLoadingError(f'Error adding connector {loadedConnector["id"]}')

                if loadedConnector['service_status'] == 1:
                    startRet = yield self.perspective_connector_start(loadedConnector['id'])
                    if not startRet:
                        self.log.error(f'Error starting connector {loadedConnector["id"]}')

            # Mark as persisted
            self.persisted = True

        except IOError as e:
            self.log.error(f'Cannot load configuration from {path}: {e}')
            defer.returnValue(False)
        except ConfigProfileLoadingError as e:
            self.log.error(f'Error while loading configuration: {e}')
            defer.returnValue(False)
        except Exception as e:
            self.log.error(f'Unknown error occurred while loading configuration: {e}')
            defer.returnValue(False)

        defer.returnValue(True)

    def perspective_is_persisted(self):
        return self.persisted

    @defer.inlineCallbacks
    def perspective_connector_add(self, ClientConfig):
        """
        Add a new connector.
        """
        c = pickle.loads(ClientConfig)
        self.log.debug(f'Adding a new connector {c.id}')

        if self.getConnector(c.id) is not None:
            self.log.error(f'Connector with cid {c.id} already exists')
            defer.returnValue(False)

        if not self._check_amqp_broker():
            defer.returnValue(False)

        # Set prefetch limit for throttling
        yield self.amqpBroker.chan.basic_qos(prefetch_count=1)

        # Declare submit_sm queue and bind
        submit_sm_queue = f'submit.sm.{c.id}'
        routing_key = f'submit.sm.{c.id}'
        self.log.info(f'Binding {submit_sm_queue} queue to {routing_key} route_key')
        yield self.amqpBroker.named_queue_declare(queue=submit_sm_queue)
        yield self.amqpBroker.chan.queue_bind(queue=submit_sm_queue, exchange="messaging", routing_key=routing_key)

        # Setup SMPP client service
        serviceManager = SMPPClientService(c, self.config)
        smListener = SMPPClientSMListener(
            config=SMPPClientSMListenerConfig(self.config.config_file),
            SMPPClientFactory=serviceManager.SMPPClientFactory,
            amqpBroker=self.amqpBroker,
            redisClient=self.redisClient,
            RouterPB=self.RouterPB,
            interceptorpb_client=self.interceptorpb_client
        )
        serviceManager.SMPPClientFactory.msgHandler = smListener.deliver_sm_event_interceptor

        self.connectors.append({
            'id': c.id,
            'config': c,
            'service': serviceManager,
            'consumer_tag': None,
            'submit_sm_q': None,
            'sm_listener': smListener
        })

        self.log.info(f'Added a new connector: {c.id}')

        # Mark as not persisted
        self.persisted = False
        defer.returnValue(True)

    @defer.inlineCallbacks
    def perspective_connector_remove(self, cid):
        """
        Remove a connector after stopping it.
        """
        self.log.debug(f'Removing connector [{cid}]')

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error(f'Unknown connector cid: {cid}')
            defer.returnValue(False)

        # Stop before removal
        if connector['service'].running == 1:
            self.log.debug(f'Stopping service for connector [{cid}] before removing')
            connector['service'].stopService()

        # Stop the queue consumer if any
        yield self.perspective_connector_stop(cid)

        if self.delConnector(cid):
            self.log.info(f'Removed connector [{cid}]')
            self.persisted = False
            defer.returnValue(True)
        else:
            self.log.error(f'Error removing connector [{cid}], cid not found')
            defer.returnValue(False)

        self.persisted = False
        defer.returnValue(True)

    def perspective_connector_list(self):
        """
        Return a list of all connectors with limited details.
        """
        self.log.debug(f'Connector list requested, returning {self.connectors}')

        connectorList = []
        for connector in self.connectors:
            c = self.getConnectorDetails(connector['id'])
            connectorList.append(c)

        self.log.info(f'Returning a list of {len(connectorList)} connectors')
        return connectorList

    @defer.inlineCallbacks
    def perspective_connector_start(self, cid):
        """
        Start a connector service.
        """
        self.log.debug(f'Starting connector [{cid}]')

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error(f'Unknown connector cid: {cid}')
            defer.returnValue(False)

        if not self._check_amqp_broker():
            defer.returnValue(False)

        if connector['service'].running == 1:
            self.log.error(f'Connector [{cid}] is already running.')
            defer.returnValue(False)

        # Check acceptable start states
        acceptedStartStates = [None, SMPPSessionStates.NONE, SMPPSessionStates.UNBOUND]
        if connector['service'].SMPPClientFactory.getSessionState() not in acceptedStartStates:
            state = connector['service'].SMPPClientFactory.getSessionState()
            self.log.error(f'Connector [{cid}] cannot start in session_state: {state}')
            defer.returnValue(False)

        connector['service'].startService()

        # Start the queue consumer for submit_sm
        submit_sm_queue = f'submit.sm.{connector["id"]}'
        consumerTag = f'SMPPClientFactory-{connector["id"]}'

        try:
            # If there's an existing consumer, stop it first
            if connector['consumer_tag'] is not None:
                self.log.debug(f'Stopping submit_sm_q consumer in connector [{cid}]')
                yield self.amqpBroker.chan.basic_cancel(consumer_tag=connector['consumer_tag'])

            yield self.amqpBroker.chan.basic_consume(
                queue=submit_sm_queue,
                no_ack=False, consumer_tag=consumerTag
            )
        except Exception as e:
            self.log.error(f'Error consuming from queue {submit_sm_queue}: {e}')
            defer.returnValue(False)

        submit_sm_q = yield self.amqpBroker.client.queue(consumerTag)
        self.log.info(f'{consumerTag} is consuming from queue: {submit_sm_queue}')

        d = submit_sm_q.get()
        d.addCallback(connector['sm_listener'].submit_sm_callback).addErrback(
            connector['sm_listener'].submit_sm_errback)

        self.log.info(f'Started connector [{cid}]')

        connector['sm_listener'].setSubmitSmQ(submit_sm_q)
        connector['consumer_tag'] = consumerTag
        connector['submit_sm_q'] = submit_sm_q

        # Mark as not persisted
        self.persisted = False
        defer.returnValue(True)

    @defer.inlineCallbacks
    def perspective_connector_stop(self, cid, delQueues=False):
        """
        Stop a connector service.
        """
        self.log.debug(f'Stopping connector [{cid}]')

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error(f'Unknown connector cid: {cid}')
            defer.returnValue(False)

        # Stop the queue consumer
        if connector['consumer_tag'] is not None:
            self.log.debug(f'Stopping submit_sm_q consumer in connector [{cid}]')
            yield self.amqpBroker.chan.basic_cancel(consumer_tag=connector['consumer_tag'])

            connector['submit_sm_q'] = None
            connector['consumer_tag'] = None

        if connector['service'].running == 0:
            self.log.error(f'Connector [{cid}] is already stopped.')
            defer.returnValue(False)

        # Delete queues if requested
        if delQueues:
            submitSmQueueName = f'submit.sm.{cid}'
            self.log.debug(f'Deleting queue [{submitSmQueueName}]')
            yield self.amqpBroker.chan.queue_delete(queue=submitSmQueueName)

        # Reject & requeue any pending messages before stopping
        if connector['sm_listener'].rejectTimers:
            for msgid, timer in list(connector['sm_listener'].rejectTimers.items()):
                if timer.active():
                    func = timer.func
                    kw = timer.kw
                    timer.cancel()
                    del connector['sm_listener'].rejectTimers[msgid]

                    self.log.debug(f'Rejecting/requeuing msgid [{msgid}] before stopping connector')
                    yield func(**kw)

        # Clear timers
        self.log.debug(f'Clearing sm_listener timers in connector [{cid}]')
        connector['sm_listener'].clearAllTimers()
        connector['sm_listener'].submit_sm_q = None

        connector['service'].stopService()
        self.log.info(f'Stopped connector [{cid}]')

        self.persisted = False
        defer.returnValue(True)

    @defer.inlineCallbacks
    def perspective_connector_stopall(self, delQueues=False):
        """
        Stop all connector services.
        """
        self.log.debug('Stopping all connectors')

        for connector in self.connectors:
            yield self.perspective_connector_stop(connector['id'], delQueues)

        self.persisted = False
        defer.returnValue(True)

    def perspective_service_status(self, cid):
        """
        Return the running status of a connector's service.
        """
        self.log.debug(f'Requested service status {cid}')
        connector = self.getConnector(cid)
        if connector is None:
            self.log.error(f'Unknown connector cid: {cid}')
            return False

        service_status = connector['service'].running
        self.log.info(f'Connector [{cid}] service status is: {service_status}')
        return service_status

    def perspective_session_state(self, cid):
        """
        Return the session state of a client connector.
        """
        self.log.debug(f'Requested session state for connector [{cid}]')

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error(f'Unknown connector cid: {cid}')
            return False

        session_state = connector['service'].SMPPClientFactory.getSessionState()
        self.log.info(f'Connector [{cid}] session state is: {session_state}')

        if session_state is None:
            return None
        else:
            return session_state.name

    def perspective_connector_details(self, cid):
        """
        Return the connector details.
        """
        self.log.debug(f'Requested details for connector [{cid}]')

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error(f'Unknown connector cid: {cid}')
            return False

        return self.getConnectorDetails(cid)

    def perspective_connector_config(self, cid):
        """
        Return the connector SMPPClientConfig object.
        """
        self.log.debug(f'Requested config for connector [{cid}]')

        connector = self.getConnector(cid)
        if connector is None:
            self.log.error(f'Unknown connector cid: {cid}')
            return False

        return pickle.dumps(connector['config'], self.pickleProtocol)

    @defer.inlineCallbacks
    def perspective_submit_sm(self, uid, cid, SubmitSmPDU, submit_sm_bill,
                              priority=1, validity_period=None,
                              pickled=True, dlr_url=None, dlr_level=1, dlr_method='POST',
                              dlr_connector=None, source_connector='httpapi'):
        """
        Enqueue a submit_sm to a connector.
        """
        connector = self.getConnector(cid)
        if connector is None:
            self.log.error(f'Unknown connector cid: {cid}')
            defer.returnValue(False)

        if not self._check_amqp_broker():
            defer.returnValue(False)

        # If not pickled, we must pickle now
        PickledSubmitSmPDU = SubmitSmPDU if pickled else pickle.dumps(SubmitSmPDU, self.pickleProtocol)
        if submit_sm_bill is not None and not pickled:
            submit_sm_bill = pickle.dumps(submit_sm_bill, self.pickleProtocol)

        pubQueueName = f"submit.sm.{cid}"
        responseQueueName = f"submit.sm.resp.{cid}"

        self.log.debug(f'Publishing SubmitSmPDU with routing_key={pubQueueName}, priority={priority}')
        c = SubmitSmContent(
            uid=uid,
            body=PickledSubmitSmPDU,
            replyto=responseQueueName,
            submit_sm_bill=submit_sm_bill,
            priority=priority,
            expiration=validity_period,
            source_connector='httpapi' if source_connector == 'httpapi' else 'smppsapi',
            destination_cid=cid
        )
        yield self.amqpBroker.publish(exchange='messaging', routing_key=pubQueueName, content=c)

        # Handle DLR or SMPPs mapping
        yield self._handle_submit_sm_dlr(c, dlr_url, dlr_level, dlr_method, dlr_connector,
                                         source_connector, SubmitSmPDU, connector)

        defer.returnValue(c.properties['message-id'])

    def _check_amqp_broker(self):
        """Check if AMQP broker is set and connected."""
        if self.amqpBroker is None:
            self.log.error('AMQP Broker is not added')
            return False
        if not self.amqpBroker.connected:
            self.log.error('AMQP Broker channel is not yet ready')
            return False
        return True

    @defer.inlineCallbacks
    def _remove_all_connectors(self):
        """Remove all connectors before loading a profile."""
        CIDs = [c['id'] for c in self.connectors]
        for cid in CIDs:
            remRet = yield self.perspective_connector_remove(cid)
            if not remRet:
                raise ConfigProfileLoadingError(f'Error removing connector {cid}')
            self.log.info(f'Removed connector [{cid}]')

    @defer.inlineCallbacks
    def _handle_submit_sm_dlr(self, c, dlr_url, dlr_level, dlr_method, dlr_connector,
                              source_connector, SubmitSmPDU, connector):
        """
        Handle DLR enqueueing or SMPPs mapping depending on source_connector.
        """
        msgid = c.properties['message-id']

        if source_connector == 'httpapi' and dlr_url is not None:
            # Enqueue DLR request in redis if available
            if self.redisClient is None or str(self.redisClient) == '<Redis Connection: Not connected>':
                self.log.warning(f"DLR not enqueued for message id:{msgid}, RC is not connected.")
            else:
                self.log.debug(
                    f'Setting DLR url ({dlr_url}) and level ({dlr_level}) for message id:{msgid}, expiring in {connector["config"].dlr_expiry}'
                )
                hashKey = f"dlr:{msgid}"
                hashValues = {
                    'sc': 'httpapi',
                    'url': dlr_url,
                    'level': dlr_level,
                    'method': dlr_method,
                    'connector': dlr_connector,
                    'expiry': connector['config'].dlr_expiry
                }
                yield self.redisClient.hmset(hashKey, hashValues)
                yield self.redisClient.expire(hashKey, connector['config'].dlr_expiry)
        elif (isinstance(source_connector, SMPPServerProtocol) and
              SubmitSmPDU.params['registered_delivery'].receipt != RegisteredDeliveryReceipt.NO_SMSC_DELIVERY_RECEIPT_REQUESTED):
            # SMPPs mapping if DLR is requested
            if self.redisClient is None or str(self.redisClient) == '<Redis Connection: Not connected>':
                self.log.warning(f"SMPPs mapping not done for message id:{msgid}, RC is not connected.")
            else:
                self.log.debug(
                    f'Setting SMPPs connector ({source_connector.system_id}) mapping for msgid:{msgid}, '
                    f'rd:{SubmitSmPDU.params["registered_delivery"]}, expiring in {source_connector.factory.config.dlr_expiry}'
                )

                hashKey = f"dlr:{msgid}"
                hashValues = {
                    'sc': 'smppsapi',
                    'system_id': source_connector.system_id,
                    'source_addr_ton': SubmitSmPDU.params['source_addr_ton'],
                    'source_addr_npi': SubmitSmPDU.params['source_addr_npi'],
                    'source_addr': SubmitSmPDU.params['source_addr'],
                    'dest_addr_ton': SubmitSmPDU.params['dest_addr_ton'],
                    'dest_addr_npi': SubmitSmPDU.params['dest_addr_npi'],
                    'destination_addr': SubmitSmPDU.params['destination_addr'],
                    'sub_date': datetime.datetime.now(),
                    'rd_receipt': SubmitSmPDU.params['registered_delivery'].receipt,
                    'expiry': source_connector.factory.config.dlr_expiry
                }
                yield self.redisClient.hmset(hashKey, hashValues)
                yield self.redisClient.expire(hashKey, source_connector.factory.config.dlr_expiry)
