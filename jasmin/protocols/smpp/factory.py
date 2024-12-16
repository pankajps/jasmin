# pylint: disable=W0401,W0611,W0231
import pickle
import sys
import logging
import re
from enum import Enum
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler

from OpenSSL import SSL
from twisted.internet import defer, reactor, ssl
from twisted.internet.protocol import ClientFactory

from jasmin.routing.Routables import RoutableSubmitSm
from smpp.twisted.protocol import DataHandlerResponse, SMPPSessionStates
from smpp.twisted.server import SMPPBindManager as _SMPPBindManager
from smpp.twisted.server import SMPPServerFactory as _SMPPServerFactory
from smpp.pdu.error import SMPPClientError
from smpp.pdu.pdu_types import CommandId, CommandStatus, PDURequest

from jasmin.protocols.smpp.error import (
    SubmitSmInvalidArgsError, SubmitSmWithoutDestinationAddrError,
    InterceptorRunError, SubmitSmInterceptionError, SubmitSmInterceptionSuccess,
    SubmitSmThroughputExceededError, SubmitSmRoutingError, SubmitSmRouteNotFoundError,
    SubmitSmChargingError, InterceptorNotSetError, InterceptorNotConnectedError
)
from jasmin.protocols.smpp.protocol import SMPPClientProtocol, SMPPServerProtocol
from jasmin.protocols.smpp.stats import SMPPClientStatsCollector, SMPPServerStatsCollector
from jasmin.protocols.smpp.validation import SmppsCredentialValidator

LOG_CATEGORY_CLIENT_BASE = "smpp.client"
LOG_CATEGORY_SERVER_BASE = "smpp.server"


class SmppClientIsNotConnected(Exception):
    """
    Raised when attempting to use the smpp object before it is fully connected (i.e., smpp is still None).
    """


class SMPPClientFactory(ClientFactory):
    """
    Factory for creating SMPP client protocol instances and handling reconnection logic.

    Attributes:
        config: SMPPClientConfig instance with connection and binding options.
        msgHandler: Callable for handling inbound messages from the SMSC.
        stats: SMPPClientStatsCollector instance for metrics.
        smpp: SMPPClientProtocol instance once connected and bound.
        connectDeferred: Deferred for the connection stage.
        exitDeferred: Deferred that fires upon disconnection without reconnection.
        connectionRetry: Boolean flag indicating whether to attempt reconnection.
    """
    protocol = SMPPClientProtocol

    def __init__(self, config, msgHandler=None):
        self.reconnectTimer = None
        self.smpp = None
        self.connectionRetry = True
        self.config = config

        # Setup statistics collector
        self.stats = SMPPClientStatsCollector().get(cid=self.config.id)
        self.stats.set('created_at', datetime.now())

        # Set up a dedicated logger
        self.log = logging.getLogger(f"{LOG_CATEGORY_CLIENT_BASE}.{config.id}")
        if len(self.log.handlers) != 1:
            self.log.setLevel(self.config.log_level)
            rotate_when = getattr(self.config, 'log_rotate', 'midnight')
            if 'stdout' in self.config.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(filename=self.config.log_file, when=rotate_when)
            formatter = logging.Formatter(self.config.log_format, self.config.log_date_format)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

        self.msgHandler = msgHandler if msgHandler is not None else self.msgHandlerStub

    def buildProtocol(self, addr):
        proto = super().buildProtocol(addr)
        proto.log = self.log
        return proto

    def getConfig(self):
        return self.config

    def msgHandlerStub(self, smpp, pdu):
        self.log.warning(f"msgHandlerStub: Received an unhandled message {pdu} ...")

    def startedConnecting(self, connector):
        self.log.info(f"Connecting to {connector.getDestination()} ...")

    def getExitDeferred(self):
        """
        Returns a Deferred that fires once the client disconnects without retrying.
        """
        return self.exitDeferred

    def clientConnectionFailed(self, connector, reason):
        self.log.error(f"Connection failed. Reason: {reason}")

        if self.config.reconnectOnConnectionFailure and self.connectionRetry:
            self.log.info(f"Reconnecting after {self.config.reconnectOnConnectionFailureDelay} seconds ...")
            self.reconnectTimer = reactor.callLater(
                self.config.reconnectOnConnectionFailureDelay, self.reConnect, connector
            )
        else:
            self.connectDeferred.errback(reason)
            self.exitDeferred.callback(None)
            self.log.info("Exiting.")

    def clientConnectionLost(self, connector, reason):
        self.log.error(f"Connection lost. Reason: {reason}")

        if self.config.reconnectOnConnectionLoss and self.connectionRetry:
            self.log.info(f"Reconnecting after {self.config.reconnectOnConnectionLossDelay} seconds ...")
            self.reconnectTimer = reactor.callLater(
                self.config.reconnectOnConnectionLossDelay, self.reConnect, connector
            )
        else:
            self.exitDeferred.callback(None)
            self.log.info("Exiting.")

    def reConnect(self, connector=None):
        """
        Reestablishes the connection if disconnected.
        """
        if connector is None:
            self.log.error("No connector available for reconnection!")
        else:
            # Reset connectDeferred if it was previously fired
            if self.connectDeferred.called:
                self.connectDeferred = defer.Deferred()
                self.connectDeferred.addCallback(self.bind)
            connector.connect()

    def _connect(self):
        """
        Initiate a TCP or SSL connection to the SMSC.
        """
        self.connectionRetry = True

        if self.config.useSSL:
            self.log.info(f"Establishing SSL connection to {self.config.host}:{self.config.port}")
            reactor.connectSSL(self.config.host, self.config.port, self, CtxFactory(self.config))
        else:
            self.log.info(f"Establishing TCP connection to {self.config.host}:{self.config.port}")
            reactor.connectTCP(self.config.host, self.config.port, self)

        self.exitDeferred = defer.Deferred()
        self.connectDeferred = defer.Deferred()
        return self.connectDeferred

    def connectAndBind(self):
        """
        Connect to the SMSC and then initiate the bind process.
        """
        self._connect()
        self.connectDeferred.addCallback(self.bind)
        return self.connectDeferred

    def disconnect(self):
        """
        Disconnect from the SMSC if connected.
        """
        if self.smpp is not None:
            self.log.info('Disconnecting SMPP client')
            return self.smpp.unbindAndDisconnect()
        return None

    def stopConnectionRetrying(self):
        """
        Stop automatic reconnection attempts.
        """
        self.log.info('Stopped automatic connection retrying.')
        if self.reconnectTimer and self.reconnectTimer.active():
            self.reconnectTimer.cancel()
            self.reconnectTimer = None
        self.connectionRetry = False

    def disconnectAndDontRetryToConnect(self):
        """
        Disconnect and prevent any further reconnection attempts.
        """
        self.log.info('Ordering a disconnect with no further reconnections.')
        self.stopConnectionRetrying()
        return self.disconnect()

    def bind(self, smpp):
        """
        Bind the SMPP session according to the specified bind operation.
        """
        self.smpp = smpp
        if self.config.bindOperation == 'transceiver':
            return smpp.bindAsTransceiver()
        elif self.config.bindOperation == 'receiver':
            return smpp.bindAsReceiver()
        elif self.config.bindOperation == 'transmitter':
            return smpp.bindAsTransmitter()
        else:
            raise SMPPClientError(f"Invalid bind operation: {self.config.bindOperation}")

    def getSessionState(self):
        if self.smpp is None:
            return SMPPSessionStates.NONE
        else:
            return self.smpp.sessionState


class CtxFactory(ssl.ClientContextFactory):
    """
    SSL context factory for establishing secure connections if required.
    """

    def __init__(self, config):
        self.smppConfig = config

    def getContext(self):
        self.method = SSL.SSLv23_METHOD
        ctx = super().getContext()
        if self.smppConfig.SSLCertificateFile:
            ctx.use_certificate_file(self.smppConfig.SSLCertificateFile)
        return ctx


class SMPPServerFactory(_SMPPServerFactory):
    """
    Factory for SMPP Server protocols. Handles inbound bind requests, maintains
    a map of bound connections, and routes submit_sm PDUs through RouterPB.
    """
    protocol = SMPPServerProtocol

    def __init__(self, config, auth_portal, RouterPB=None, SMPPClientManagerPB=None,
                 interceptorpb_client=None):
        self.config = config
        self._auth_portal = auth_portal
        self.RouterPB = RouterPB
        self.SMPPClientManagerPB = SMPPClientManagerPB
        self.interceptorpb_client = interceptorpb_client

        # Dictionary of current connections indexed by system_id
        self.bound_connections = {}

        # Setup statistics collector
        self.stats = SMPPServerStatsCollector().get(cid=self.config.id)
        self.stats.set('created_at', datetime.now())

        # Set up logger
        self.log = logging.getLogger(f"{LOG_CATEGORY_SERVER_BASE}.{config.id}")
        if len(self.log.handlers) != 1:
            self.log.setLevel(config.log_level)
            if 'stdout' in self.config.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(filename=self.config.log_file, when=self.config.log_rotate)
            formatter = logging.Formatter(config.log_format, config.log_date_format)
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

        # Use submit_sm_event_interceptor by default
        self.msgHandler = self.submit_sm_event_interceptor

    def addInterceptorPBClient(self, interceptorpb_client):
        self.interceptorpb_client = interceptorpb_client
        self.log.info('Added Interceptor to SMPPServerFactory')

    def submit_sm_event_interceptor(self, system_id, *args):
        """
        Intercept submit_sm before calling submit_sm_event.
        Validates arguments, checks destination address, user credentials, applies defaults,
        and optionally runs interception scripts before routing.
        """
        self.log.debug(f'Intercepting submit_sm event for system_id: {system_id}')

        # Args validation
        if len(args) != 2:
            self.log.error(f'(submit_sm_event/{system_id}) Invalid args: {args}')
            raise SubmitSmInvalidArgsError()

        proto, SubmitSmPDU = args
        if not isinstance(SubmitSmPDU, PDURequest) or SubmitSmPDU.id != CommandId.submit_sm:
            self.log.error(f'(submit_sm_event/{system_id}) Expected submit_sm PDU, got: {SubmitSmPDU}')
            raise SubmitSmInvalidArgsError()

        if not isinstance(proto, SMPPServerProtocol):
            self.log.error(f'(submit_sm_event/{system_id}) Invalid protocol object: {proto}')
            raise SubmitSmInvalidArgsError()

        user = proto.user
        user.getCnxStatus().smpps['submit_sm_request_count'] += 1

        if not SubmitSmPDU.params['destination_addr']:
            self.log.error(f'(submit_sm_event/{system_id}) destination_addr not defined in SubmitSmPDU')
            raise SubmitSmWithoutDestinationAddrError()

        # Credential validation
        validator = SmppsCredentialValidator('Send', user, SubmitSmPDU)
        validator.validate()

        # Update PDU with user defaults
        SubmitSmPDU = validator.updatePDUWithUserDefaults(SubmitSmPDU)

        # Also update chained PDUs (multipart)
        _pdu = SubmitSmPDU
        while hasattr(_pdu, 'nextPdu'):
            _pdu = _pdu.nextPdu
            _pdu = validator.updatePDUWithUserDefaults(_pdu)

        if self.RouterPB is None:
            self.log.error(f'(submit_sm_event_interceptor/{system_id}) RouterPB not set: no routing')
            return

        routable = RoutableSubmitSm(SubmitSmPDU, user)

        # Interception
        interceptor = self.RouterPB.getMTInterceptionTable().getInterceptorFor(routable)
        if interceptor is not None:
            self.log.debug(f"RouterPB selected {interceptor} interceptor for this SubmitSmPDU")
            if self.interceptorpb_client is None:
                self.stats.inc('interceptor_error_count')
                self.log.error("InterceptorPB not set!")
                raise InterceptorNotSetError('InterceptorPB not set!')
            if not self.interceptorpb_client.isConnected:
                self.stats.inc('interceptor_error_count')
                self.log.error("InterceptorPB not connected!")
                raise InterceptorNotConnectedError('InterceptorPB not connected!')

            script = interceptor.getScript()
            self.log.debug(f"Interceptor script loaded: {script}")

            d = self.interceptorpb_client.run_script(script, routable)
            d.addCallback(self.submit_sm_post_interception, system_id=system_id, proto=proto)
            d.addErrback(self.submit_sm_post_interception)
            return d
        else:
            return self.submit_sm_post_interception(routable=routable, system_id=system_id, proto=proto)

    def submit_sm_post_interception(self, *args, **kw):
        """
        Handles submit_sm after interception. Routes the message via RouterPB and sends it
        through SMPPClientManagerPB. Also handles billing, QoS, and logging.
        """
        message_id = None
        status = None

        try:
            # Post-interception result handling
            if len(args) == 1:
                intercept_result = args[0]
                if isinstance(intercept_result, bool) and not intercept_result:
                    self.stats.inc('interceptor_error_count')
                    self.log.error('Interceptor returned False.')
                    raise InterceptorRunError('Interception failed')
                elif isinstance(intercept_result, dict) and intercept_result['smpp_status'] > 0:
                    self.stats.inc('interceptor_error_count')
                    self.log.error(f"Interceptor script returned error smpp_status {intercept_result['smpp_status']}")
                    raise SubmitSmInterceptionError(code=intercept_result['smpp_status'])
                elif isinstance(intercept_result, dict) and intercept_result['smpp_status'] == 0:
                    self.stats.inc('interceptor_count')
                    self.log.info(f'Interceptor script returned success smpp_status {intercept_result["smpp_status"]}')
                    if 'message_id' in intercept_result['extra']:
                        message_id = str(intercept_result['extra']['message_id'])
                    raise SubmitSmInterceptionSuccess()
                elif isinstance(intercept_result, (str, bytes)):
                    self.stats.inc('interceptor_count')
                    routable = pickle.loads(intercept_result)
                else:
                    self.stats.inc('interceptor_error_count')
                    self.log.error(f'Interceptor returned invalid data: {intercept_result}')
                    raise InterceptorRunError(f'Invalid interceptor return: {intercept_result}')
            else:
                routable = kw['routable']

            system_id = kw['system_id']
            proto = kw['proto']

            self.log.debug(f'Handling submit_sm_post_interception for system_id: {system_id}')

            # Route lookup
            route = self.RouterPB.getMTRoutingTable().getRouteFor(routable)
            if route is None:
                self.log.error("No route found for user %s and SubmitSmPDU: %s", routable.user, routable.pdu)
                raise SubmitSmRouteNotFoundError()

            # Connector selection
            self.log.debug(f"Route {route} selected for SubmitSmPDU")
            routedConnector = route.getConnector()

            # Failover route logic
            if repr(route) == 'FailoverMTRoute':
                self.log.debug('Failover route selected, ensuring connector is bound.')
                while True:
                    c = self.SMPPClientManagerPB.perspective_connector_details(routedConnector.cid)
                    if c:
                        self.log.debug(f'Connector [{routedConnector.cid}] session_state: {c["session_state"]}')
                    else:
                        self.log.debug(f'Connector [{routedConnector.cid}] not found')

                    if c and c['session_state'].startswith('BOUND_'):
                        break
                    else:
                        routedConnector = route.getConnector()
                        if routedConnector is None:
                            break

            if routedConnector is None:
                self.log.error("Failover route has no bound connector for SubmitSmPDU: %s", routable.pdu)
                raise SubmitSmRoutingError()

            # QoS throttling
            if (routable.user.mt_credential.getQuota('smpps_throughput') is not None and
                    routable.user.mt_credential.getQuota('smpps_throughput') >= 0 and
                    routable.user.getCnxStatus().smpps['qos_last_submit_sm_at'] != 0):
                qos_throughput_second = 1 / float(routable.user.mt_credential.getQuota('smpps_throughput'))
                qos_throughput_td = timedelta(microseconds=qos_throughput_second * 1000000)
                qos_delay = datetime.now() - routable.user.getCnxStatus().smpps['qos_last_submit_sm_at']
                if qos_delay < qos_throughput_td:
                    self.log.error(
                        "QoS exceeded for user (%s): current delay %s vs required %s, rejecting message.",
                        routable.user, qos_delay, qos_throughput_td)
                    raise SubmitSmThroughputExceededError()

            routable.user.getCnxStatus().smpps['qos_last_submit_sm_at'] = datetime.now()

            # Billing
            if self.config.billing_feature:
                bill = route.getBillFor(routable.user)
                self.log.debug(
                    f"SubmitSmBill [bid:{bill.bid}] [total:{bill.getTotalAmounts()}] for this SubmitSmPDU")
                charging_requirements = []
                u_balance = routable.user.mt_credential.getQuota('balance')
                u_subsm_count = routable.user.mt_credential.getQuota('submit_sm_count')
                if u_balance is not None and bill.getTotalAmounts() > 0:
                    charging_requirements.append({
                        'condition': bill.getTotalAmounts() <= u_balance,
                        'error_message': f'Insufficient balance ({u_balance}) for cost {bill.getTotalAmounts()}'
                    })
                if u_subsm_count is not None:
                    charging_requirements.append({
                        'condition': bill.getAction('decrement_submit_sm_count') <= u_subsm_count,
                        'error_message': f'Insufficient submit_sm_count ({u_subsm_count}) for cost {bill.getAction("decrement_submit_sm_count")}'
                    })

                if self.RouterPB.chargeUserForSubmitSms(routable.user, bill, requirements=charging_requirements) is None:
                    self.log.error(
                        f'Charging user {routable.user} failed [bid:{bill.bid}, total:{bill.getTotalAmounts()}]')
                    raise SubmitSmChargingError()
            else:
                bill = None

            # Priority from SubmitSmPDU
            priority = 0
            if routable.pdu.params['priority_flag'] is not None:
                priority = routable.pdu.params['priority_flag']._value_

            if self.SMPPClientManagerPB is None:
                self.log.error(f'(submit_sm_event/{system_id}) SMPPClientManagerPB not set, no submission.')
                return

            # Send SubmitSmPDU through SMPPClientManagerPB
            self.log.debug(f"Selected connector '{routedConnector.cid}' for SubmitSmPDU")
            c = self.SMPPClientManagerPB.perspective_submit_sm(
                uid=routable.user.uid,
                cid=routedConnector.cid,
                SubmitSmPDU=routable.pdu,
                submit_sm_bill=bill,
                priority=priority,
                pickled=False,
                source_connector=proto
            )

            if not hasattr(c, 'result') or not c.result:
                self.log.error(f'Failed to send SubmitSmPDU to [cid:{routedConnector.cid}], got: {c}')
                raise SubmitSmRoutingError()

            # message_id retrieved from ESME_ROK scenario
            message_id = c.result

        except (SubmitSmInterceptionError, SubmitSmInterceptionSuccess, InterceptorRunError,
                SubmitSmRouteNotFoundError, SubmitSmThroughputExceededError, SubmitSmChargingError,
                SubmitSmRoutingError) as e:
            # Known exceptions have a defined status
            status = e.status
        except Exception as e:
            # Unknown exception
            self.log.critical(f'Unknown exception: {e}')
            status = CommandStatus.ESME_RUNKNOWNERR
        else:
            # Successfully routed and submitted
            if self.config.log_privacy:
                logged_content = f'** {len(routable.pdu.params["short_message"])} byte content **'
            else:
                # Sanitize non-printable chars
                content = routable.pdu.params['short_message']
                if isinstance(content, bytes):
                    logged_content = repr(re.sub(rb'[^\x20-\x7E]+', b'.', content))
                else:
                    logged_content = repr(re.sub(r'[^\x20-\x7E]+', '.', content))

            self.log.info(
                f"SMS-MT [uid:{routable.user.uid}] [cid:{routedConnector.cid}] "
                f"[msgid:{message_id}] [prio:{priority}] [from:{routable.pdu.params['source_addr']}] "
                f"[to:{routable.pdu.params['destination_addr']}] [content:{logged_content}]"
            )
            status = CommandStatus.ESME_ROK
        finally:
            if message_id is not None:
                return DataHandlerResponse(status=status, message_id=message_id)
            elif status is not None:
                return DataHandlerResponse(status=status)

    def buildProtocol(self, addr):
        proto = super().buildProtocol(addr)
        proto.log = self.log
        return proto

    def addBoundConnection(self, connection, user):
        """
        Overload to remove dependency on config.systems. Manages user-bound connections.
        """
        system_id = connection.system_id
        self.log.debug(f'Adding SMPP binding for {system_id}')
        if system_id not in self.bound_connections:
            self.bound_connections[system_id] = SMPPBindManager(user)
        self.bound_connections[system_id].addBinding(connection)
        bind_type = connection.bind_type
        self.log.info(
            f"Added {bind_type} bind for '{system_id}'. Active binds: {self.getBoundConnectionCountsStr(system_id)}."
        )

    def removeConnection(self, connection):
        """
        Overload to remove dependency on config.systems. Removes bindings for a given connection.
        """
        if connection.system_id is None:
            self.log.debug("SMPP connection attempt failed without binding.")
        else:
            system_id = connection.system_id
            bind_type = connection.bind_type
            self.bound_connections[system_id].removeBinding(connection)
            self.log.info(
                f"Dropped {bind_type} bind for '{system_id}'. Active binds: {self.getBoundConnectionCountsStr(system_id)}."
            )

            if self.bound_connections[system_id].getBindingCount() == 0:
                self.bound_connections.pop(system_id)

    def canOpenNewConnection(self, user, bind_type):
        """
        Checks if a new connection can be opened for a given user and bind_type.
        """
        # Authorization
        if not user.smpps_credential.getAuthorization('bind'):
            self.log.warning(
                f'New bind rejected for username: "{user.username}", authorization failed.'
            )
            return False
        # Quotas
        max_bindings = user.smpps_credential.getQuota('max_bindings')
        if max_bindings is not None:
            bind_count = (user.getCnxStatus().smpps['bound_connections_count']['bind_transmitter'] +
                          user.getCnxStatus().smpps['bound_connections_count']['bind_receiver'] +
                          user.getCnxStatus().smpps['bound_connections_count']['bind_transceiver'])
            if bind_count >= max_bindings:
                self.log.warning(
                    f'New bind rejected for username: "{user.username}", max_bindings limit reached.'
                )
                return False

        return True

    def unbindAndRemoveGateway(self, user, ban=True):
        """
        Unbind and optionally ban a user from binding again.
        """
        if ban:
            user.smpps_credential.setAuthorization('bind', False)

        d = self.unbindGateway(user.username)
        return d


class SMPPBindManager(_SMPPBindManager):
    """
    Overloaded SMPPBindManager to include user tracking and connection status updates.
    """

    def __init__(self, user):
        super().__init__(system_id=user.username)
        self.user = user

    def addBinding(self, connection):
        super().addBinding(connection)
        self.user.getCnxStatus().smpps['bind_count'] += 1
        self.user.getCnxStatus().smpps['bound_connections_count'][connection.bind_type.name] += 1

    def removeBinding(self, connection):
        super().removeBinding(connection)
        self.user.getCnxStatus().smpps['unbind_count'] += 1
        self.user.getCnxStatus().smpps['bound_connections_count'][connection.bind_type.name] -= 1
