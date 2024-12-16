# pylint: disable=E0203
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

from twisted.internet.protocol import ClientFactory
from twisted.internet import defer, reactor
from txamqp.client import TwistedDelegate
from jasmin.queues.protocol import AmqpProtocol

LOG_CATEGORY = "jasmin-amqp-factory"


class AmqpFactory(ClientFactory):
    protocol = AmqpProtocol

    def __init__(self, config):
        """
        Initialize the AmqpFactory with the given configuration.

        :param config: The AMQP connection configuration object, which includes
                       host, port, username, password, vhost, heartbeat, and
                       reconnection strategies.
        """
        self.reconnectTimer = None
        self.connectionRetry = True
        self.connected = False
        self.config = config
        self.channelReady = None
        self.delegate = TwistedDelegate()
        self.amqp = None   # The current protocol instance.
        self.client = None # Alias for protocol instance.
        self.queues = []   # Track declared queues to avoid redeclaration.

        self.log = self._setup_logger()
        self.log.debug("AmqpFactory initialized with config: %s", self.config)

    def _setup_logger(self):
        """Set up and return a dedicated logger instance."""
        logger = logging.getLogger(LOG_CATEGORY)
        if not logger.handlers:
            logger.setLevel(self.config.log_level)
            if 'stdout' in self.config.log_file:
                handler = logging.StreamHandler(sys.stdout)
            else:
                handler = TimedRotatingFileHandler(
                    filename=self.config.log_file,
                    when=self.config.log_rotate
                )
            formatter = logging.Formatter(
                self.config.log_format,
                self.config.log_date_format
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False
        return logger

    def preConnect(self):
        """
        Prepare deferreds before connecting.

        This method is called to ensure deferreds are ready. It's not called
        automatically when Jasmin is run as a twistd plugin, so it should be
        invoked manually before connecting.
        """
        self.connectionRetry = True
        self.exitDeferred = defer.Deferred()

        if self.channelReady is None:
            self.channelReady = defer.Deferred()

        # (Re)initialize connectDeferred if it was previously called or doesn't exist yet.
        if not hasattr(self, 'connectDeferred') or self.connectDeferred.called:
            self.connectDeferred = defer.Deferred()
            self.connectDeferred.addCallback(self.authenticate)

    def startedConnecting(self, connector):
        """Called when the factory starts to attempt a connection."""
        self.log.info("Connecting to %s ...", connector.getDestination())

    def getExitDeferred(self):
        """
        Get a Deferred that fires once the connection is lost and no reconnection
        attempts will be made.
        """
        return self.exitDeferred

    def getChannelReadyDeferred(self):
        """
        Get a Deferred that fires once the channel is successfully opened and ready.
        """
        return self.channelReady

    def clientConnectionFailed(self, connector, reason):
        """
        Called when a connection attempt fails.
        """
        self.log.error("Connection failed. Reason: %s", reason)
        self.connected = False

        if self.config.reconnectOnConnectionFailure and self.connectionRetry:
            delay = self.config.reconnectOnConnectionFailureDelay
            self.log.info("Reconnecting after %d seconds ...", delay)
            self.reconnectTimer = reactor.callLater(delay, self.reConnect, connector)
        else:
            self.connectDeferred.errback(reason)
            self.exitDeferred.callback(self)
            self.log.info("Exiting (no reconnection attempts).")

    def clientConnectionLost(self, connector, reason):
        """
        Called when the connection is lost after it was previously established.
        """
        if 'Connection was closed cleanly.' not in str(reason):
            self.log.error("Connection lost. Reason: %s", reason)
        else:
            self.log.info("Connection lost cleanly: %s", reason)

        self.connected = False
        self.client = None

        if self.config.reconnectOnConnectionLoss and self.connectionRetry:
            delay = self.config.reconnectOnConnectionLossDelay
            self.log.info("Reconnecting after %d seconds ...", delay)
            self.reconnectTimer = reactor.callLater(delay, self.reConnect, connector)
        else:
            self.exitDeferred.callback(self)
            self.log.info("Exiting (no reconnection attempts).")

    def reConnect(self, connector=None):
        """
        Reconnect the client using the provided connector.

        :param connector: The connector to use for reconnection attempts.
        """
        if connector is None:
            self.log.error("No connector provided for reconnection.")
            return
        self.preConnect()
        connector.connect()

    def _connect(self):
        """
        Establish a TCP connection to the AMQP broker using the given config.
        """
        self.log.info('Establishing TCP connection to %s:%d', self.config.host, self.config.port)
        reactor.connectTCP(self.config.host, self.config.port, self)
        self.preConnect()
        return self.connectDeferred

    def connect(self):
        """
        Initiate the connection to the AMQP broker and return a deferred
        that fires upon successful connection or failure.
        """
        return self._connect()

    def buildProtocol(self, addr):
        """
        Build the AMQP protocol instance when the connection is established.
        """
        p = self.protocol(self.delegate, self.config.vhost, self.config.getSpec(),
                          heartbeat=self.config.heartbeat)
        p.factory = self
        self.client = p
        return p

    def authenticate(self, _):
        """
        Authenticate with the AMQP broker using the provided credentials.
        """
        deferred = self.client.start({"LOGIN": self.config.username, "PASSWORD": self.config.password})
        deferred.addCallback(self._authenticated)
        deferred.addErrback(self._authentication_failed)

    def _authenticated(self, _):
        """Called after successful authentication."""
        self.log.info("Successfully authenticated to AMQP broker.")
        d = self.client.channel(1)
        d.addCallback(self._got_channel)
        d.addErrback(self._got_channel_failed)

    def _got_channel(self, chan):
        """
        Called once a channel has been obtained.
        """
        self.log.info("Acquired channel from AMQP broker.")
        self.chan = chan
        self.queues = []
        d = self.chan.channel_open()
        d.addCallback(self._channel_open)
        d.addErrback(self._channel_open_failed)

    def _channel_open(self, _):
        """Called when the channel is successfully opened."""
        self.log.info("Channel is open and ready.")
        self.connected = True
        self.channelReady.callback(self)

    def _channel_open_failed(self, error):
        """Called when opening the channel fails."""
        self.log.error("Failed to open channel: %s", error)

    def _got_channel_failed(self, error):
        """Called if acquiring a channel fails."""
        self.log.error("Error getting channel: %s", error)

    def _authentication_failed(self, error):
        """Called if authentication fails."""
        self.log.error("AMQP authentication failed: %s", error)

    def disconnect(self, reason=None):
        """
        Disconnect the current AMQP connection.

        Note: Setting `self.channelReady` to False may not be strictly
        correct, since it was originally a Deferred. This matches the original
        logic and ensures that functionality remains unchanged.
        """
        self.channelReady = False

        if self.client is not None:
            return self.client.close(reason)

        return None

    def named_queue_declare(self, *args, **kwargs):
        """
        Declare a named queue if it hasn't already been declared.

        This is a wrapper around the channel's queue_declare method to prevent
        redeclaring an existing queue.
        """
        if not self.connected:
            self.log.error("Cannot declare queue. AMQP Client is not connected.")
            return None

        qname = kwargs.get('queue')
        if qname in self.queues:
            self.log.debug('Queue [%s] already declared, skipping redeclaration.', qname)
            return None

        return self.chan.queue_declare(*args, **kwargs).addCallback(self._queue_declared)

    def _queue_declared(self, queue):
        """Called once a queue has been declared successfully."""
        self.log.info("Queue declared: [%s]", queue.queue)
        self.queues.append(queue.queue)

    def publish(self, **kwargs):
        """
        Publish a message using the channel, if connected.

        :param kwargs: Arguments for the basic_publish method.
        """
        if not self.connected:
            self.log.error("Cannot publish message. AMQP Client is not connected: %s", kwargs)
            return None
        return self.chan.basic_publish(**kwargs)

    def stopConnectionRetrying(self):
        """
        Stop the factory from attempting to reconnect.

        This is typically called when a service stop is requested.
        The `connectionRetry` flag is reset to True upon the next connect() call.
        """
        if self.reconnectTimer and self.reconnectTimer.active():
            self.reconnectTimer.cancel()
            self.reconnectTimer = None

        self.connectionRetry = False

    def disconnectAndDontRetryToConnect(self):
        """
        Disconnect and prevent any future reconnection attempts.
        """
        self.stopConnectionRetrying()
        return self.disconnect()
