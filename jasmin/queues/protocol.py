from txamqp.protocol import AMQClient


class AmqpProtocol(AMQClient):
    def connectionMade(self):
        """
        Called when a TCP connection to the AMQP broker has been established.

        This method logs the connection details, then calls the parent class's
        connectionMade to initialize the AMQP communication. Finally, it
        triggers the factory's connectDeferred to signal that a connection
        is successfully made.
        """
        self.factory.log.info(f"Connection made to {self.factory.config.host}:{self.factory.config.port}")
        super().connectionMade()

        # Signal that the connection has been established
        self.factory.connectDeferred.callback(self)
