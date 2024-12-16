#!/usr/bin/python3

import os
import signal
import sys
import syslog

from lockfile import FileLock, LockTimeout, AlreadyLocked
from twisted.internet import reactor, defer
from twisted.python import usage

from jasmin.protocols.smpp.configs import SMPPServerPBClientConfig
from jasmin.protocols.smpp.proxies import SMPPServerPBProxy
from jasmin.queues.configs import AmqpConfig
from jasmin.queues.factory import AmqpFactory
from jasmin.routing.configs import DLRThrowerConfig
from jasmin.routing.throwers import DLRThrower
from jasmin.config import ROOT_PATH
from jasmin.bin import BaseDaemon

CONFIG_PATH = os.getenv('CONFIG_PATH', f'{ROOT_PATH}/etc/jasmin/')


class Options(usage.Options):
    """
    Command-line options for the DlrDaemon.

    Options:
    -c, --config: The path to the dlr.cfg configuration file.
    -i, --id: Daemon identifier, used for lockfile naming.
    """
    optParameters = [
        ['config', 'c', f'{CONFIG_PATH}/dlr.cfg', 'Jasmin dlrd configuration file'],
        ['id', 'i', 'master', 'Daemon ID, must be unique per dlrd instance'],
    ]


class DlrDaemon(BaseDaemon):
    """
    DlrDaemon manages the lifecycle of the DLR routing services, including:
    - AMQP Broker connection
    - SMPPServerPB client
    - DLRThrower service

    It sets up signal handlers and provides start/stop logic for each component.
    """

    def startAMQPBrokerService(self):
        """Initialize and start the AMQP Broker service."""
        AMQPServiceConfigInstance = AmqpConfig(self.options['config'])
        self.components['amqp-broker-factory'] = AmqpFactory(AMQPServiceConfigInstance)
        self.components['amqp-broker-factory'].preConnect()

        self.components['amqp-broker-client'] = reactor.connectTCP(
            AMQPServiceConfigInstance.host,
            AMQPServiceConfigInstance.port,
            self.components['amqp-broker-factory']
        )

    def stopAMQPBrokerService(self):
        """Stop the AMQP Broker service."""
        return self.components['amqp-broker-client'].disconnect()

    def startSMPPServerPBClient(self):
        """Connect to the SMPPServerPB."""
        SMPPServerPBClientConfigInstance = SMPPServerPBClientConfig(self.options['config'])
        self.components['smpps-pb-client'] = SMPPServerPBProxy()

        return self.components['smpps-pb-client'].connect(
            SMPPServerPBClientConfigInstance.host,
            SMPPServerPBClientConfigInstance.port,
            SMPPServerPBClientConfigInstance.username,
            SMPPServerPBClientConfigInstance.password,
            retry=True
        )

    def stopSMPPServerPBClient(self):
        """Disconnect from the SMPPServerPB."""
        if self.components['smpps-pb-client'].isConnected:
            return self.components['smpps-pb-client'].disconnect()

    def startDLRThrowerService(self):
        """Initialize and start the DLRThrower service."""
        DLRThrowerConfigInstance = DLRThrowerConfig(self.options['config'])
        self.components['dlr-thrower'] = DLRThrower(DLRThrowerConfigInstance)
        self.components['dlr-thrower'].addSmpps(self.components['smpps-pb-client'])

        # AMQP Broker is used to listen to the DLRThrower queue
        return self.components['dlr-thrower'].addAmqpBroker(self.components['amqp-broker-factory'])

    def stopDLRThrowerService(self):
        """Stop the DLRThrower service."""
        return self.components['dlr-thrower'].stopService()

    @defer.inlineCallbacks
    def start(self):
        """Start DlrDaemon by initializing all services."""
        syslog.syslog(syslog.LOG_INFO, "Starting Dlr Daemon ...")

        # Start AMQP Broker
        try:
            self.startAMQPBrokerService()
            yield self.components['amqp-broker-factory'].getChannelReadyDeferred()
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, f"Cannot start AMQP Broker: {e}")
        else:
            syslog.syslog(syslog.LOG_INFO, "AMQP Broker Started.")

        # Start SMPPServerPB Client
        try:
            yield self.startSMPPServerPBClient()
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, f"Cannot start SMPPServerPBClient: {e}")
        else:
            syslog.syslog(syslog.LOG_INFO, "SMPPServerPBClient Started.")

        # Start DLRThrower
        try:
            yield self.startDLRThrowerService()
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, f"Cannot start DLRThrower: {e}")
        else:
            syslog.syslog(syslog.LOG_INFO, "DLRThrower Started.")

    @defer.inlineCallbacks
    def stop(self):
        """Stop all services and gracefully shut down."""
        syslog.syslog(syslog.LOG_INFO, "Stopping Dlr Daemon ...")

        if 'smpps-pb-client' in self.components:
            yield self.stopSMPPServerPBClient()
            syslog.syslog(syslog.LOG_INFO, "SMPPServerPBClient stopped.")

        if 'dlr-thrower' in self.components:
            yield self.stopDLRThrowerService()
            syslog.syslog(syslog.LOG_INFO, "DLRThrower stopped.")

        if 'amqp-broker-client' in self.components:
            yield self.stopAMQPBrokerService()
            syslog.syslog(syslog.LOG_INFO, "AMQP Broker disconnected.")

        reactor.stop()

    def sighandler_stop(self, signum, frame):
        """Handle stop signals cleanly."""
        syslog.syslog(syslog.LOG_INFO, "Received signal to stop Dlr Daemon")
        return self.stop()


def main():
    lock = None
    try:
        options = Options()
        options.parseOptions()

        lock_file = f"/tmp/dlrd-{options['id']}"
        lock = FileLock(lock_file)
        lock.acquire(timeout=2)

        dlr_d = DlrDaemon(options)

        # Setup signal handlers
        signal.signal(signal.SIGINT, dlr_d.sighandler_stop)
        signal.signal(signal.SIGTERM, dlr_d.sighandler_stop)

        # Start DlrDaemon
        dlr_d.start()
        reactor.run()

    except usage.UsageError as errortext:
        print(f'{sys.argv[0]}: {errortext}')
        print(f'{sys.argv[0]}: Try --help for usage details.')
    except LockTimeout:
        print("Lock not acquired! Exiting.")
    except AlreadyLocked:
        print("Another instance of dlrd is already running, exiting.")
    finally:
        # Release the lock if held
        if lock is not None and lock.i_am_locking():
            lock.release()


if __name__ == '__main__':
    main()
