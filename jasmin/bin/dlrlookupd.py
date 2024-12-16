#!/usr/bin/python3

import os
import signal
import sys
import syslog
import ntpath

from lockfile import FileLock, LockTimeout, AlreadyLocked
from twisted.internet import reactor, defer
from twisted.python import usage

from jasmin.managers.configs import DLRLookupConfig
from jasmin.managers.dlr import DLRLookup
from jasmin.queues.configs import AmqpConfig
from jasmin.queues.factory import AmqpFactory
from jasmin.redis.client import ConnectionWithConfiguration
from jasmin.redis.configs import RedisForJasminConfig
from jasmin.config import ROOT_PATH
from jasmin.bin import BaseDaemon

CONFIG_PATH = os.getenv('CONFIG_PATH', f'{ROOT_PATH}/etc/jasmin/')


class Options(usage.Options):
    """
    Command-line options for the dlrlookupd daemon.

    Options:
    -c, --config: The path to dlrlookupd.cfg configuration file.
    -i, --id: A unique daemon ID to ensure non-conflicting lockfiles.
    """
    optParameters = [
        ['config', 'c', f'{CONFIG_PATH}/dlrlookupd.cfg', 'Jasmin dlrlookupd configuration file'],
        ['id', 'i', 'master', 'Daemon ID, must be unique per dlrlookupd instance'],
    ]

    optFlags = []


class DlrlookupDaemon(BaseDaemon):
    """
    DlrlookupDaemon is responsible for:
    - Connecting to Redis (for DLR storage and lookup)
    - Connecting to the AMQP broker (for message queueing)
    - Starting the DLRLookup service

    It manages startup and shutdown procedures and handles system signals
    for graceful termination.
    """

    @defer.inlineCallbacks
    def startRedisClient(self):
        """
        Start the Redis client using configurations from the config file.
        Authenticates and selects the correct DB if credentials are provided.
        """
        RedisForJasminConfigInstance = RedisForJasminConfig(self.options['config'])
        self.components['rc'] = yield ConnectionWithConfiguration(RedisForJasminConfigInstance)

        # Authenticate and select the Redis DB if password is provided
        if RedisForJasminConfigInstance.password is not None:
            yield self.components['rc'].auth(RedisForJasminConfigInstance.password)
            yield self.components['rc'].select(RedisForJasminConfigInstance.dbid)

    def stopRedisClient(self):
        """Stop and disconnect the Redis client."""
        return self.components['rc'].disconnect()

    def startAMQPBrokerService(self):
        """Initialize and connect to the AMQP broker."""
        AMQPServiceConfigInstance = AmqpConfig(self.options['config'])
        self.components['amqp-broker-factory'] = AmqpFactory(AMQPServiceConfigInstance)
        self.components['amqp-broker-factory'].preConnect()

        self.components['amqp-broker-client'] = reactor.connectTCP(
            AMQPServiceConfigInstance.host,
            AMQPServiceConfigInstance.port,
            self.components['amqp-broker-factory']
        )

    def stopAMQPBrokerService(self):
        """Stop the AMQP broker service."""
        return self.components['amqp-broker-client'].disconnect()

    @defer.inlineCallbacks
    def startDLRLookupService(self):
        """
        Start the DLRLookup service.

        The DLRLookupConfig's log_file is adjusted to avoid conflicts with Jasmin sm-listener logs.
        """
        DLRLookupConfigInstance = DLRLookupConfig(self.options['config'])

        # Adjust log file as per #629 to avoid conflicts
        base_name = ntpath.basename(DLRLookupConfigInstance.log_file)
        dir_name = ntpath.dirname(DLRLookupConfigInstance.log_file)
        DLRLookupConfigInstance.log_file = f'{dir_name}/dlrlookupd-{base_name}'

        self.components['dlrlookup'] = DLRLookup(
            DLRLookupConfigInstance,
            self.components['amqp-broker-factory'],
            self.components['rc']
        )
        yield self.components['dlrlookup'].subscribe()

    @defer.inlineCallbacks
    def start(self):
        """Start the Dlrlookup daemon and all its services."""
        syslog.syslog(syslog.LOG_INFO, "Starting Dlrlookup Daemon ...")

        # Connect to Redis
        try:
            yield self.startRedisClient()
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, f"Cannot start RedisClient: {e}")
        else:
            syslog.syslog(syslog.LOG_INFO, "RedisClient Started.")

        # Start AMQP Broker
        try:
            self.startAMQPBrokerService()
            yield self.components['amqp-broker-factory'].getChannelReadyDeferred()
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, f"Cannot start AMQP Broker: {e}")
        else:
            syslog.syslog(syslog.LOG_INFO, "AMQP Broker Started.")

        # Start DLR Lookup service
        try:
            self.startDLRLookupService()
        except Exception as e:
            syslog.syslog(syslog.LOG_ERR, f"Cannot start DLRLookup: {e}")
        else:
            syslog.syslog(syslog.LOG_INFO, "DLRLookup Started.")

    @defer.inlineCallbacks
    def stop(self):
        """Stop the Dlrlookup daemon and all its services gracefully."""
        syslog.syslog(syslog.LOG_INFO, "Stopping Dlrlookup Daemon ...")

        if 'amqp-broker-client' in self.components:
            yield self.stopAMQPBrokerService()
            syslog.syslog(syslog.LOG_INFO, "AMQP Broker disconnected.")

        if 'rc' in self.components:
            yield self.stopRedisClient()
            syslog.syslog(syslog.LOG_INFO, "RedisClient stopped.")

        reactor.stop()

    def sighandler_stop(self, signum, frame):
        """Handle termination signals cleanly."""
        syslog.syslog(syslog.LOG_INFO, "Received signal to stop Jasmin DlrlookupDaemon")
        return self.stop()


def main():
    lock = None
    try:
        options = Options()
        options.parseOptions()

        lock_file = f"/tmp/dlrlookupd-{options['id']}"
        lock = FileLock(lock_file)
        lock.acquire(timeout=2)

        dlrlookup_daemon = DlrlookupDaemon(options)

        # Setup signal handlers
        signal.signal(signal.SIGINT, dlrlookup_daemon.sighandler_stop)
        signal.signal(signal.SIGTERM, dlrlookup_daemon.sighandler_stop)

        # Start daemon
        dlrlookup_daemon.start()
        reactor.run()

    except usage.UsageError as errortext:
        print(f'{sys.argv[0]}: {errortext}')
        print(f'{sys.argv[0]}: Try --help for usage details.')
    except LockTimeout:
        print("Lock not acquired! Exiting.")
    except AlreadyLocked:
        print("There's another instance of dlrlookupd running, exiting.")
    finally:
        if lock is not None and lock.i_am_locking():
            lock.release()


if __name__ == '__main__':
    main()
