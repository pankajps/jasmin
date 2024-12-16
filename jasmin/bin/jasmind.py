#!/usr/bin/env python3

import os
import signal
import sys
import traceback
import logging
from typing import Optional

from lockfile import FileLock, LockTimeout, AlreadyLocked
from twisted.cred import portal
from twisted.cred.checkers import AllowAnonymousAccess, InMemoryUsernamePasswordDatabaseDontUse
from twisted.internet import reactor, defer
from twisted.python import usage
from twisted.spread import pb
from twisted.web import server

from jasmin.interceptor.configs import InterceptorPBClientConfig
from jasmin.interceptor.proxies import InterceptorPBProxy
from jasmin.managers.clients import SMPPClientManagerPB
from jasmin.managers.configs import SMPPClientPBConfig, DLRLookupConfig
from jasmin.managers.dlr import DLRLookup
from jasmin.protocols.cli.configs import JCliConfig
from jasmin.protocols.cli.factory import JCliFactory
from jasmin.protocols.http.configs import HTTPApiConfig
from jasmin.protocols.http.server import HTTPApi
from jasmin.protocols.smpp.configs import SMPPServerConfig, SMPPServerPBConfig
from jasmin.protocols.smpp.factory import SMPPServerFactory
from jasmin.protocols.smpp.pb import SMPPServerPB
from jasmin.queues.configs import AmqpConfig
from jasmin.queues.factory import AmqpFactory
from jasmin.redis.client import ConnectionWithConfiguration
from jasmin.redis.configs import RedisForJasminConfig
from jasmin.routing.configs import RouterPBConfig, deliverSmThrowerConfig, DLRThrowerConfig
from jasmin.routing.router import RouterPB
from jasmin.routing.throwers import deliverSmThrower, DLRThrower
from jasmin.tools.cred.checkers import RouterAuthChecker
from jasmin.tools.cred.portal import JasminPBRealm, SmppsRealm
from jasmin.tools.spread.pb import JasminPBPortalRoot
from jasmin.config import ROOT_PATH
from jasmin.bin import BaseDaemon

CONFIG_PATH = os.getenv('CONFIG_PATH', f'{ROOT_PATH}/etc/jasmin/')
LOG_CATEGORY = "jasmin-daemon"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(process)d %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)


class Options(usage.Options):
    """
    Command line options for the Jasmin Daemon.
    """
    optParameters = [
        ['config', 'c', f'{CONFIG_PATH}/jasmin.cfg', 'Jasmin configuration file'],
        ['username', 'u', None, 'jCli username to load configuration profile on startup'],
        ['password', 'p', None, 'jCli password to load configuration profile on startup'],
    ]

    optFlags = [
        ['disable-smpp-server', None, 'Do not start the SMPP Server service'],
        ['enable-dlr-thrower', None, 'Enable DLR Thrower service (not recommended, use dlrd daemon instead)'],
        ['enable-dlr-lookup', None, 'Enable DLR Lookup service (not recommended, use dlrlookupd daemon instead)'],
        ['disable-deliver-thrower', None, 'Do not start the DeliverSm Thrower service'],
        ['disable-http-api', None, 'Do not start the HTTP API'],
        ['disable-jcli', None, 'Do not start the jCli console'],
        ['enable-interceptor-client', None, 'Start the Interceptor client service'],
    ]


class JasminDaemon(BaseDaemon):
    """
    Jasmin Daemon main class.

    Handles the initialization and shutdown procedures for all Jasmin services
    including SMPP server, HTTP API, AMQP, Redis, Router, and various supporting
    services.

    Services are dynamically started or stopped based on the provided command line options.
    """

    def __init__(self, opt):
        super().__init__(opt)
        self.log = logging.getLogger(LOG_CATEGORY)
        self.log.propagate = False  # Already configured globally
        self.components = {}  # Holds references to started components

    @defer.inlineCallbacks
    def startRedisClient(self):
        """Connect and initialize the Redis client."""
        redis_config = RedisForJasminConfig(self.options['config'])
        self.components['rc'] = yield ConnectionWithConfiguration(redis_config)
        # Authenticate and select db if password is set
        if redis_config.password is not None:
            yield self.components['rc'].auth(redis_config.password)
            yield self.components['rc'].select(redis_config.dbid)

    def stopRedisClient(self):
        """Disconnect the Redis client."""
        return self.components['rc'].disconnect()

    def startAMQPBrokerService(self):
        """Start AMQP Broker service."""
        amqp_conf = AmqpConfig(self.options['config'])
        self.components['amqp-broker-factory'] = AmqpFactory(amqp_conf)
        self.components['amqp-broker-factory'].preConnect()

        # Connect to AMQP broker
        self.components['amqp-broker-client'] = reactor.connectTCP(
            amqp_conf.host,
            amqp_conf.port,
            self.components['amqp-broker-factory']
        )

    def stopAMQPBrokerService(self):
        """Stop AMQP Broker service."""
        return self.components['amqp-broker-client'].disconnect()

    def startRouterPBService(self):
        """Start RouterPB server."""
        router_conf = RouterPBConfig(self.options['config'])
        self.components['router-pb-factory'] = RouterPB(router_conf)

        # Authentication portal for RouterPB
        p = portal.Portal(JasminPBRealm(self.components['router-pb-factory']))
        if router_conf.authentication:
            c = InMemoryUsernamePasswordDatabaseDontUse()
            c.addUser(router_conf.admin_username, router_conf.admin_password)
            p.registerChecker(c)
        else:
            p.registerChecker(AllowAnonymousAccess())

        jpb_root = JasminPBPortalRoot(p)
        self.components['router-pb-server'] = reactor.listenTCP(
            router_conf.port,
            pb.PBServerFactory(jpb_root),
            interface=router_conf.bind
        )

        # Add AMQP broker to router
        return self.components['router-pb-factory'].addAmqpBroker(self.components['amqp-broker-factory'])

    def stopRouterPBService(self):
        """Stop RouterPB server."""
        return self.components['router-pb-server'].stopListening()

    def startSMPPClientManagerPBService(self):
        """Start SMPP Client Manager PB server."""
        smpp_client_conf = SMPPClientPBConfig(self.options['config'])
        self.components['smppcm-pb-factory'] = SMPPClientManagerPB(smpp_client_conf)

        # Authentication portal for SMPPClientManagerPB
        p = portal.Portal(JasminPBRealm(self.components['smppcm-pb-factory']))
        if smpp_client_conf.authentication:
            c = InMemoryUsernamePasswordDatabaseDontUse()
            c.addUser(smpp_client_conf.admin_username, smpp_client_conf.admin_password)
            p.registerChecker(c)
        else:
            p.registerChecker(AllowAnonymousAccess())
        jpb_root = JasminPBPortalRoot(p)

        self.components['smppcm-pb-server'] = reactor.listenTCP(
            smpp_client_conf.port,
            pb.PBServerFactory(jpb_root),
            interface=smpp_client_conf.bind
        )

        # Add AMQP, Redis, RouterPB, and possibly InterceptorPBClient
        self.components['smppcm-pb-factory'].addAmqpBroker(self.components['amqp-broker-factory'])
        self.components['smppcm-pb-factory'].addRedisClient(self.components['rc'])
        self.components['smppcm-pb-factory'].addRouterPB(self.components['router-pb-factory'])
        if 'interceptor-pb-client' in self.components:
            self.components['smppcm-pb-factory'].addInterceptorPBClient(self.components['interceptor-pb-client'])

    def stopSMPPClientManagerPBService(self):
        """Stop SMPP Client Manager PB server."""
        return self.components['smppcm-pb-server'].stopListening()

    @defer.inlineCallbacks
    def startDLRLookupService(self):
        """Start DLRLookup service."""
        dlr_conf = DLRLookupConfig(self.options['config'])
        self.components['dlrlookup'] = DLRLookup(dlr_conf, self.components['amqp-broker-factory'], self.components['rc'])
        yield self.components['dlrlookup'].subscribe()

    def startSMPPServerPBService(self):
        """Start SMPP Server PB service."""
        smpp_server_pb_conf = SMPPServerPBConfig(self.options['config'])
        self.components['smpps-pb-factory'] = SMPPServerPB(smpp_server_pb_conf)

        p = portal.Portal(JasminPBRealm(self.components['smpps-pb-factory']))
        if smpp_server_pb_conf.authentication:
            c = InMemoryUsernamePasswordDatabaseDontUse()
            c.addUser(smpp_server_pb_conf.admin_username, smpp_server_pb_conf.admin_password)
            p.registerChecker(c)
        else:
            p.registerChecker(AllowAnonymousAccess())
        jpb_root = JasminPBPortalRoot(p)

        self.components['smpps-pb-server'] = reactor.listenTCP(
            smpp_server_pb_conf.port,
            pb.PBServerFactory(jpb_root),
            interface=smpp_server_pb_conf.bind
        )

    def stopSMPPServerPBService(self):
        """Stop SMPP Server PB."""
        return self.components['smpps-pb-server'].stopListening()

    def startSMPPServerService(self):
        """Start SMPP Server."""
        smpp_server_conf = SMPPServerConfig(self.options['config'])

        # SMPP auth portal
        p = portal.Portal(
            SmppsRealm(smpp_server_conf.id, self.components['router-pb-factory'])
        )
        p.registerChecker(RouterAuthChecker(self.components['router-pb-factory']))

        self.components['smpp-server-factory'] = SMPPServerFactory(
            smpp_server_conf,
            auth_portal=p,
            RouterPB=self.components['router-pb-factory'],
            SMPPClientManagerPB=self.components['smppcm-pb-factory']
        )

        self.components['smpp-server'] = reactor.listenTCP(
            smpp_server_conf.port,
            self.components['smpp-server-factory'],
            interface=smpp_server_conf.bind
        )

        # Interceptor if enabled
        if 'interceptor-pb-client' in self.components:
            self.components['smpp-server-factory'].addInterceptorPBClient(
                self.components['interceptor-pb-client']
            )

    def stopSMPPServerService(self):
        """Stop SMPP Server."""
        return self.components['smpp-server'].stopListening()

    @defer.inlineCallbacks
    def startdeliverSmThrowerService(self):
        """Start DeliverSm Thrower service."""
        deliver_conf = deliverSmThrowerConfig(self.options['config'])
        self.components['deliversm-thrower'] = deliverSmThrower(deliver_conf)
        self.components['deliversm-thrower'].addSmpps(self.components['smpp-server-factory'])
        yield self.components['deliversm-thrower'].addAmqpBroker(self.components['amqp-broker-factory'])

    def stopdeliverSmThrowerService(self):
        """Stop DeliverSm Thrower service."""
        return self.components['deliversm-thrower'].stopService()

    @defer.inlineCallbacks
    def startDLRThrowerService(self):
        """Start DLR Thrower service."""
        dlr_thrower_conf = DLRThrowerConfig(self.options['config'])
        self.components['dlr-thrower'] = DLRThrower(dlr_thrower_conf)
        self.components['dlr-thrower'].addSmpps(self.components['smpp-server-factory'])
        yield self.components['dlr-thrower'].addAmqpBroker(self.components['amqp-broker-factory'])

    def stopDLRThrowerService(self):
        """Stop DLR Thrower service."""
        return self.components['dlr-thrower'].stopService()

    def startHTTPApiService(self):
        """Start the HTTP API service."""
        http_conf = HTTPApiConfig(self.options['config'])
        interceptorpb_client = self.components.get('interceptor-pb-client', None)

        self.components['http-api-factory'] = HTTPApi(
            self.components['router-pb-factory'],
            self.components['smppcm-pb-factory'],
            http_conf,
            interceptorpb_client
        )

        self.components['http-api-server'] = reactor.listenTCP(
            http_conf.port,
            server.Site(self.components['http-api-factory'], logPath=http_conf.access_log),
            interface=http_conf.bind
        )

    def stopHTTPApiService(self):
        """Stop HTTP API service."""
        return self.components['http-api-server'].stopListening()

    def startJCliService(self):
        """Start the jCli console server."""
        loadConfigProfileWithCreds = {
            'username': self.options['username'],
            'password': self.options['password']
        }
        jcli_conf = JCliConfig(self.options['config'])
        jcli_factory = JCliFactory(
            jcli_conf,
            self.components['smppcm-pb-factory'],
            self.components['router-pb-factory'],
            self.components['smpp-server-factory'],
            loadConfigProfileWithCreds
        )

        self.components['jcli-server'] = reactor.listenTCP(
            jcli_conf.port,
            jcli_factory,
            interface=jcli_conf.bind
        )

    def stopJCliService(self):
        """Stop the jCli console server."""
        return self.components['jcli-server'].stopListening()

    def startInterceptorPBClient(self):
        """Start Interceptor client."""
        interceptor_conf = InterceptorPBClientConfig(self.options['config'])
        self.components['interceptor-pb-client'] = InterceptorPBProxy()
        return self.components['interceptor-pb-client'].connect(
            interceptor_conf.host,
            interceptor_conf.port,
            interceptor_conf.username,
            interceptor_conf.password,
            retry=True
        )

    def stopInterceptorPBClient(self):
        """Stop Interceptor client."""
        if self.components['interceptor-pb-client'].isConnected:
            return self.components['interceptor-pb-client'].disconnect()

    @defer.inlineCallbacks
    def start(self):
        """Start all Jasmin Daemon services based on the given options."""
        self.log.info("Starting Jasmin Daemon ...")

        # Optional interceptor client
        if self.options['enable-interceptor-client']:
            try:
                yield self.startInterceptorPBClient()
            except Exception as e:
                self.log.error(f"Cannot connect to interceptor: {e}\n{traceback.format_exc()}")
            else:
                self.log.info("Interceptor client Started.")

        # Redis client
        try:
            yield self.startRedisClient()
        except Exception as e:
            self.log.error(f"Cannot start RedisClient: {e}\n{traceback.format_exc()}")
        else:
            self.log.info("RedisClient Started.")

        # AMQP Broker
        try:
            yield self.startAMQPBrokerService()
            yield self.components['amqp-broker-factory'].getChannelReadyDeferred()
        except Exception as e:
            self.log.error(f"Cannot start AMQP Broker: {e}\n{traceback.format_exc()}")
        else:
            self.log.info("AMQP Broker Started.")

        # RouterPB
        try:
            yield self.startRouterPBService()
        except Exception as e:
            self.log.error(f"Cannot start RouterPB: {e}\n{traceback.format_exc()}")
        else:
            self.log.info("RouterPB Started.")

        # SMPPClientManagerPB
        try:
            self.startSMPPClientManagerPBService()
        except Exception as e:
            self.log.error(f"Cannot start SMPPClientManagerPB: {e}\n{traceback.format_exc()}")
        else:
            self.log.info("SMPPClientManagerPB Started.")

        # DLR Lookup
        if self.options['enable-dlr-lookup']:
            try:
                self.startDLRLookupService()
            except Exception as e:
                self.log.error(f"Cannot start DLRLookup: {e}\n{traceback.format_exc()}")
            else:
                self.log.info("DLRLookup Started.")

        # SMPP Server
        if not self.options['disable-smpp-server']:
            try:
                self.startSMPPServerService()
            except Exception as e:
                self.log.error(f"Cannot start SMPPServer: {e}\n{traceback.format_exc()}")
            else:
                self.log.info("SMPPServer Started.")

            try:
                self.startSMPPServerPBService()
                self.components['smpps-pb-factory'].addSmpps(self.components['smpp-server-factory'])
            except Exception as e:
                self.log.error(f"Cannot start SMPPServerPB: {e}\n{traceback.format_exc()}")
            else:
                self.log.info("SMPPServerPB Started.")

        # deliverSmThrower
        if not self.options['disable-deliver-thrower']:
            try:
                yield self.startdeliverSmThrowerService()
            except Exception as e:
                self.log.error(f"Cannot start deliverSmThrower: {e}\n{traceback.format_exc()}")
            else:
                self.log.info("deliverSmThrower Started.")

        # DLRThrower
        if self.options['enable-dlr-thrower']:
            try:
                yield self.startDLRThrowerService()
            except Exception as e:
                self.log.error(f"Cannot start DLRThrower: {e}\n{traceback.format_exc()}")
            else:
                self.log.info("DLRThrower Started.")

        # HTTPApi
        if not self.options['disable-http-api']:
            try:
                self.startHTTPApiService()
            except Exception as e:
                self.log.error(f"Cannot start HTTPApi: {e}\n{traceback.format_exc()}")
            else:
                self.log.info("HTTPApi Started.")

        # jCli
        if not self.options['disable-jcli']:
            try:
                self.startJCliService()
            except Exception as e:
                self.log.error(f"Cannot start jCli: {e}\n{traceback.format_exc()}")
            else:
                self.log.info("jCli Started.")

    @defer.inlineCallbacks
    def stop(self):
        """Stop all Jasmin Daemon services cleanly."""
        self.log.info("Stopping Jasmin Daemon ...")

        if 'jcli-server' in self.components:
            yield self.stopJCliService()
            self.log.info("jCli stopped.")

        if 'http-api-server' in self.components:
            yield self.stopHTTPApiService()
            self.log.info("HTTPApi stopped.")

        if 'dlr-thrower' in self.components:
            yield self.stopDLRThrowerService()
            self.log.info("DLRThrower stopped.")

        if 'deliversm-thrower' in self.components:
            yield self.stopdeliverSmThrowerService()
            self.log.info("deliverSmThrower stopped.")

        if 'smpps-pb-server' in self.components:
            yield self.stopSMPPServerPBService()
            self.log.info("SMPPServerPB stopped.")

        if 'smpp-server' in self.components:
            yield self.stopSMPPServerService()
            self.log.info("SMPPServer stopped.")

        if 'smppcm-pb-server' in self.components:
            yield self.stopSMPPClientManagerPBService()
            self.log.info("SMPPClientManagerPB stopped.")

        if 'router-pb-server' in self.components:
            yield self.stopRouterPBService()
            self.log.info("RouterPB stopped.")

        if 'amqp-broker-client' in self.components:
            yield self.stopAMQPBrokerService()
            self.log.info("AMQP Broker disconnected.")

        if 'rc' in self.components:
            yield self.stopRedisClient()
            self.log.info("RedisClient stopped.")

        if 'interceptor-pb-client' in self.components:
            yield self.stopInterceptorPBClient()
            self.log.info("Interceptor client stopped.")

        reactor.stop()

    def sighandler_stop(self, signum: int, frame: Optional[object]):
        """
        Handle system signals to stop Jasmin Daemon cleanly.
        """
        self.log.info("Received signal to stop Jasmin Daemon")
        return self.stop()


if __name__ == '__main__':
    lock = None
    try:
        options = Options()
        options.parseOptions()

        lock = FileLock("/tmp/jasmind")
        lock.acquire(timeout=2)

        # Prepare to start
        ja_d = JasminDaemon(options)
        # Setup signal handlers
        signal.signal(signal.SIGINT, ja_d.sighandler_stop)
        signal.signal(signal.SIGTERM, ja_d.sighandler_stop)

        ja_d.start()
        reactor.run()

    except usage.UsageError as errortext:
        print(f"{sys.argv[0]}: {errortext}")
        print(f"{sys.argv[0]}: Try --help for usage details.")
        sys.exit(1)
    except LockTimeout:
        print("Could not acquire lock within timeout, exiting.")
        sys.exit(1)
    except AlreadyLocked:
        print("Another instance of jasmind is already running, exiting.")
        sys.exit(1)
    finally:
        # Release the lock if held
        if lock is not None and lock.i_am_locking():
            lock.release()
