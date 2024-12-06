#!/usr/bin/env python3

import os
import signal
import sys
import logging

from lockfile import FileLock, LockTimeout, AlreadyLocked
from twisted.cred import portal
from twisted.cred.checkers import AllowAnonymousAccess, InMemoryUsernamePasswordDatabaseDontUse
from twisted.internet import reactor, defer
from twisted.python import usage
from twisted.spread import pb

from jasmin.interceptor.configs import InterceptorPBConfig
from jasmin.interceptor.interceptor import InterceptorPB
from jasmin.tools.cred.portal import JasminPBRealm
from jasmin.tools.spread.pb import JasminPBPortalRoot
from jasmin.config import ROOT_PATH
from jasmin.bin import BaseDaemon

CONFIG_PATH = os.getenv('CONFIG_PATH', f'{ROOT_PATH}/etc/jasmin/')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(process)d %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)
logger = logging.getLogger("interceptor-daemon")


class Options(usage.Options):
    optParameters = [
        ['config', 'c', f'{CONFIG_PATH}/interceptor.cfg',
         'Jasmin interceptor configuration file'],
    ]


class InterceptorDaemon(BaseDaemon):
    def startInterceptorPBService(self):
        """
        Start Interceptor PB server.
        """
        conf = InterceptorPBConfig(self.options['config'])
        self.components['interceptor-pb-factory'] = InterceptorPB(conf)

        # Set authentication portal
        p = portal.Portal(JasminPBRealm(self.components['interceptor-pb-factory']))
        if conf.authentication:
            c = InMemoryUsernamePasswordDatabaseDontUse()
            c.addUser(conf.admin_username, conf.admin_password)
            p.registerChecker(c)
        else:
            p.registerChecker(AllowAnonymousAccess())
        jpb_root = JasminPBPortalRoot(p)

        self.components['interceptor-pb-server'] = reactor.listenTCP(
            conf.port,
            pb.PBServerFactory(jpb_root),
            interface=conf.bind
        )

    def stopInterceptorPBService(self):
        """
        Stop Interceptor PB server.
        """
        return self.components['interceptor-pb-server'].stopListening()

    @defer.inlineCallbacks
    def start(self):
        """
        Start Interceptord daemon.
        """
        logger.info("Starting InterceptorPB Daemon...")

        # Start Interceptor PB server
        try:
            yield self.startInterceptorPBService()
        except Exception as e:
            logger.error(f"Cannot start Interceptor: {e}", exc_info=True)
        else:
            logger.info("Interceptor Started.")

    @defer.inlineCallbacks
    def stop(self):
        """
        Stop Interceptord daemon.
        """
        logger.info("Stopping Interceptor Daemon...")

        if 'interceptor-pb-server' in self.components:
            yield self.stopInterceptorPBService()
            logger.info("InterceptorPB stopped.")

        # Stop the reactor after services have been stopped
        reactor.stop()

    def sighandler_stop(self, signum: int, frame):
        """
        Handle stop signal cleanly.
        """
        logger.info("Received signal to stop Interceptor Daemon")
        return self.stop()


if __name__ == '__main__':
    lock = FileLock("/tmp/interceptord")

    try:
        options = Options()
        options.parseOptions()

        # Ensure no parallel runs of this script
        lock.acquire(timeout=2)

        # Prepare to start
        in_d = InterceptorDaemon(options)
        # Setup signal handlers
        signal.signal(signal.SIGINT, in_d.sighandler_stop)
        signal.signal(signal.SIGTERM, in_d.sighandler_stop)

        # Start the daemon
        in_d.start()
        reactor.run()

    except usage.UsageError as err:
        print(f"{sys.argv[0]}: {err}")
        print(f"{sys.argv[0]}: Try --help for usage details.")
        sys.exit(1)
    except LockTimeout:
        print("Could not acquire lock within timeout, exiting.")
        sys.exit(1)
    except AlreadyLocked:
        print("Another instance of interceptord is already running, exiting.")
        sys.exit(1)
    finally:
        # Release the lock if held
        if lock.i_am_locking():
            lock.release()
