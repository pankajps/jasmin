import pickle
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

from twisted.internet import defer
from twisted.spread import pb

import jasmin

LOG_CATEGORY = "jasmin-smpps-pb"


class SMPPServerPB(pb.Avatar):
    def __init__(self, SmppServerPBConfig):
        self.config = SmppServerPBConfig
        self.avatar = None
        self.smpps = None
        self.log = self._setup_logger()
        self.log.info('SmppServerPB configured and ready.')

    def _setup_logger(self):
        """Set up and return a dedicated logger instance."""
        log = logging.getLogger(LOG_CATEGORY)
        if not log.handlers:
            log.setLevel(self.config.log_level)

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
            log.addHandler(handler)
            log.propagate = False
        return log

    def setAvatar(self, avatar):
        """Set the avatar (authenticated user) for the current connection."""
        if isinstance(avatar, str):
            self.log.info('Authenticated Avatar: %s', avatar)
        else:
            self.log.info('Anonymous connection')
        self.avatar = avatar

    def addSmpps(self, smppsFactory):
        """Associate or replace the SMPP server instance."""
        message = 'Added' if self.smpps is None else 'Replaced'
        self.log.info('%s SMPP Server: %s', message, smppsFactory.config.id)
        self.smpps = smppsFactory

    def perspective_list_bound_systemids(self):
        """Return a list of currently bound SMPP system IDs."""
        return list(self.smpps.bound_connections.keys()) if self.smpps else []

    @defer.inlineCallbacks
    def perspective_deliverer_send_request(self, system_id, pdu, pickled=True):
        """
        Forward a PDU to the appropriate deliverer for the given system_id.
        If pickled is True, the PDU is unpickled first.
        """
        bound_connection = self.smpps.bound_connections.get(system_id) if self.smpps else None
        deliverer = bound_connection.getNextBindingForDelivery() if bound_connection else None

        if deliverer is None:
            self.log.error('Found no deliverer for system_id %s', system_id)
            defer.returnValue(False)

        if pickled:
            pdu = pickle.loads(pdu)

        try:
            yield deliverer.sendRequest(pdu, deliverer.config().responseTimerSecs)
        except Exception as e:
            self.log.exception('Error while sending PDU through deliverer (system_id: %s): %s', system_id, e)
            defer.returnValue(False)
        else:
            defer.returnValue(True)

    def perspective_version_release(self):
        """Return the current Jasmin release version."""
        return jasmin.get_release()

    def perspective_version(self):
        """Return the current Jasmin version."""
        return jasmin.get_version()
