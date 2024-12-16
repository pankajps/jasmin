import pickle
import datetime as dt
import time
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

from twisted.spread import pb
from jasmin.tools.eval import CompiledNode

LOG_CATEGORY = "jasmin-interceptor-pb"


class InterceptorPB(pb.Avatar):
    def __init__(self, InterceptorPBConfig):
        """
        Initialize the InterceptorPB instance.
        :param InterceptorPBConfig: Configuration object for the interceptor.
        """
        self.config = InterceptorPBConfig
        self.avatar = None

        # Set up logging
        self.log = self._setup_logger()
        self.log.info('Interceptor configured and ready.')

    def _setup_logger(self):
        """
        Configure and return a logger instance.
        """
        logger = logging.getLogger(LOG_CATEGORY)
        if not logger.handlers:
            logger.setLevel(self.config.log_level)
            handler = (logging.StreamHandler(sys.stdout)
                       if 'stdout' in self.config.log_file
                       else TimedRotatingFileHandler(
                filename=self.config.log_file, when=self.config.log_rotate))
            formatter = logging.Formatter(self.config.log_format, self.config.log_date_format)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False
        return logger

    def setAvatar(self, avatar):
        """
        Set the avatar for the current session.
        :param avatar: Avatar identifier.
        """
        if isinstance(avatar, str):
            self.log.info('Authenticated Avatar: %s', avatar)
        else:
            self.log.info('Anonymous connection')
        self.avatar = avatar

    def perspective_run_script(self, pyCode, routable):
        """
        Execute Python code with the provided routable.
        :param pyCode: Python code to execute.
        :param routable: Serialized routable object.
        :return: Execution result or error statuses.
        """
        try:
            # Deserialize the routable object
            routable = pickle.loads(routable)
        except (pickle.PickleError, TypeError) as e:
            self.log.error('Failed to deserialize routable: %s', e, exc_info=True)
            return False

        smpp_status = None
        http_status = None

        try:
            self.log.info('Executing script with routable (from:%s, to:%s).',
                          routable.pdu.params['source_addr'],
                          routable.pdu.params['destination_addr'])

            # Compile and execute the Python code
            node = CompiledNode().get(pyCode)
            glo = {'routable': routable, 'smpp_status': smpp_status, 'http_status': http_status, 'extra': {}}

            start = time.perf_counter()
            eval(node, {}, glo)
            end = time.perf_counter()
            delay = end - start

            self.log.debug('Execution completed in %.2f seconds.', delay)

            # Log slow script execution
            if 0 <= self.config.log_slow_script <= delay:
                self.log.warning('Execution delay [%.2f seconds] for script [%s].', delay, pyCode)
        except Exception as e:
            self.log.error('Error executing script: %s', e, exc_info=True)
            return False

        # Handle status codes
        return self._handle_status_codes(glo, pyCode)

    def _handle_status_codes(self, glo, pyCode):
        """
        Process and validate SMPP and HTTP status codes.
        :param glo: Global execution context.
        :param pyCode: Python code executed.
        :return: Serialized routable or status response.
        """
        if glo['smpp_status'] is None and glo['http_status'] is None:
            return pickle.dumps(glo['routable'], pickle.HIGHEST_PROTOCOL)

        if glo['smpp_status'] is None or not isinstance(glo['smpp_status'], int):
            self.log.info('Setting smpp_status to 255 for script [%s].', pyCode)
            glo['smpp_status'] = 255

        if glo['http_status'] is None or not isinstance(glo['http_status'], int):
            self.log.info('Setting http_status to 520 for script [%s].', pyCode)
            glo['http_status'] = 520

        response = {'http_status': glo['http_status'], 'smpp_status': glo['smpp_status'], 'extra': glo['extra']}
        self.log.info('Returning statuses: %s', response)
        return response
