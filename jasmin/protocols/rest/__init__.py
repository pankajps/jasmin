import base64
import json
import logging
import logging.handlers
import os
import sys

import jasmin
from falcon import (
    HTTPUnauthorized,
    HTTPUnsupportedMediaType, App
)

from .api import PingResource, BalanceResource, RateResource, SendResource, SendBatchResource
from .config import (
    log_level,
    log_file,
    log_rotate,
    log_format,
    log_date_format,
    show_jasmin_version
)

sys.path.append(f"{os.path.dirname(os.path.abspath(jasmin.__file__))}/vendor")

# Configure logger
logger = logging.getLogger('jasmin-restapi')
if len(logger.handlers) == 0:
    logger.setLevel(log_level)
    handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_file,
        when=log_rotate
    )
    handler.setFormatter(logging.Formatter(log_format, log_date_format))
    logger.addHandler(handler)


class TokenNotFound(Exception):
    """Raised when an authentication token is not found or invalid."""
    pass


class JsonResponserMiddleware:
    """
    Ensures all responses are JSON-encoded and adds Jasmin version signature if enabled.
    """

    def process_response(self, request, response, resource, req_succeeded: bool):
        """
        Convert the response body to JSON if not already done and
        set `application/json` as the content type.

        Add a 'Powered-By' header if show_jasmin_version is True.
        """
        if response.content_type != 'application/json':
            response.content_type = 'application/json'

        # Encode response.body as JSON if status code is 200
        if response.status and response.status.startswith('200') and isinstance(response.body, dict):
            response.body = json.dumps(response.body)

        # Add Jasmin signature
        if show_jasmin_version:
            response.set_header('Powered-By', f'Jasmin {jasmin.get_release()}')


class LoggingMiddleware:
    """
    Logs each API call, including status, user, and request details.
    """

    def process_response(self, request, response, resource, req_succeeded: bool):
        """
        Log the request based on the response status code.
        """
        username = request.context.get('username', '*')
        ip = request.remote_addr
        method = request.method
        uri = request.relative_uri
        status_code = response.status[:3] if response.status else '???'

        if status_code == '200':
            logger.info(f'[{status_code}] {username}@{ip} {method} {uri}')
        else:
            logger.error(f'[{status_code}] {username}@{ip} {method} {uri}')


class ContentTypeFilter:
    """
    Enforces that the client accepts only JSON responses.
    """

    def process_request(self, request, response):
        """
        Check if the client accepts JSON.
        Raise HTTPUnsupportedMediaType otherwise.
        """
        if not request.client_accepts_json:
            raise HTTPUnsupportedMediaType(
                'Unsupported media type',
                'This API supports JSON media type only.',
                href='http://docs.jasminsms.com/en/latest/apis/rest/index.html'
            )


class AuthenticationFilter:
    """
    Extracts username/password from the 'Authorization' header.
    Authentication is only required for routes starting with '/secure'.
    """

    def _token_decode(self, request, token: str):
        """
        Decodes the Basic auth token and sets username/password in request.context.

        :param request: The current request object.
        :param token: The Basic auth token from the Authorization header.
        :raises HTTPUnauthorized: If the token is invalid or cannot be decoded.
        """
        try:
            token_parts = token.split(' ')
            if len(token_parts) != 2 or token_parts[0].lower() != 'basic':
                raise TokenNotFound(f'Invalid token format: {token}')

            # Decode Base64 Basic auth token
            auth_token = base64.b64decode(token_parts[1]).decode('utf-8')
            username, password = auth_token.split(':', 1)
            request.context['username'] = username
            request.context['password'] = password
        except TokenNotFound as e:
            raise HTTPUnauthorized(
                title=str(e),
                description='Please provide a valid Basic auth token',
                href='http://docs.jasminsms.com/en/latest/apis/rest/index.html'
            )
        except Exception as e:
            raise HTTPUnauthorized(
                title=f'Invalid token: {e}',
                description='Please provide a valid Basic auth token',
                href='http://docs.jasminsms.com/en/latest/apis/rest/index.html'
            )

    def process_request(self, request, response):
        """
        Checks if request path requires authentication. If so, validate the Basic auth token.
        """
        path_parts = request.path.strip('/').split('/')
        if len(path_parts) > 0 and path_parts[0] == 'secure':
            token = request.get_header('Authorization')
            if token is None:
                raise HTTPUnauthorized(
                    title='Authentication required',
                    description='Please provide a valid Basic auth token',
                    href='http://docs.jasminsms.com/en/latest/apis/rest/index.html'
                )
            self._token_decode(request, token)


logger.info('Starting Jasmin REST API ...')

api = App(
    middleware=[
        ContentTypeFilter(),
        AuthenticationFilter(),
        JsonResponserMiddleware(),
        LoggingMiddleware()
    ]
)

# Register routes
api.add_route('/ping', PingResource())
logger.info('   [OK] /ping')
api.add_route('/secure/balance', BalanceResource())
logger.info('   [OK] /secure/balance')
api.add_route('/secure/rate', RateResource())
logger.info('   [OK] /secure/rate')
api.add_route('/secure/send', SendResource())
logger.info('   [OK] /secure/send')
api.add_route('/secure/sendbatch', SendBatchResource())
logger.info('   [OK] /secure/sendbatch')

logger.info('API Started.')
