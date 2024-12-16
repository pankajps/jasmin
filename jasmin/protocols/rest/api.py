import json
import os
import sys
import uuid
import re
from datetime import datetime
from typing import Dict, Any, Tuple, Optional

import requests
import falcon

import jasmin
from .config import (
    old_api_uri,
    http_throughput_per_worker,
    smart_qos
)
from .tasks import httpapi_send

sys.path.append(f"{os.path.dirname(os.path.abspath(jasmin.__file__))}/vendor")


class JasminHttpApiProxy:
    """
    A proxy class for calling old Jasmin httpapi endpoints.

    This class provides a uniform method (call_jasmin) to send GET requests to the old Jasmin
    HTTP API and return the results. It raises appropriate Falcon HTTP errors on connection issues.
    """

    def call_jasmin(self, url: str, params: Optional[Dict[str, Any]] = None) -> Tuple[int, str]:
        """
        Call the old Jasmin httpapi and return the (status_code, response_body).

        :param url: The endpoint on the old Jasmin httpapi to call.
        :param params: A dictionary of query parameters to include in the request.
        :return: A tuple (status_code, response_text).
        :raises falcon.HTTPInternalServerError: If a connection error or unexpected error occurs.
        """
        full_url = f"{old_api_uri}/{url}"
        try:
            response = requests.get(full_url, params=params)
        except requests.exceptions.ConnectionError as e:
            raise falcon.HTTPInternalServerError(
                title='Jasmin httpapi connection error',
                description=f"Could not connect to Jasmin httpapi ({old_api_uri}): {e}"
            )
        except Exception as e:
            raise falcon.HTTPInternalServerError(
                title='Jasmin httpapi unknown error',
                description=str(e)
            )

        return response.status_code, response.text.strip('"')


class JasminRestApi:
    """
    Base class for all REST API resources providing common functionalities:

    - Building responses from Jasmin httpapi results.
    - Decoding JSON request data.
    - Replacing underscores in keys to match old Jasmin API constraints.
    """

    def build_response_from_proxy_result(self, response: falcon.Response, result: Tuple[int, str]) -> None:
        """
        Build a unified response format from the old Jasmin httpapi call result.

        If the status code is 200, attempts to parse the response as JSON, otherwise treats it as a string.
        This method updates the Falcon response object accordingly.

        :param response: falcon.Response object to update.
        :param result: A tuple (status_code, response_text) from call_jasmin.
        """
        status_code, content = result
        parsed_content = None

        if status_code == 200:
            # Try to load as JSON
            try:
                parsed_content = json.loads(content)
            except json.JSONDecodeError:
                # Not valid JSON, treat as string
                parsed_content = content
            response.body = json.dumps({'data': parsed_content})
        else:
            # Error from Jasmin old api: return message as-is
            response.body = json.dumps({'message': content})

        response.status = getattr(falcon, f'HTTP_{status_code}', falcon.HTTP_500)

    def decode_request_data(self, request: falcon.Request) -> Dict[str, Any]:
        """
        Decode the request stream and parse as JSON.

        :param request: falcon.Request object containing the request data.
        :return: Parsed JSON as Python dictionary.
        :raises falcon.HTTPPreconditionFailed: If JSON cannot be parsed.
        """
        request_data = request.stream.read()
        try:
            params = json.loads(request_data)
        except Exception:
            raise falcon.HTTPPreconditionFailed(
                title='Cannot parse JSON data',
                description=f'Unparseable JSON data: {request_data}'
            )
        return params

    def replace_underscores_in_keys(self, param_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replace underscores with dashes in dictionary keys to comply with old Jasmin API parameter naming.

        :param param_dict: Dictionary of parameters.
        :return: New dictionary with underscores replaced by dashes in keys.
        """
        transformed = {}
        for k, v in param_dict.items():
            new_key = re.sub('_', '-', k)
            transformed[new_key] = v
        return transformed


class PingResource(JasminRestApi, JasminHttpApiProxy):
    """
    Resource for the GET /ping endpoint.

    Used to check if Jasmin's httpapi is responsive.
    """

    def on_get(self, request: falcon.Request, response: falcon.Response):
        """
        GET /ping
        Calls the Jasmin httpapi /ping endpoint and returns the result.
        """
        self.build_response_from_proxy_result(response, self.call_jasmin('ping'))


class BalanceResource(JasminRestApi, JasminHttpApiProxy):
    """
    Resource for the GET /secure/balance endpoint.

    Allows the user to check their balance.
    """

    def on_get(self, request: falcon.Request, response: falcon.Response):
        """
        GET /secure/balance
        Calls the Jasmin httpapi /balance endpoint using the user's credentials.
        """
        self.build_response_from_proxy_result(
            response,
            self.call_jasmin(
                'balance',
                params={
                    'username': request.context.get('username'),
                    'password': request.context.get('password')
                }
            )
        )


class RateResource(JasminRestApi, JasminHttpApiProxy):
    """
    Resource for the GET /secure/rate endpoint.

    Indicates the rate of the message once sent.
    """

    def on_get(self, request: falcon.Request, response: falcon.Response):
        """
        GET /secure/rate
        Calls the Jasmin httpapi /rate endpoint using the user's credentials and any additional parameters.
        """
        request_args = request.params.copy()
        request_args.update({
            'username': request.context.get('username'),
            'password': request.context.get('password')
        })

        # Replace underscores with dashes
        request_args = self.replace_underscores_in_keys(request_args)

        self.build_response_from_proxy_result(
            response,
            self.call_jasmin('rate', params=request_args)
        )


class SendResource(JasminRestApi, JasminHttpApiProxy):
    """
    Resource for the POST /secure/send endpoint.

    Calls the Jasmin http api /send resource to send a single message.
    """

    def on_post(self, request: falcon.Request, response: falcon.Response):
        """
        POST /secure/send
        Decode JSON data, add user credentials, replace underscores, and call the /send endpoint.
        """
        request_args = self.decode_request_data(request).copy()
        request_args.update({
            'username': request.context.get('username'),
            'password': request.context.get('password')
        })

        request_args = self.replace_underscores_in_keys(request_args)

        self.build_response_from_proxy_result(
            response,
            self.call_jasmin('send', params=request_args)
        )


class SendBatchResource(JasminRestApi, JasminHttpApiProxy):
    """
    Resource for the POST /secure/sendbatch endpoint.

    Allows scheduling or immediate sending of a batch of messages through the Jasmin http api.
    """

    def parse_schedule_at(self, val: Optional[str]) -> int:
        """
        Parse the schedule_at parameter and return a countdown in seconds.

        Accepted formats:
        - 'YYYY-MM-DD HH:MM:SS'
        - '<number>s' (e.g. '3600s' for 1 hour)

        :param val: The schedule_at string from the request
        :return: Countdown in seconds
        :raises falcon.HTTPPreconditionFailed: If the date is invalid or in the past.
        """
        if val is None:
            return 0

        # Try ISO datetime format
        try:
            schedule_at = datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
            if schedule_at < datetime.now():
                raise falcon.HTTPPreconditionFailed(
                    title='Cannot schedule batch in past date',
                    description=f"Invalid past date given: {schedule_at}"
                )
            return int((schedule_at - datetime.now()).total_seconds())
        except ValueError:
            # Try seconds format
            match = re.match(r"^(\d+)s$", val)
            if not match:
                raise falcon.HTTPPreconditionFailed(
                    title='Cannot parse scheduled_at value',
                    description=(
                        f"Unknown format: {val}. Valid formats are 'YYYY-MM-DD HH:MM:SS' "
                        "or a number of seconds followed by 's' (e.g. '3600s'). "
                        "Refer to http://docs.jasminsms.com/en/latest/apis/rest."
                    )
                )
            return int(match.group(1))

    def on_post(self, request: falcon.Request, response: falcon.Response):
        """
        POST /secure/sendbatch
        Schedule or send immediately a batch of messages through Jasmin http api.

        This endpoint first authenticates the user by calling the /balance endpoint.
        If authentication fails, it raises an error.

        Once authenticated, it processes the batch of messages:
        - Parse JSON payload.
        - Determine schedule time.
        - For each message, send it immediately or schedule it with Celery tasks.

        Returns a JSON response with a batchId and messageCount.
        """
        # Authenticate user first
        status, _ = self.call_jasmin('balance', params={
            'username': request.context.get('username'),
            'password': request.context.get('password')
        })
        if status != 200:
            raise falcon.HTTPPreconditionFailed(
                title='Authentication failed',
                description=f"Authentication failed for user: {request.context.get('username')}"
            )

        batch_id = uuid.uuid4()
        params = self.decode_request_data(request)
        config = {'throughput': http_throughput_per_worker, 'smart_qos': smart_qos}

        # Determine scheduling
        countdown = self.parse_schedule_at(params.get('batch_config', {}).get('schedule_at', None))

        message_count = 0
        # Iterate over messages and schedule/send them
        for msg_params in params.get('messages', {}):
            message_params = {
                'username': request.context.get('username'),
                'password': request.context.get('password')
            }
            message_params.update(params.get('globals', {}))
            message_params.update(msg_params)

            message_params = self.replace_underscores_in_keys(message_params)

            # Validate essential parameters
            if 'to' not in message_params or (
                    'content' not in message_params and 'hex-content' not in message_params
            ):
                continue

            # Handle multiple recipients
            if isinstance(message_params.get('to', ''), list):
                recipients = message_params.pop('to')
                for recipient in recipients:
                    single_msg_params = message_params.copy()
                    single_msg_params['to'] = recipient
                    if countdown == 0:
                        httpapi_send.delay(batch_id, params.get('batch_config', {}), single_msg_params, config)
                    else:
                        httpapi_send.apply_async(
                            args=[batch_id, params.get('batch_config', {}), single_msg_params, config],
                            countdown=countdown
                        )
                    message_count += 1
            else:
                # Single recipient
                if countdown == 0:
                    httpapi_send.delay(batch_id, params.get('batch_config', {}), message_params, config)
                else:
                    httpapi_send.apply_async(
                        args=[batch_id, params.get('batch_config', {}), message_params, config],
                        countdown=countdown
                    )
                message_count += 1

        # Build response
        response_data = {
            'data': {
                "batchId": str(batch_id),
                "messageCount": message_count
            }
        }
        if countdown > 0:
            response_data['data']['scheduled'] = f'{countdown}s'

        response.body = json.dumps(response_data)
        response.status = falcon.HTTP_200
