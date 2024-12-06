import json
import os
import sys
import uuid
import re
from datetime import datetime

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
    """A proxy class to call the old Jasmin httpapi endpoints."""

    def call_jasmin(self, url: str, params: dict = None) -> (int, str):
        """Call the old Jasmin httpapi and return (status_code, response_body)"""
        full_url = f"{old_api_uri}/{url}"
        try:
            response = requests.get(full_url, params=params)
        except requests.exceptions.ConnectionError as e:
            raise falcon.HTTPInternalServerError(
                title='Jasmin httpapi connection error',
                description=f"Could not connect to Jasmin http api ({old_api_uri}): {e}"
            )
        except Exception as e:
            raise falcon.HTTPInternalServerError(
                title='Jasmin httpapi unknown error',
                description=str(e)
            )

        return response.status_code, response.text.strip('"')


class JasminRestApi:
    """Base class for all REST API resources providing common functionalities."""

    def build_response_from_proxy_result(self, response: falcon.Response, result: tuple):
        """
        Build a unified response format from the old Jasmin httpapi call result.

        :param response: falcon.Response object
        :param result: A tuple (status_code, response_text)
        """
        status_code, content = result
        # Attempt to parse content as JSON if possible
        parsed_content = None
        if status_code == 200:
            # Try to load as JSON
            try:
                parsed_content = json.loads(content)
            except json.JSONDecodeError:
                # Not a valid JSON, treat it as a string response
                parsed_content = content
            response.body = json.dumps({'data': parsed_content})
        else:
            # Error from Jasmin old api: directly send message as is
            response.body = json.dumps({'message': content})

        response.status = getattr(falcon, f'HTTP_{status_code}', falcon.HTTP_500)

    def decode_request_data(self, request: falcon.Request) -> dict:
        """
        Decode the request stream and return a valid JSON dictionary.

        :param request: falcon.Request object
        :return: Decoded JSON as Python dict
        :raises falcon.HTTPPreconditionFailed: If the JSON cannot be parsed
        """
        request_data = request.stream.read()
        try:
            params = json.loads(request_data)
        except Exception:
            raise falcon.HTTPPreconditionFailed(
                title='Cannot parse JSON data',
                description=f'Got unparseable json data: {request_data}'
            )
        return params

    def replace_underscores_in_keys(self, param_dict: dict) -> dict:
        """
        Replace underscores with dashes in keys to comply with old Jasmin API constraints.

        :param param_dict: Dictionary of parameters
        :return: New dictionary with transformed keys
        """
        transformed = {}
        for k, v in param_dict.items():
            new_key = re.sub('_', '-', k)
            transformed[new_key] = v
        return transformed


class PingResource(JasminRestApi, JasminHttpApiProxy):
    def on_get(self, request: falcon.Request, response: falcon.Response):
        """
        GET /ping request:
        Check if Jasmin's httpapi is responsive.
        """
        self.build_response_from_proxy_result(response, self.call_jasmin('ping'))


class BalanceResource(JasminRestApi, JasminHttpApiProxy):
    def on_get(self, request: falcon.Request, response: falcon.Response):
        """
        GET /secure/balance request:
        Allows the user to check their balance.
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
    def on_get(self, request: falcon.Request, response: falcon.Response):
        """
        GET /secure/rate request:
        Indicates the rate of the message once sent.
        """
        request_args = request.params.copy()
        request_args.update({
            'username': request.context.get('username'),
            'password': request.context.get('password')
        })

        # Replace underscores with dashes in keys
        request_args = self.replace_underscores_in_keys(request_args)

        self.build_response_from_proxy_result(
            response,
            self.call_jasmin('rate', params=request_args)
        )


class SendResource(JasminRestApi, JasminHttpApiProxy):
    def on_post(self, request: falcon.Request, response: falcon.Response):
        """
        POST /secure/send request:
        Calls Jasmin http api /send resource.
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
    def parse_schedule_at(self, val: str) -> int:
        """
        Parse the schedule_at parameter and return a countdown in seconds.

        Accepted formats:
        - 'YYYY-MM-DD HH:MM:SS'
        - '<number>s' (e.g. '3600s' for 1 hour)

        :param val: The schedule_at string from the request
        :return: Countdown in seconds
        :raises falcon.HTTPPreconditionFailed: if the date is invalid or in the past
        """
        if val is None:
            return 0

        # Try ISO format first
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
                        f"Got unknown format: {val}, correct formats are 'YYYY-MM-DD HH:MM:SS' "
                        "or number of seconds followed by 's', e.g. '3600s'. "
                        "Refer to http://docs.jasminsms.com/en/latest/apis/rest."
                    )
                )
            return int(match.group(1))

    def on_post(self, request: falcon.Request, response: falcon.Response):
        """
        POST /secure/sendbatch request:
        Schedule or immediately send a batch of messages through the Jasmin http api.
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
        # Iterate over each message and schedule/send it
        for msg_params in params.get('messages', {}):
            # Construct message parameters
            message_params = {
                'username': request.context.get('username'),
                'password': request.context.get('password')
            }
            message_params.update(params.get('globals', {}))
            message_params.update(msg_params)

            message_params = self.replace_underscores_in_keys(message_params)

            # Ignore messages missing essential parameters
            if 'to' not in message_params or (
                    'content' not in message_params and 'hex-content' not in message_params):
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
