import time
import logging
import logging.handlers
from datetime import datetime, timedelta

import requests
from celery import Celery, Task

from .config import old_api_uri, log_level, log_file, log_rotate, log_format, log_date_format

# Configure logger
logger = logging.getLogger('jasmin-restapi')
if not logger.handlers:
    logger.setLevel(log_level)
    handler = logging.handlers.TimedRotatingFileHandler(filename=log_file, when=log_rotate)
    handler.setFormatter(logging.Formatter(log_format, log_date_format))
    logger.addHandler(handler)


app = Celery(__name__)
app.config_from_object('jasmin.protocols.rest.config')


class JasminTask(Task):
    """
    A base Celery Task that includes shared QoS tracking across tasks.
    The worker_tracker dictionary holds:
    - last_req_at: timestamp of the last request
    - last_req_time: elapsed time of the last request (in seconds)
    - throughput: current throughput setting (requests/second)
    """

    def __init__(self):
        super().__init__()
        self.worker_tracker = {
            'last_req_at': datetime.now(),
            'last_req_time': 0,
            'throughput': 0
        }

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f'Task [{task_id}] failed: {exc}')

    def _apply_qos_delay(self):
        """
        Apply a delay if required by the current throughput setting to respect QoS constraints.
        Returns the number of seconds to sleep before sending the next request.
        """
        if self.worker_tracker['throughput'] > 0:
            # throughput = requests/second => each request should take at least (1/throughput) seconds
            qos_interval = 1.0 / float(self.worker_tracker['throughput'])
            qos_interval_td = timedelta(microseconds=qos_interval * 1_000_000)

            qos_delay = datetime.now() - self.worker_tracker['last_req_at']
            if qos_delay < qos_interval_td:
                # Not enough time passed since last request, need to slow down
                slow_down_seconds = (qos_interval_td - qos_delay).total_seconds()
                logger.debug(
                    f'QoS: Slowing down request by {slow_down_seconds:.4f}s '
                    f'to meet throughput: {self.worker_tracker["throughput"]}/s'
                )
                return slow_down_seconds
        return 0

    def _adjust_throughput(self, response_elapsed, config):
        """
        Adjust the throughput setting based on the response time compared to the last request time
        if smart_qos is enabled.

        :param response_elapsed: The elapsed time (in seconds) of the current request.
        :param config: Global config dict including 'throughput' and 'smart_qos'.
        """
        current_throughput = self.worker_tracker['throughput'] or config['throughput']

        if config.get('smart_qos') and self.worker_tracker['last_req_time'] is not None:
            # If current request is slower than last one, try to slow down
            if response_elapsed > self.worker_tracker['last_req_time']:
                if current_throughput > 0 and (current_throughput - (current_throughput * 0.1)) > 0:
                    logger.debug(
                        f'Smart QoS: Slowing down throughput {current_throughput}/s by 10%'
                    )
                    current_throughput -= current_throughput * 0.1
                elif current_throughput == 0:
                    # If unlimited, reduce to a finite slower value
                    logger.debug('Smart QoS: Reducing unlimited throughput to 0.5/s')
                    current_throughput = 0.5
            # If current request is faster, try to boost throughput
            elif response_elapsed < self.worker_tracker['last_req_time']:
                # Boost only if allowed and doesn't exceed configured max throughput
                if current_throughput > 0 and config['throughput'] > 0 and (
                        current_throughput + (current_throughput * 0.1) <= config['throughput']
                ):
                    logger.debug(
                        f'Smart QoS: Boosting throughput {current_throughput}/s by 10%'
                    )
                    current_throughput += current_throughput * 0.1
                elif current_throughput > 0 and config['throughput'] == 0:
                    # If no upper limit, restore unlimited
                    logger.debug('Smart QoS: Restoring throughput to unlimited')
                    current_throughput = 0

        self.worker_tracker['throughput'] = current_throughput
        self.worker_tracker['last_req_time'] = response_elapsed


@app.task(bind=True, base=JasminTask)
def httpapi_send(self, batch_id, batch_config, message_params, config):
    """
    Calls Jasmin's /send httpapi endpoint to send SMS messages.

    Implements QoS and throughput control, and optionally calls user-defined callback or errback URLs.

    :param batch_id: A unique identifier for the message batch.
    :param batch_config: Batch configuration dict, possibly containing 'callback_url' or 'errback_url'.
    :param message_params: SMS parameters including 'to' number and 'content' or 'hex-content'.
    :param config: Global config dict including 'throughput' and 'smart_qos'.
    """
    # Apply QoS delay if needed
    slow_down_seconds = self._apply_qos_delay()
    if slow_down_seconds > 0:
        time.sleep(slow_down_seconds)

    # Perform the HTTP GET request
    try:
        r = requests.get(f'{old_api_uri}/send', params=message_params)
    except requests.exceptions.ConnectionError as e:
        logger.error(f'[{batch_id}] Jasmin httpapi connection error: {e}')
        if batch_config.get('errback_url'):
            batch_callback.delay(
                batch_config['errback_url'], batch_id, message_params.get('to'), 0,
                f'HTTPAPI Connection error: {e}'
            )
        return
    except Exception as e:
        logger.exception(f'[{batch_id}] Unknown error encountered:')
        if batch_config.get('errback_url'):
            batch_callback.delay(
                batch_config['errback_url'], batch_id, message_params.get('to'), 0,
                f'Unknown error: {e}'
            )
        return
    else:
        # Update QoS tracking and adjust throughput if needed
        self.worker_tracker['last_req_at'] = datetime.now()
        self._adjust_throughput(r.elapsed.total_seconds(), config)

        # Check HTTP status and callback accordingly
        if r.status_code != 200:
            logger.error(f'[{batch_id}] Non-200 status: {r.text.strip()}')
            if batch_config.get('errback_url'):
                batch_callback.delay(
                    batch_config['errback_url'], batch_id, message_params.get('to'), 0,
                    f'HTTPAPI error: {r.text.strip()}'
                )
        else:
            # Success scenario: Call the callback if present
            if batch_config.get('callback_url'):
                batch_callback.delay(
                    batch_config['callback_url'], batch_id, message_params.get('to'), 1, r.text
                )


@app.task(bind=True, base=JasminTask)
def batch_callback(self, url, batch_id, to, status, status_text):
    """
    Calls user-defined callback or errback URL to inform them about batch execution status.

    :param url: The callback or errback URL to be called.
    :param batch_id: Identifier of the batch.
    :param to: The recipient of the message.
    :param status: Status code (0 for error, 1 for success).
    :param status_text: A text describing the status.
    """
    operation_name = 'Errback' if status == 0 else 'Callback'
    try:
        requests.get(url, params={'batchId': batch_id, 'to': to, 'status': status, 'statusText': status_text})
    except Exception as e:
        logger.error(f'({operation_name}) of batch {batch_id} to {url} failed ({type(e)}): {e}.')
    else:
        logger.info(f'({operation_name}) of batch {batch_id} to {url} succeeded.')
