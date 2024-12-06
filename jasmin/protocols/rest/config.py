"""Jasmin Celery and REST API Configuration"""

import logging
from jasmin.config import LOG_PATH

# REST API Settings
old_api_uri = 'http://127.0.0.1:1401'        # Base URI for the old Jasmin HTTP API
show_jasmin_version = True                   # Whether to show Jasmin version in REST responses
auth_cache_seconds = 10                      # How long to cache authentication results (in seconds)
auth_cache_max_keys = 500                    # Max number of keys to store in the auth cache

# Logging Configuration
log_level = logging.getLevelName('INFO')     # Set logging level to INFO
log_file = f'{LOG_PATH}/restapi.log'         # REST API log file path
log_rotate = 'W6'                            # Rotate logs every 6 weeks
log_format = '%(asctime)s %(levelname)-8s %(process)d %(message)s'
log_date_format = '%Y-%m-%d %H:%M:%S'

# Celery Configuration
broker_url = 'amqp://guest:guest@127.0.0.1:5672//'  # AMQP broker connection
result_backend = 'redis://:@127.0.0.1:6379/1'       # Redis backend for Celery results
task_serializer = 'json'                            # Task serialization format
result_serializer = 'json'                          # Result serialization format
accept_content = ['json']                           # Accept only JSON content
timezone = 'UTC'                                    # Timezone for Celery tasks
enable_utc = True                                   # Use UTC for task timestamps

# Throughput & QoS Settings
http_throughput_per_worker = 8  # Max HTTP requests per second *per worker* (0 to disable control)
smart_qos = True                # Enable "Smart QoS": dynamically adjust throughput based on response times
