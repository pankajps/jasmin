#!/bin/bash
set -euo pipefail

# Set default values for environment variables if not provided
: "${CONFIG_PATH:=/etc/jasmin}"
: "${REDIS_CLIENT_HOST:=localhost}"
: "${REDIS_CLIENT_PORT:=6379}"
: "${AMQP_BROKER_HOST:=localhost}"
: "${AMQP_BROKER_PORT:=5672}"
: "${RESTAPI_MODE:=0}"
: "${RESTAPI_OLD_HTTP_HOST:=localhost}"
: "${ENABLE_PUBLISH_SUBMIT_SM_RESP:=1}"

CONFIG_FILE="${CONFIG_PATH}/jasmin.cfg"

# Function to update configuration parameters
update_config() {
  local section="$1"
  local key="$2"
  local value="$3"
  sed -i "/\[$section\]/,/\[/{s/^$key=.*/$key=$value/}" "$CONFIG_FILE"
}

echo "Updating Redis and AMQP configuration in $CONFIG_FILE"
update_config "jcli" "bind" "0.0.0.0"
update_config "redis-client" "host" "$REDIS_CLIENT_HOST"
update_config "redis-client" "port" "$REDIS_CLIENT_PORT"
update_config "amqp-broker" "host" "$AMQP_BROKER_HOST"
update_config "amqp-broker" "port" "$AMQP_BROKER_PORT"

echo 'Cleaning lock files in /tmp'
rm -f /tmp/*.lock

if [ "$RESTAPI_MODE" = "1" ]; then
  # Find Jasmin installation directory
  jasmin_root=$(python -c "import jasmin; print(jasmin.__path__[0])")
  CONFIG_PY="${jasmin_root}/protocols/rest/config.py"

  echo "Updating REST API configuration in $CONFIG_PY"
  sed -i "s|^\(.*old_api_uri\s*=\s*\).*|\1'http://$RESTAPI_OLD_HTTP_HOST:1401'|" "$CONFIG_PY"
  sed -i "s|^\(.*broker_url\s*=\s*\).*|\1'amqp://guest:guest@$AMQP_BROKER_HOST:$AMQP_BROKER_PORT//'|" "$CONFIG_PY"
  sed -i "s|^\(.*result_backend\s*=\s*\).*|\1'redis://@$REDIS_CLIENT_HOST:$REDIS_CLIENT_PORT/1'|" "$CONFIG_PY"

  echo 'Starting Jasmin REST API using Gunicorn'
  exec gunicorn -b 0.0.0.0:8080 jasmin.protocols.rest:api \
    --access-logfile /var/log/jasmin/rest-api.access.log \
    --disable-redirect-access-to-syslog
else
  # Update publish_submit_sm_resp setting
  if [ "$ENABLE_PUBLISH_SUBMIT_SM_RESP" = "1" ]; then
    echo 'Enabling publish_submit_sm_resp in jasmin.cfg'
    sed -i "s/^.*publish_submit_sm_resp\s*=.*/publish_submit_sm_resp = True/" "$CONFIG_FILE"
  else
    echo 'Disabling publish_submit_sm_resp in jasmin.cfg'
    sed -i "s/^.*publish_submit_sm_resp\s*=.*/publish_submit_sm_resp = False/" "$CONFIG_FILE"
  fi

  # Start interceptord if requested
  if [ "${2:-}" = "--enable-interceptor-client" ]; then
    if command -v interceptord.py >/dev/null 2>&1; then
      echo 'Starting interceptord'
      interceptord.py &
    else
      echo 'Warning: interceptord.py not found, skipping'
    fi
  fi

  echo 'Starting jasmind with arguments: '"$*"
  exec "$@"
fi
