# Use the official Python slim Debian-based image
FROM python:3.12.8-slim-bookworm

LABEL maintainer="Jookies LTD <jasmin@jookies.net>"

# Set environment variables
ENV RESTAPI_MODE=0 \
    RESTAPI_OLD_HTTP_HOST="127.0.0.1" \
    ENABLE_PUBLISH_SUBMIT_SM_RESP=0 \
    ROOT_PATH=/ \
    CONFIG_PATH=/etc/jasmin \
    RESOURCE_PATH=/etc/jasmin/resource \
    STORE_PATH=/etc/jasmin/store \
    LOG_PATH=/var/log/jasmin \
    UNICODEMAP_JP=unicode-ascii \
    JCLI_ADMIN=jcliadmin \
    JCLI_PWD=jclipwd

# Install system dependencies and create jasmin user
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
        libssl-dev \
        python3-dev \
        python3-pip \
        git \
        bash \
        sed \
    && addgroup --system jasmin \
    && adduser --system --ingroup jasmin jasmin \
    && mkdir -p "${CONFIG_PATH}" "${RESOURCE_PATH}" "${STORE_PATH}" "${LOG_PATH}" \
    && chown -R jasmin:jasmin "${CONFIG_PATH}" "${RESOURCE_PATH}" "${STORE_PATH}" "${LOG_PATH}" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip

# Install required Python packages from Git repositories
RUN pip install --no-cache-dir \
        -e git+https://github.com/jookies/txamqp.git@master#egg=txamqp3 \
        -e git+https://github.com/jookies/python-messaging.git@master#egg=python-messaging \
        -e git+https://github.com/jookies/smpp.pdu.git@master#egg=smpp.pdu3 \
        -e git+https://github.com/jookies/smpp.twisted.git@master#egg=smpp.twisted3

# Copy application code
COPY . .

# Install the application
RUN pip install --no-cache-dir .

# Perform sed operations to update REST API configuration as root
RUN if [ "$RESTAPI_MODE" = "1" ]; then \
        jasmin_root=$(python -c "import jasmin; print(jasmin.__path__[0])") && \
        CONFIG_PY="${jasmin_root}/protocols/rest/config.py" && \
        sed -i "s|^\(.*old_api_uri\s*=\s*\).*|\1'http://$RESTAPI_OLD_HTTP_HOST:1401'|" "$CONFIG_PY" && \
        sed -i "s|^\(.*broker_url\s*=\s*\).*|\1'amqp://guest:guest@$AMQP_BROKER_HOST:$AMQP_BROKER_PORT//'|" "$CONFIG_PY" && \
        sed -i "s|^\(.*result_backend\s*=\s*\).*|\1'redis://@$REDIS_CLIENT_HOST:$REDIS_CLIENT_PORT/1'|" "$CONFIG_PY"; \
    fi

# Install Gunicorn for REST API mode
RUN pip install --no-cache-dir gunicorn

# (Optional) Copy configuration files
# Uncomment the following lines if you have configuration files to include
# COPY misc/config/*.cfg "${CONFIG_PATH}/"
# COPY misc/config/resource/*.xml "${RESOURCE_PATH}/"

# Set the working directory to the configuration path
WORKDIR "${CONFIG_PATH}"

# Expose necessary ports
EXPOSE 2775 8990 1401 8080

# (Optional) Define volumes
# Uncomment the following line if you want to define Docker volumes
# VOLUME ["/var/log/jasmin", "/etc/jasmin", "/etc/jasmin/store"]

# Copy the entrypoint script and set execute permissions
COPY ./compose/local/jasmin/entrypoint /entrypoint
RUN sed -i 's/\r$//g' /entrypoint && chmod +x /entrypoint

COPY ./compose/local/jasmin/start /start
RUN sed -i 's/\r$//g' /start && chmod +x /start


#RUN mkdir -p "${CONFIG_PATH}" "${RESOURCE_PATH}" "${STORE_PATH}" "${LOG_PATH}" \
#    && chown -R jasmin:jasmin "${CONFIG_PATH}" "${RESOURCE_PATH}" "${STORE_PATH}" "${LOG_PATH}"

# Switch to the non-root user
#USER jasmin

# Define the entrypoint and default command
ENTRYPOINT ["/entrypoint"]

CMD ["jasmind.py", "--enable-interceptor-client", "--enable-dlr-thrower", "--enable-dlr-lookup", "-u", "jcliadmin", "-p", "jclipwd"]
