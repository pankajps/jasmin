# define an alias for the specific python version used in this file.
FROM docker.io/python:3.12.8-slim-bookworm AS python

# Python build stage
FROM python AS python-build-stage

ARG BUILD_ENVIRONMENT=production

# Install apt packages required only for building wheels
RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Requirements are installed here to ensure they will be cached.
COPY ./requirements .

RUN pip wheel --wheel-dir /usr/src/app/wheels -r ${BUILD_ENVIRONMENT}.txt

# Python 'run' stage
FROM python AS python-run-stage

ARG BUILD_ENVIRONMENT=local
ARG APP_HOME=/app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV BUILD_ENV=${BUILD_ENVIRONMENT}

WORKDIR ${APP_HOME}

# Install devcontainer dependencies and utils
RUN apt-get update && apt-get install --no-install-recommends -y \
    sudo git bash-completion nano ssh \
    && rm -rf /var/lib/apt/lists/*

# Create devcontainer user and add it to sudoers
RUN groupadd --gid 1000 jasmin \
    && useradd --uid 1000 --gid jasmin --shell /bin/bash --create-home jasmin \
    && echo jasmin ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/jasmin \
    && chmod 0440 /etc/sudoers.d/jasmin

# Install required system dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
    gcc \
    libffi-dev \
    libssl-dev \
    libpq-dev sed gettext \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Upgrade pip to the latest version
RUN pip install --no-cache-dir --upgrade pip

# Install required Python packages from Git repositories
RUN pip install --no-cache-dir \
        -e git+https://github.com/jookies/txamqp.git@master#egg=txamqp3 \
        -e git+https://github.com/jookies/python-messaging.git@master#egg=python-messaging \
        -e git+https://github.com/jookies/smpp.pdu.git@master#egg=smpp.pdu3 \
        -e git+https://github.com/jookies/smpp.twisted.git@master#egg=smpp.twisted3

# copy python dependency wheels from python-build-stage
COPY --from=python-build-stage /usr/src/app/wheels /wheels/

# Install python deps from wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels/ /wheels/*

# Copy application code
COPY . .

# Install the application
RUN pip install --no-cache-dir . --find-links=/wheels/ \
  && rm -rf /wheels/


# (Optional) Copy configuration files
# Uncomment the following lines if you have configuration files to include
# COPY misc/config/*.cfg "${CONFIG_PATH}/"
# COPY misc/config/resource/*.xml "${RESOURCE_PATH}/"

ARG CONFIG_PATH=/etc/jasmin
ENV CONFIG_PATH=${CONFIG_PATH}

ARG RESOURCE_PATH=/etc/jasmin/resource
ENV RESOURCE_PATH=${RESOURCE_PATH}

ARG STORE_PATH=/etc/jasmin/store
ENV STORE_PATH=${STORE_PATH}

ARG LOG_PATH=/var/log/jasmin
ENV LOG_PATH=${LOG_PATH}

RUN mkdir -p "${CONFIG_PATH}" "${RESOURCE_PATH}" "${STORE_PATH}" "${LOG_PATH}" \
    && chown -R jasmin:jasmin "${CONFIG_PATH}" "${RESOURCE_PATH}" "${STORE_PATH}" "${LOG_PATH}" \
    && chmod 777 "${LOG_PATH}"


# Copy the entrypoint script and set execute permissions
COPY ./compose/local/jasmin/entrypoint /entrypoint
RUN sed -i 's/\r$//g' /entrypoint && chmod +x /entrypoint

COPY ./compose/local/jasmin/start /start
RUN sed -i 's/\r$//g' /start && chmod +x /start

# Set the working directory to the configuration path
WORKDIR "${CONFIG_PATH}"

# Switch to the non-root user
#USER jasmin

# Define the entrypoint and default command
ENTRYPOINT ["/entrypoint"]

CMD ["jasmind.py", "--enable-interceptor-client", "--enable-dlr-thrower", "--enable-dlr-lookup", "-u", "${JCLI_ADMIN}", "-p", "${JCLI_PWD}"]
