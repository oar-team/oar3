ARG PYTHON_IMAGE

FROM $PYTHON_IMAGE

ENV PYTHONUNBUFFERED=1 \
    PYTHONBYTECODEBASE=/tmp/python \
    PATH=/venv/bin:$PATH

ADD scripts/ci/docker/build-base.sh /tmp/
RUN /bin/bash /tmp/build-base.sh

ADD pyproject.toml poetry.lock /app/
ADD scripts/ci/docker/build-venv.sh /tmp/
RUN /bin/bash /tmp/build-venv.sh

ADD scripts/ci/docker/entrypoint.sh /docker-entrypoint.sh

WORKDIR /app

ENTRYPOINT ["/docker-entrypoint.sh"]
