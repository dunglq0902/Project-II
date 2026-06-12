FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8

RUN apt-get update \
     && apt-get install -y --no-install-recommends \
         default-jdk \
       git \
       curl \
       build-essential \
       wget \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /workspace

# Copy only the minimal test requirements to avoid heavy installs (Airflow, etc.)
COPY docker/test-requirements.txt /workspace/test-requirements.txt

RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /workspace/test-requirements.txt

# Copy project into the image
COPY . /workspace

ENV PYTHONPATH=/workspace

COPY docker/test-runner-entrypoint.sh /usr/local/bin/test-entrypoint.sh
RUN chmod +x /usr/local/bin/test-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/test-entrypoint.sh"]
