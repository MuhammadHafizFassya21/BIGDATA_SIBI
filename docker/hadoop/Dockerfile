# ==============================================================================
# Hadoop NameNode with Python 3 Runtime for Hadoop Streaming (BIGDATA_SIBI)
# ==============================================================================
# Base Image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
# Provides: Hadoop 3.2.1, OpenJDK 8, and Python 3 for LocalJobRunner MapReduce
# ==============================================================================

FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

USER root

# Update package sources and install Python 3 standard runtime
# Handle legacy Debian repository archival if necessary
RUN if grep -q 'stretch' /etc/os-release 2>/dev/null; then \
        sed -i 's/deb.debian.org/archive.debian.org/g' /etc/apt/sources.list && \
        sed -i 's|security.debian.org|archive.debian.org/debian-security/|g' /etc/apt/sources.list && \
        sed -i '/stretch-updates/d' /etc/apt/sources.list ; \
    fi && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-minimal \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Preserve original BDE entrypoint and daemon run command
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/run.sh"]

