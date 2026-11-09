# BIGDATA_SIBI Hadoop NameNode
FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

USER root

# Fix archived Debian Stretch repositories
RUN set -eux; \
    if grep -q 'stretch' /etc/os-release 2>/dev/null; then \
        sed -i \
            -e 's|deb.debian.org|archive.debian.org|g' \
            -e 's|security.debian.org|archive.debian.org|g' \
            /etc/apt/sources.list; \
        sed -i '/stretch-updates/d' /etc/apt/sources.list; \
    fi; \
    apt-get -o Acquire::Check-Valid-Until=false update; \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-minimal; \
    ln -sf /usr/bin/python3 /usr/bin/python; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

# Preserve original Hadoop entrypoint
ENTRYPOINT ["/entrypoint.sh"]
CMD ["/run.sh"]