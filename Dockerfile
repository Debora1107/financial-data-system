FROM rabbitmq:3-management

WORKDIR /var/lib/rabbitmq

COPY rabbitmq.conf /etc/rabbitmq/rabbitmq.conf

EXPOSE 5672 15672

HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD rabbitmq-diagnostics -q ping || exit 1
