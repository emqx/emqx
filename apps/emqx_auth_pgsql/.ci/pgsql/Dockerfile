ARG BUILD_FROM=postgres:11
FROM ${BUILD_FROM}
COPY pg.conf /etc/postgresql/postgresql.conf
COPY server-cert.pem /etc/postgresql/server-cert.pem
COPY server-key.pem /etc/postgresql/server-key.pem
RUN chown -R postgres:postgres /etc/postgresql \
    && chmod 600 /etc/postgresql/*.pem 
CMD ["-c", "config_file=/etc/postgresql/postgresql.conf"]
