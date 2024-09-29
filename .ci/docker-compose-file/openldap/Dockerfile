FROM ghcr.io/emqx/openldap:1.0.0

## entrypoint
COPY .ci/docker-compose-file/openldap/entrypoint /usr/local/etc/openldap/entrypoint

## conf && schema
COPY .ci/docker-compose-file/openldap/slapd.conf /usr/local/etc/openldap/slapd.conf
COPY apps/emqx_ldap/test/data/emqx.schema /usr/local/etc/openldap/schema/emqx.schema

## data
COPY apps/emqx_ldap/test/data/emqx.io.ldif /usr/local/etc/openldap/schema/emqx.io.ldif
COPY apps/emqx_ldap/test/data/emqx.groups.ldif /usr/local/etc/openldap/schema/emqx.groups.ldif

## pem
COPY .ci/docker-compose-file/certs/ca.crt /usr/local/etc/openldap/cacert.pem
COPY .ci/docker-compose-file/certs/server.crt /usr/local/etc/openldap/cert.pem
COPY .ci/docker-compose-file/certs/server.key /usr/local/etc/openldap/key.pem

RUN mkdir -p /usr/local/etc/openldap/data \
    && slapadd -l /usr/local/etc/openldap/schema/emqx.io.ldif -f /usr/local/etc/openldap/slapd.conf

WORKDIR /usr/local/etc/openldap

EXPOSE 389 636

ENTRYPOINT ["./entrypoint"]

CMD []
