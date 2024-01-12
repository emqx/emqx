FROM docker.io/zmstone/openldap:2.5.16@sha256:a813922115a1d1f1b974399595921d1778fae22b3f1ee15dcfa8cfa89700dbc7

COPY .ci/docker-compose-file/openldap/slapd.conf /usr/local/etc/openldap/slapd.conf
COPY apps/emqx_ldap/test/data/emqx.io.ldif /usr/local/etc/openldap/schema/emqx.io.ldif
COPY apps/emqx_ldap/test/data/emqx.schema /usr/local/etc/openldap/schema/emqx.schema
COPY .ci/docker-compose-file/certs/ca.crt /usr/local/etc/openldap/cacert.pem
COPY .ci/docker-compose-file/certs/server.crt /usr/local/etc/openldap/cert.pem
COPY .ci/docker-compose-file/certs/server.key /usr/local/etc/openldap/key.pem

RUN mkdir -p /usr/local/etc/openldap/data \
    && slapadd -l /usr/local/etc/openldap/schema/emqx.io.ldif -f /usr/local/etc/openldap/slapd.conf

WORKDIR /usr/local/etc/openldap

EXPOSE 389 636

ENTRYPOINT ["/usr/local/libexec/slapd", "-h", "ldap:/// ldaps:///", "-d", "3", "-f", "/usr/local/etc/openldap/slapd.conf"]

CMD []
