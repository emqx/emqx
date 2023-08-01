FROM buildpack-deps:bookworm

ARG LDAP_TAG=2.5.16

RUN apt-get update && apt-get install -y groff groff-base
RUN wget https://www.openldap.org/software/download/OpenLDAP/openldap-release/openldap-${LDAP_TAG}.tgz \
    && tar xvzf openldap-${LDAP_TAG}.tgz \
    && cd openldap-${LDAP_TAG} \
    && ./configure && make depend && make && make install \
    && cd .. && rm -rf  openldap-${LDAP_TAG}

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
