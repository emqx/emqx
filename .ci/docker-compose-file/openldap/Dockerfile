FROM buildpack-deps:stable

ARG LDAP_TAG=2.4.50

RUN apt-get update && apt-get install -y groff groff-base
RUN wget ftp://ftp.openldap.org/pub/OpenLDAP/openldap-release/openldap-${LDAP_TAG}.tgz \
    && gunzip -c openldap-${LDAP_TAG}.tgz | tar xvfB - \
    && cd openldap-${LDAP_TAG} \
    && ./configure && make depend && make && make install \
    && cd .. && rm -rf  openldap-${LDAP_TAG}

COPY .ci/docker-compose-file/openldap/slapd.conf /usr/local/etc/openldap/slapd.conf
COPY apps/emqx_auth_ldap/emqx.io.ldif /usr/local/etc/openldap/schema/emqx.io.ldif
COPY apps/emqx_auth_ldap/emqx.schema /usr/local/etc/openldap/schema/emqx.schema
COPY apps/emqx_auth_ldap/test/certs/*.pem /usr/local/etc/openldap/

RUN mkdir -p /usr/local/etc/openldap/data \
    && slapadd -l /usr/local/etc/openldap/schema/emqx.io.ldif -f /usr/local/etc/openldap/slapd.conf

WORKDIR /usr/local/etc/openldap

EXPOSE 389 636

ENTRYPOINT ["/usr/local/libexec/slapd", "-h", "ldap:/// ldaps:///", "-d", "3", "-f", "/usr/local/etc/openldap/slapd.conf"]

CMD []
