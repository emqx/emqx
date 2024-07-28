# LDAP authentication

To run manual tests with the default docker-compose files.

Expose openldap container port by uncommenting the `ports` config in `docker-compose-ldap.yaml `

To start openldap:

```
docker-compose -f ./.ci/docker-compose-file/docker-compose.yaml -f ./.ci/docker-compose-file/docker-compose-ldap.yaml up -docker
```

## LDAP database

LDAP database is populated from below files:
```
apps/emqx_ldap/test/data/emqx.io.ldif /usr/local/etc/openldap/schema/emqx.io.ldif
apps/emqx_ldap/test/data/emqx.schema /usr/local/etc/openldap/schema/emqx.schema
```

## Minimal EMQX config

```
authentication = [
  {
    backend = ldap
    base_dn = "uid=${username},ou=testdevice,dc=emqx,dc=io"
    filter = "(& (objectClass=mqttUser) (uid=${username}))"
    mechanism = password_based
    method {
      is_superuser_attribute = isSuperuser
      password_attribute = userPassword
      type = hash
    }
    password = public
    pool_size = 8
    query_timeout = "5s"
    request_timeout = "10s"
    server = "localhost:1389"
    username = "cn=root,dc=emqx,dc=io"
  }
]
```

## Example ldapsearch command

```
ldapsearch -x -H ldap://localhost:389 -D "cn=root,dc=emqx,dc=io" -W -b "uid=mqttuser0007,ou=testdevice,dc=emqx,dc=io" "(&(objectClass=mqttUser)(uid=mqttuser0007))"
```

## Example mqttx command

The client password hashes are generated from their username.

```
# disabled user
mqttx pub -t 't/1' -h localhost -p 1883 -m x -u mqttuser0006 -P mqttuser0006

# enabled super-user
mqttx pub -t 't/1' -h localhost -p 1883 -m x -u mqttuser0007 -P mqttuser0007
```
