emqx_auth_ldap
==============

EMQX LDAP Authentication Plugin

Build
-----

```
make
```

Load the Plugin
---------------

```
# ./bin/emqx_ctl plugins load emqx_auth_ldap
```

Generate Password
---------------

```
slappasswd -h '{ssha}' -s public
```

Configuration Open LDAP
-----------------------

vim /etc/openldap/slapd.conf

```
include         /etc/openldap/schema/core.schema
include         /etc/openldap/schema/cosine.schema
include         /etc/openldap/schema/inetorgperson.schema
include         /etc/openldap/schema/ppolicy.schema
include         /etc/openldap/schema/emqx.schema

database bdb
suffix "dc=emqx,dc=io"
rootdn "cn=root,dc=emqx,dc=io"
rootpw {SSHA}eoF7NhNrejVYYyGHqnt+MdKNBh4r1w3W

directory       /etc/openldap/data
```

If the ldap launched with error below:
```
Unrecognized database type (bdb)
5c4a72b9 slapd.conf: line 7: <database> failed init (bdb)
slapadd: bad configuration file!
```

Insert lines to the slapd.conf
```
modulepath /usr/lib/ldap
moduleload back_bdb.la
```

Import EMQX User Data
----------------------

Use ldapadd
```
# ldapadd -x -D "cn=root,dc=emqx,dc=io" -w public -f emqx.com.ldif
```

Use slapadd
```
# sudo slapadd -l schema/emqx.io.ldif -f slapd.conf
```

Launch slapd
```
# sudo slapd -d 3
```

Test
-----
After configure slapd correctly and launch slapd successfully.
You could execute

``` bash
# make tests
```

License
-------

Apache License Version 2.0

Author
------

EMQX Team.

