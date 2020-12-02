emqx_auth_username
==================

EMQ X Authentication with Username and Password

Build
-----

```
make && make tests
```

Configuration
-------------

etc/emqx_auth_username.conf:

```
## Password hash.
##
## Value: plain | md5 | sha | sha256 
auth.user.password_hash = sha256
```

[REST API](https://developer.emqx.io/docs/emq/v3/en/rest.html)
------------

List all usernames
```
# Request
GET api/v4/auth_username

# Response
{
    "code": 0,
    "data": ["username1"]
}
```

Add a username:
```
# Request
POST api/v4/auth_username
{
    "username": "some_name",
    "password": "password"
}

# Response
{
    "code": 0
}
```

Update password for a username:
```
# Request
PUT api/v4/auth_username/$NAME
{
    "password": "password"
}

# Response
{
    "code", 0
}
```

Lookup a username info:
```
# Request
GET api/v4/auth_username/$NAME

# Response
{
    "code": 0,
    "data": {
        "username": "some_username",
        "password": "hashed_password"
    }
}
```

Delete a username:
```
# Request
DELETE api/v4/auth_username/$NAME

# Response
{
    "code": 0
}
```

Load the Plugin
---------------

```
./bin/emqx_ctl plugins load emqx_auth_username
```

License
-------

Apache License Version 2.0

Author
------

EMQ X Team.

