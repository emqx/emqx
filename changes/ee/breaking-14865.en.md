Dropped old LDAP authentication config layout (deprecated since v5.4).
Move `password_attribute` and `is_superuser_attribute` under the `method` block:
  ```hcl
  method {
    type = hash
    password_attribute = "userPassword"
    is_superuser_attribute = "isSuperuser"
  }
  ```
