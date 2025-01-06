For safety reasons, the PBKDF2 algorithm used for authentication in the built-in database no longer supports derived key lengths exceeding 1024 bytes.

- If the derived key length is not explicitly configured, the default key length will continue to function as before.
- If the derived key length is configured to exceed 1024 bytes, system administrators must take the following actions before upgrading to version 5.9:
  - Update the configuration to set the derived key length to less than 1024 bytes.
  - Update all MQTT users by deleting and re-adding them.

