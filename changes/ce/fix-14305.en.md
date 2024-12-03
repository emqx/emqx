Use safer pbkdf2_hmac implementation in following EMQX functions:

- MongoDB integration
- authn. Also removes support of md4, md5, ripemd160 as they are not FIPS_DIGEST.


