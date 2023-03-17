Certificate and Key files for testing

## Cassandra (v3.x)

### How to convert server PEM to JKS Format

1. Convert server.crt and server.key to server.p12

```bash
openssl pkcs12 -export -in server.crt -inkey server.key -out server.p12 -name "certificate"
```

2. Convert server.p12 to server.jks

```bash
keytool -importkeystore -srckeystore server.p12 -srcstoretype pkcs12 -destkeystore server.jks
```

### How to convert CA PEM certificate to truststore.jks

```
keytool -import -file ca.pem -keystore truststore.jks
```
