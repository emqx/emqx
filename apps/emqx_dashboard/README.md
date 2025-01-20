# EMQX Dashboard

This application provides access to the EMQX Dashboard as well as the actual,
underlying REST API itself and provides authorization to protect against
unauthorized access. Furthermore it connects middleware adding CORS headers.
Last but not least it exposes the `/status` endpoint needed for healtcheck
monitoring.

## Implementation details

This implementation is based on `minirest`, and relies on `hoconsc` to provide an
OpenAPI spec for `swagger`.

Note, at this point EMQX Dashboard itself is an independent frontend project and
is integrated through a static file handler. This code here is responsible to
provide an HTTP(S) server to give access to it and its underlying API calls.
This includes user management and login for the frontend.

## MFA

Multi-factor authorization is introduced in 5.9.
So far, only TOTP is supported.
The feature is only accessible from enterprise edition dashboard.

### Configuration

MFA is disabled by default. To enable MFA, the admin needs to set the `dashboard.default_mfa` configuration item to `none` or `{mechanism: totp}`.

To enable MFA for a specific user, the admin needs to call the API `users/{username}/mfa` with `POST` body `{"mechanism": "totp"}`.

To disable MFA for a specific user, the admin needs to call the API `users/{username}/mfa` with `DELETE` method.

### MFA Protocol

Here we describe the steps for EMQX dashboard to follow when MFA is enabled for a user:

- During the `/login` process, the initially submitted body only includes the `username` and `password`, without providing an MFA token:
  - If the user has not set up MFA, then the process proceeds to password hash comparison.
  - If the user has set up MFA, there are two scenarios:
    - If an MFA token has never been submitted before (indicating that the Authenticator App needs to be initialized):
      - The backend verifies the password. If the password is incorrect, it returns a `401` error to indicate bad username or password. If the password is correct, the process continues.
      - A `403` error is returned, and the body includes `{"error": "bad_mfa_token", "mechanism": "totp", "cluster": "EMQX Cluster Name", "secret": "ABCDJBSWY3DPEHPK3PXP"}`.
      - The Dashbaord uses this secret to generate a QR code.
    - If an MFA token has been submitted before (indicating that the Authenticator App for generating the code is already set up):
      - The backend returns a `403` error, and the body includes `{"error": "bad_mfa_token", "mechanism": "totp", "cluster": "EMQX Cluster Name"}` (note that no secret is provided).
      - The Dashboard displays a input box for MFA token.
    - The user enters the verification code and calls the `/login` endpoint again, with the body containing the `username`, `password`, and `mfa_token`.
    - The backend checks the token. If the token is incorrect, it returns a `401` error stating that the MFA token is invalid.
    - If the password is correct, the process proceeds with the regular password verification.
