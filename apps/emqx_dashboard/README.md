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

Multifactor authorization is introduced in 5.9.
So far, only TOTP is supported.
The feature is only accessible from enterprise edition dashboard.

### Configuration

MFA is disabled by default. To enable MFA, the admin needs to set the `dashboard.default_mfa` configuration item to `none` or `{mechanism: totp}`.

To enable MFA for a specific user, the admin needs to call the API `users/{username}/mfa` with `POST` body `{"mechanism": "totp"}`.

To disable MFA for a specific user, the admin needs to call the API `users/{username}/mfa` with `DELETE` method.

### MFA Protocol

Here we describe the steps for EMQX dashboard to follow when MFA is enabled for a user.

- Step 1: call `/login` API without MFA token
- Step 2: call `/login` API with MFA token based on the result of Step 1.

Detailed steps are:

- The 1st `/login`, the body only includes `username` and `password`, without providing an MFA token.
  - If the user does not require MFA, then the process proceeds to password hash comparison.
  - If the user has set up MFA, there are two scenarios:
    - If an MFA token has never been submitted before (indicating that the Authenticator App needs to be initialized):
      - The backend verifies the password. If the password is incorrect, it returns a `401` error to indicate bad username or password. If the password is correct, the process continues.
      - A `403` error is returned, and the body like `{"code": "BAD_MFA_TOKEN", "message": {"error": "missing_mfa_token", "mechanism": "totp", "cluster": "EMQX Cluster Name", "secret": "ABCDJBSWY3DPEHPK3PXP"}}`.
      - The Dashbaord uses this secret to generate a QR code for Authenticator App.
    - If an MFA token has been submitted before (indicating that the Authenticator App for generating the code is already set up):
      - The backend returns a `403` error, and the body like `{"code": "BAD_MFA_TOKEN", "message": {"error": "missing_mfa_token", "mechanism": "totp", "cluster": "EMQX Cluster Name"}` (note that no secret is provided).
      - The Dashboard displays a input box for MFA token.
- The 2nd `/login`, the user enters the MFA token (TOTP code from Authenticator App)
  - The backend checks the token
  - If the token is valid, the process proceeds with the regular password verification.
  - If the token is invalid, it returns a `403` error with body like `{"code": "BAD_MFA_TOKEN", "message": {"error": "bad_mfa_token", "mechanism": "totp", "cluster": "EMQX Cluster Name"}`

### Discussion

- Security Concern: The password is vulnerable to brute-force attacks until the MFA token is validated for the first time.
- Error Standardization: There is no standard error/message to indicate that MFA initialization is required. In our implementation, we use a `403` status code for this purpose.
- Permission Restrictions: The POST and DELETE methods on the `users/{username}/mfa` endpoint are restricted to the `administrator` users or the current bearer token owner, meaning a `viewer` role user cannot change another user's MFA settings.

