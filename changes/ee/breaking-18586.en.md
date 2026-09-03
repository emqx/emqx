`dashboard.default_mfa` is now an account-wide requirement rather than a one-time default. While it is set, a Dashboard user can no longer disable their own MFA: `DELETE /current_user/mfa` answers `403 MFA_ENFORCED`, and a user whose MFA is in the `disabled` state is enrolled again at their next login. Rotating to a new authenticator keeps MFA on and so stays available.

An administrator can still exempt an account. Disabling another user's MFA with `DELETE /users/:username/mfa`, or `emqx ctl admins mfa <Username> disable` from the node console, records the exemption and takes that account out of the requirement. Enabling `default_mfa` on an existing cluster therefore forces every non-exempted user who signs in with a Dashboard password, including anyone who had previously turned MFA off, to enroll at their next login.

The requirement does not reach SSO logins. Those are decided by the `force_mfa` flag of the SSO backend, which `dashboard.default_mfa` does not feed into.
