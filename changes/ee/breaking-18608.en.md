Dashboard account APIs are now separate from user administration APIs.

Users manage their own accounts through `/current_user`:

- `GET /current_user` returns the signed-in user's username, role, effective scopes, and MFA status.
- `POST /current_user/change_pwd` changes the signed-in user's password. The request must include `old_pwd` and `new_pwd`.
- `POST /current_user/mfa` sets up or rotates the signed-in user's MFA.
- `DELETE /current_user/mfa` disables the signed-in user's MFA.

Breaking changes:

- `POST /users/:username/change_pwd` is deprecated and restricted to the signed-in user. EMQX returns `403` when the path identifies another user. Use `POST /current_user/change_pwd` instead. Administrators can reset another local user's password only with `emqx ctl admins passwd`.
- `POST` and `DELETE /users/:username/mfa` now manage other users and require the administrator role. Use `/current_user/mfa` to manage your own MFA. Namespaced administrators cannot manage another user's MFA.
- The `mfa_management` scope is now administrator-only and grants permission to manage another user's MFA. It no longer lets non-administrators override an MFA requirement on their own accounts. EMQX returns `400` when this scope is assigned to a non-administrator. Another administrator or the node console must remove an administrator-set MFA requirement.

Operations that end a Dashboard session now also end the sessions of SSO accounts: rotating or disabling MFA, changing a password, changing a role, and deleting a user. An SSO account previously kept its bearer tokens through all of these. Ending one account's sessions no longer ends those of an account with the same name on a different backend.
