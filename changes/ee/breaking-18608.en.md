Split the Dashboard self-service APIs out of the administrator user-management APIs. A user managing their own account now uses `/current_user/*`; the endpoints under `/users/:username/*` manage other users.

New self-service endpoints, authorized by the signed-in identity alone and requiring no permission scope:

- `GET /current_user`: own username, role, effective scopes and MFA status.
- `POST /current_user/change_pwd`: change own password (`old_pwd` + `new_pwd`).
- `POST /current_user/mfa`: set up or rotate own MFA.
- `DELETE /current_user/mfa`: disable own MFA.

This is a breaking change:

- `POST /users/:username/change_pwd` is removed. Use `POST /current_user/change_pwd` to change your own password. This endpoint always verified `old_pwd`, so it was never an administrator reset; resetting another user's password remains available only from the node console (`emqx ctl admins passwd`).
- `POST` and `DELETE /users/:username/mfa` now require the administrator role and manage other users only. Use `/current_user/mfa` for your own account. A namespaced (multi-tenant) administrator can no longer reach another user's MFA, not even within its own namespace.
- The `mfa_management` scope becomes administrator-only and means "manage other users' MFA". It previously also acted as a self-exemption key that let a non-administrator bypass an MFA lock on their own account. That meaning is gone, and assigning the scope to a non-administrator is now rejected with `400`. An account whose MFA an administrator has reset can be unlocked by another administrator or from the node console.

Dashboard clients and any scripts calling the removed or narrowed endpoints must be updated.
