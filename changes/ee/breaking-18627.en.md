Dashboard SAML SSO now verifies IdP signatures by default in all security profiles.

Previously the default followed the security profile: the hardened profile verified signatures, but the legacy profile (the default until v7.0) did not, so it accepted an unsigned, forged SAMLResponse and issued a Dashboard session.

If you intentionally run an unsigned IdP, set `sso.saml.idp_signs_envelopes = false` and `sso.saml.idp_signs_assertions = false` explicitly. If the IdP does sign but its metadata carries no certificate, the SAML backend now fails to start with `missing_idp_certificate`.
