-ifndef(EMQX_ACME_HRL).
-define(EMQX_ACME_HRL, true).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_managed_certs.hrl").

-define(LOG(Level, Data),
    ?SLOG(Level, maps:merge(#{tag => "ACME", domain => [acme]}, (Data)))
).

-define(DEFAULT_DIR_URL, "https://acme-v02.api.letsencrypt.org/directory").
-define(DEFAULT_CHALLENGE_PORT, 80).
-define(DEFAULT_RENEW_BEFORE_DAYS, 30).
-define(DEFAULT_CHECK_INTERVAL_HOURS, 24).
-define(DEFAULT_CERT_BUNDLE_NAME, <<"acme">>).
-define(DEFAULT_DASHBOARD_HTTPS_PORT, 18084).

-endif.
