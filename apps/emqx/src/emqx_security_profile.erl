%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_security_profile).

-moduledoc """
This module manages the security profile of EMQX, which can be either "legacy" or
"hardened".

NOTE: this module may be called without the EMQX application started,
e.g. in schema validation code.
""".

-define(PT_KEY, {?MODULE, profile}).
-define(PROFILE_ENV_VAR, "EMQX_SECURITY_PROFILE").

%% Since 7.0
-define(PROFILE_DEFAULT, hardened).

-export([profile/0, policy/1, clear_profile/0]).

-export_type([profile/0]).

-type profile() :: legacy | hardened.

%---------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Returns a value depending on the current security profile.

Use this function only for introspection/logging purposes.
Do not rely on security profile values directly in the code logic,
use `emqx_security_profile:policy/1` instead.
""".
-spec profile() -> profile().
profile() ->
    case persistent_term:get(?PT_KEY, undefined) of
        undefined ->
            cache_profile();
        Profile ->
            Profile
    end.

-doc """
Returns policy depending on the current security profile.
""".
-spec policy
    (mqtt_default_bind) -> loopback | any;
    (dashboard_http_default_bind) -> loopback | any;
    (authn_not_configured) -> allow | deny;
    (authn_backend_failure) -> ignore | deny;
    (authz_backend_failure) -> ignore | deny;
    (authz_rule_render_failure) -> ignore | deny;
    (dashboard_unchanged_default_credentials) -> allow | deny;
    (access_control_hook_failure) -> ignore | interrupt;
    (outbound_tls_verify) -> verify_none | verify_peer;
    (authn_jwt_missing) -> ignore | deny;
    (internal_subscription_checks) -> boolean();
    (authz_context) -> legacy | restricted;
    (delayed_publish_reauthorization) -> boolean();
    (exhook_server_unavailable) -> honor_failed_action | deny;
    (exhook_message_publish_failure) -> ignore | deny;
    (plugin_install_sha256_binding) -> optional | required;
    (authn_builtin_default_autogenerate_password) -> boolean();
    (authn_builtin_default_manual_password_hash) -> sha256 | pbkdf2;
    (authn_builtin_accept_weak_password_hash) -> boolean();
    (authn_mnesia_mt_user_conflict_protection) -> boolean();
    (authz_default_include_mountpoint) -> boolean();
    (authz_mnesia_mt_rule_conflict_protection) -> boolean().
policy(mqtt_default_bind) ->
    case profile() of
        legacy -> any;
        hardened -> loopback
    end;
policy(dashboard_http_default_bind) ->
    case profile() of
        legacy -> any;
        hardened -> loopback
    end;
policy(authn_not_configured) ->
    case profile() of
        legacy -> allow;
        hardened -> deny
    end;
policy(authn_backend_failure) ->
    case profile() of
        legacy -> ignore;
        hardened -> deny
    end;
policy(authz_backend_failure) ->
    case profile() of
        legacy -> ignore;
        hardened -> deny
    end;
policy(authz_rule_render_failure) ->
    case profile() of
        legacy -> ignore;
        hardened -> deny
    end;
policy(dashboard_unchanged_default_credentials) ->
    case profile() of
        legacy -> allow;
        hardened -> deny
    end;
policy(access_control_hook_failure) ->
    case profile() of
        legacy -> ignore;
        hardened -> interrupt
    end;
policy(outbound_tls_verify) ->
    case profile() of
        legacy -> verify_none;
        hardened -> verify_peer
    end;
policy(authn_jwt_missing) ->
    case profile() of
        legacy -> ignore;
        hardened -> deny
    end;
policy(internal_subscription_checks) ->
    case profile() of
        legacy -> false;
        hardened -> true
    end;
policy(authz_context) ->
    case profile() of
        legacy -> legacy;
        hardened -> restricted
    end;
policy(delayed_publish_reauthorization) ->
    case profile() of
        legacy -> false;
        hardened -> true
    end;
policy(exhook_server_unavailable) ->
    case profile() of
        legacy -> honor_failed_action;
        hardened -> deny
    end;
policy(exhook_message_publish_failure) ->
    case profile() of
        legacy -> ignore;
        hardened -> deny
    end;
policy(plugin_install_sha256_binding) ->
    case profile() of
        legacy -> optional;
        hardened -> required
    end;
policy(authn_builtin_default_autogenerate_password) ->
    case profile() of
        legacy -> false;
        hardened -> true
    end;
policy(authn_builtin_default_manual_password_hash) ->
    case profile() of
        legacy -> sha256;
        hardened -> pbkdf2
    end;
policy(authn_builtin_accept_weak_password_hash) ->
    case profile() of
        legacy -> true;
        hardened -> false
    end;
policy(authn_mnesia_mt_user_conflict_protection) ->
    case profile() of
        legacy -> false;
        hardened -> true
    end;
policy(authz_default_include_mountpoint) ->
    case profile() of
        legacy -> false;
        hardened -> true
    end;
policy(authz_mnesia_mt_rule_conflict_protection) ->
    case profile() of
        legacy -> false;
        hardened -> true
    end.

-doc """
Clears the cached security profile. This function is intended for testing purposes only.
""".
clear_profile() ->
    persistent_term:erase(?PT_KEY).

%%---------------------------------------------------------------------
%% Internal functions
%%---------------------------------------------------------------------

cache_profile() ->
    Profile =
        case os:getenv(?PROFILE_ENV_VAR) of
            false ->
                ?PROFILE_DEFAULT;
            "" ->
                ?PROFILE_DEFAULT;
            "legacy" ->
                legacy;
            "hardened" ->
                hardened;
            Other ->
                Message = io_lib:format(
                    "Invalid security profile(~p) value: ~p. "
                    "Valid values are: `legacy', `hardened', or empty (defaulting to ~p).",
                    [?PROFILE_ENV_VAR, Other, ?PROFILE_DEFAULT]
                ),
                exit({invalid_security_profile, iolist_to_binary(Message)})
        end,
    _ = persistent_term:put(?PT_KEY, Profile),
    Profile.
