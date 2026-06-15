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

%% Change to hardened in v7.0
-define(PROFILE_DEFAULT, legacy).

-export([profile/0, policy/1, clear_profile/0]).

%---------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-doc """
Returns a value depending on the current security profile.

Use this function only for introspection/logging purposes.
Do not rely on security profile values directly in the code logic,
use `emqx_security_profile:policy/1` instead.
""".
-spec profile() -> legacy | hardened.
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
    (authz_backend_failure) -> ignore | deny;
    (dashboard_unchanged_default_credentials) -> allow | deny.
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
policy(authz_backend_failure) ->
    case profile() of
        legacy -> ignore;
        hardened -> deny
    end;
policy(dashboard_unchanged_default_credentials) ->
    case profile() of
        legacy -> allow;
        hardened -> deny
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
