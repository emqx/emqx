%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gcp_device_authn).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, _Config) ->
    {ok, #{}}.

update(
    _Config,
    State
) ->
    {ok, State}.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(Credential, _State) ->
    check(Credential).

destroy(_State) ->
    emqx_gcp_device:clear_table(),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

% The check logic is the following:
%% 1. If clientid is not GCP-like or password is not a JWT, the result is ignore
%% 2. If clientid is GCP-like and password is a JWT, but expired, the result is password_error
%% 3. If clientid is GCP-like and password is a valid and not expired JWT:
%%  3.1 If there are no keys for the client, the result is ignore
%%  3.2 If there are some keys for the client:
%%   3.2.1 If there are no actual (not expired keys), the result is password_error
%%   3.2.2 If there are some actual keys and one of them matches the JWT, the result is success
%%   3.2.3 If there are some actual keys and none of them matches the JWT, the result is password_error
check(#{password := Password} = ClientInfo) ->
    case gcp_deviceid_from_clientid(ClientInfo) of
        {ok, DeviceId} ->
            case is_valid_jwt(Password) of
                true ->
                    check_jwt(ClientInfo, DeviceId);
                {false, not_a_jwt} ->
                    ?tp(authn_gcp_device_check, #{
                        result => ignore, reason => "not a JWT", client => ClientInfo
                    }),
                    ?TRACE_AUTHN_PROVIDER(debug, "auth_ignored", #{
                        reason => "not a JWT",
                        client => ClientInfo
                    }),
                    ignore;
                {false, expired} ->
                    ?tp(authn_gcp_device_check, #{
                        result => not_authorized, reason => "expired JWT", client => ClientInfo
                    }),
                    ?TRACE_AUTHN_PROVIDER(info, "auth_failed", #{
                        reason => "expired JWT",
                        client => ClientInfo
                    }),
                    {error, not_authorized}
            end;
        not_a_gcp_clientid ->
            ?tp(authn_gcp_device_check, #{
                result => ignore, reason => "not a GCP ClientId", client => ClientInfo
            }),
            ?TRACE_AUTHN_PROVIDER(debug, "auth_ignored", #{
                reason => "not a GCP ClientId",
                client => ClientInfo
            }),
            ignore
    end.

check_jwt(ClientInfo, DeviceId) ->
    case emqx_gcp_device:get_device_actual_keys(DeviceId) of
        not_found ->
            ?tp(authn_gcp_device_check, #{
                result => ignore, reason => "key not found", client => ClientInfo
            }),
            ?TRACE_AUTHN_PROVIDER(debug, "auth_ignored", #{
                reason => "key not found",
                client => ClientInfo
            }),
            ignore;
        Keys ->
            case any_key_matches(Keys, ClientInfo) of
                true ->
                    ?tp(authn_gcp_device_check, #{
                        result => ok, reason => "auth success", client => ClientInfo
                    }),
                    ?TRACE_AUTHN_PROVIDER(debug, "auth_success", #{
                        reason => "auth success",
                        client => ClientInfo
                    }),
                    ok;
                false ->
                    ?tp(authn_gcp_device_check, #{
                        result => {error, bad_username_or_password},
                        reason => "no matching or valid keys",
                        client => ClientInfo
                    }),
                    ?TRACE_AUTHN_PROVIDER(info, "auth_failed", #{
                        reason => "no matching or valid keys",
                        client => ClientInfo
                    }),
                    {error, bad_username_or_password}
            end
    end.

any_key_matches(Keys, ClientInfo) ->
    lists:any(fun(Key) -> key_matches(Key, ClientInfo) end, Keys).

key_matches(KeyRaw, #{password := Jwt} = _ClientInfo) ->
    Jwk = jose_jwk:from_pem(KeyRaw),
    case jose_jws:verify(Jwk, Jwt) of
        {true, _, _} ->
            true;
        {false, _, _} ->
            false
    end.

gcp_deviceid_from_clientid(#{clientid := <<"projects/", RestClientId/binary>>}) ->
    case binary:split(RestClientId, <<"/">>, [global]) of
        [
            _Project,
            <<"locations">>,
            _Location,
            <<"registries">>,
            _Registry,
            <<"devices">>,
            DeviceId
        ] ->
            {ok, DeviceId};
        _ ->
            not_a_gcp_clientid
    end;
gcp_deviceid_from_clientid(_ClientInfo) ->
    not_a_gcp_clientid.

is_valid_jwt(Password) ->
    Now = erlang:system_time(second),
    try jose_jwt:peek(Password) of
        #jose_jwt{fields = #{<<"exp">> := Exp}} when is_integer(Exp) andalso Exp >= Now ->
            true;
        #jose_jwt{fields = #{<<"exp">> := _Exp}} ->
            {false, expired};
        #jose_jwt{} ->
            true;
        _ ->
            {false, not_a_jwt}
    catch
        _:_ ->
            {false, not_a_jwt}
    end.
