%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Note:
%% This is not an implementation of the RFC 7804:
%%   Salted Challenge Response HTTP Authentication Mechanism.
%% This backend is an implementation of scram,
%% which uses an external web resource as a source of user information.

-module(emqx_authn_scram_restapi).

-feature(maybe_expr, enable).

-include("emqx_auth_http.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-define(REQUIRED_USER_INFO_KEYS, [
    <<"stored_key">>,
    <<"server_key">>,
    <<"salt">>
]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config0) ->
    emqx_authn_http:with_validated_config(Config0, fun(Config, State) ->
        ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
        % {Config, State} = parse_config(Config0),
        {ok, _Data} = emqx_authn_utils:create_resource(
            ResourceId,
            emqx_bridge_http_connector,
            Config
        ),
        {ok, merge_scram_conf(Config, State#{resource_id => ResourceId})}
    end).

update(Config0, #{resource_id := ResourceId} = _State) ->
    emqx_authn_http:with_validated_config(Config0, fun(Config, NState) ->
        % {Config, NState} = parse_config(Config0),
        case emqx_authn_utils:update_resource(emqx_bridge_http_connector, Config, ResourceId) of
            {error, Reason} ->
                error({load_config_error, Reason});
            {ok, _} ->
                {ok, merge_scram_conf(Config, NState#{resource_id => ResourceId})}
        end
    end).

authenticate(
    #{
        auth_method := AuthMethod,
        auth_data := AuthData,
        auth_cache := AuthCache
    } = Credential,
    State
) ->
    RetrieveFun = fun(Username) ->
        retrieve(Username, Credential, State)
    end,
    OnErrFun = fun(Msg, Reason) ->
        ?TRACE_AUTHN_PROVIDER(Msg, #{
            reason => Reason
        })
    end,
    emqx_utils_scram:authenticate(
        AuthMethod, AuthData, AuthCache, State, RetrieveFun, OnErrFun, ?AUTHN_DATA_FIELDS
    );
authenticate(_Credential, _State) ->
    ignore.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

retrieve(
    Username,
    Credential,
    #{
        resource_id := ResourceId,
        method := Method,
        request_timeout := RequestTimeout,
        cache_key_template := CacheKeyTemplate
    } = State
) ->
    case emqx_authn_http:generate_request(Credential#{username := Username}, State) of
        {ok, Request} ->
            CacheKey = emqx_auth_template:cache_key(Credential, CacheKeyTemplate),
            Response = emqx_authn_utils:cached_simple_sync_query(
                CacheKey, ResourceId, {Method, Request, RequestTimeout}
            ),
            ?TRACE_AUTHN_PROVIDER("scram_restapi_response", #{
                request => emqx_authn_http:request_for_log(Credential, State),
                response => emqx_authn_http:response_for_log(Response),
                resource => ResourceId
            }),
            case Response of
                {ok, 200, Headers, Body} ->
                    handle_response(Headers, Body);
                {ok, _StatusCode, _Headers} ->
                    {error, bad_response};
                {ok, _StatusCode, _Headers, _Body} ->
                    {error, bad_response};
                {error, _Reason} = Error ->
                    Error
            end;
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER("generate_request_failed", #{
                reason => Reason
            }),
            {error, Reason}
    end.

handle_response(Headers, Body) ->
    ContentType = proplists:get_value(<<"content-type">>, Headers),
    maybe
        {ok, NBody} ?= emqx_authn_http:safely_parse_body(ContentType, Body),
        {ok, UserInfo} ?= body_to_user_info(NBody),
        {ok, AuthData} ?= emqx_authn_http:extract_auth_data(scram_restapi, NBody),
        {ok, maps:merge(AuthData, UserInfo)}
    end.

body_to_user_info(Body) ->
    Required0 = maps:with(?REQUIRED_USER_INFO_KEYS, Body),
    case maps:size(Required0) =:= erlang:length(?REQUIRED_USER_INFO_KEYS) of
        true ->
            case safely_convert_hex(Required0) of
                {ok, Required} ->
                    {ok, emqx_utils_maps:safe_atom_key_map(Required)};
                Error ->
                    ?TRACE_AUTHN_PROVIDER("decode_keys_failed", #{http_body => Body}),
                    Error
            end;
        _ ->
            ?TRACE_AUTHN_PROVIDER("missing_requried_keys", #{http_body => Body}),
            {error, bad_response}
    end.

safely_convert_hex(Required) ->
    try
        {ok,
            maps:map(
                fun(_Key, Hex) ->
                    binary:decode_hex(Hex)
                end,
                Required
            )}
    catch
        _Class:Reason ->
            {error, Reason}
    end.

merge_scram_conf(Conf, State) ->
    maps:merge(maps:with([algorithm, iteration_count], Conf), State).
