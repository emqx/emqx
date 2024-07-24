%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_scram_http).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").

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

-define(OPTIONAL_USER_INFO_KEYS, [
    <<"is_superuser">>
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
    emqx_utils_scram:authenticate(AuthMethod, AuthData, AuthCache, RetrieveFun, OnErrFun, State);
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
        request_timeout := RequestTimeout
    } = State
) ->
    Request = emqx_authn_http:generate_request(Credential#{username := Username}, State),
    Response = emqx_resource:simple_sync_query(ResourceId, {Method, Request, RequestTimeout}),
    ?TRACE_AUTHN_PROVIDER("scram_http_response", #{
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
    end.

handle_response(Headers, Body) ->
    ContentType = proplists:get_value(<<"content-type">>, Headers),
    case safely_parse_body(ContentType, Body) of
        {ok, NBody} ->
            body_to_user_info(NBody);
        {error, Reason} = Error ->
            ?TRACE_AUTHN_PROVIDER(
                error,
                "parse_scram_http_response_failed",
                #{content_type => ContentType, body => Body, reason => Reason}
            ),
            Error
    end.

body_to_user_info(Body) ->
    Required0 = maps:with(?REQUIRED_USER_INFO_KEYS, Body),
    case maps:size(Required0) =:= erlang:length(?REQUIRED_USER_INFO_KEYS) of
        true ->
            case safely_convert_hex(Required0) of
                {ok, Required} ->
                    UserInfo0 = maps:merge(Required, maps:with(?OPTIONAL_USER_INFO_KEYS, Body)),
                    UserInfo1 = emqx_utils_maps:safe_atom_key_map(UserInfo0),
                    UserInfo = maps:merge(#{is_superuser => false}, UserInfo1),
                    {ok, UserInfo};
                Error ->
                    Error
            end;
        _ ->
            ?TRACE_AUTHN_PROVIDER("bad_response_body", #{http_body => Body}),
            {error, bad_response}
    end.

safely_parse_body(ContentType, Body) ->
    try
        parse_body(ContentType, Body)
    catch
        _Class:_Reason ->
            {error, invalid_body}
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

parse_body(<<"application/json", _/binary>>, Body) ->
    {ok, emqx_utils_json:decode(Body, [return_maps])};
parse_body(<<"application/x-www-form-urlencoded", _/binary>>, Body) ->
    Flags = ?REQUIRED_USER_INFO_KEYS ++ ?OPTIONAL_USER_INFO_KEYS,
    RawMap = maps:from_list(cow_qs:parse_qs(Body)),
    NBody = maps:with(Flags, RawMap),
    {ok, NBody};
parse_body(ContentType, _) ->
    {error, {unsupported_content_type, ContentType}}.

merge_scram_conf(Conf, State) ->
    maps:merge(maps:with([algorithm, iteration_count], Conf), State).
