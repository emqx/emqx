%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_redis).

-behaviour(emqx_authn_provider).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-include_lib("emqx_auth/include/emqx_authn.hrl").
-include("emqx_auth_redis.hrl").

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config0) ->
    maybe
        {ok, Config, State} ?= parse_config(Config0),
        ResourceId = emqx_authn_utils:make_resource_id(?AUTHN_BACKEND_BIN),
        {ok, _} ?=
            emqx_authn_utils:create_resource(
                ResourceId,
                emqx_redis,
                Config,
                ?AUTHN_MECHANISM_BIN,
                ?AUTHN_BACKEND_BIN
            ),
        {ok, State#{resource_id => ResourceId}}
    end.

update(Config0, #{resource_id := ResourceId} = _State) ->
    maybe
        {ok, Config, State} ?= parse_config(Config0),
        {ok, _} ?=
            emqx_authn_utils:update_resource(
                emqx_redis, Config, ResourceId, ?AUTHN_MECHANISM_BIN, ?AUTHN_BACKEND_BIN
            ),
        {ok, State#{resource_id => ResourceId}}
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    {error, bad_username_or_password};
authenticate(
    #{password := Password} = Credential,
    #{
        cmd := {CommandName, KeyTemplate, Fields},
        resource_id := ResourceId,
        password_hash_algorithm := Algorithm,
        cache_key_template := CacheKeyTemplate
    }
) ->
    NKey = emqx_auth_template:render_str(KeyTemplate, Credential),
    Command = [CommandName, NKey | Fields],
    CacheKey = emqx_auth_template:cache_key(Credential, CacheKeyTemplate),
    case emqx_authn_utils:cached_simple_sync_query(CacheKey, ResourceId, {cmd, Command}) of
        {ok, []} ->
            ignore;
        {ok, Values} ->
            case merge(Fields, Values) of
                Selected when Selected =/= #{} ->
                    case
                        emqx_authn_utils:check_password_from_selected_map(
                            Algorithm, Selected, Password
                        )
                    of
                        ok ->
                            {ok, emqx_authn_utils:is_superuser(Selected)};
                        {error, _Reason} = Error ->
                            Error
                    end;
                _ ->
                    ?TRACE_AUTHN_PROVIDER(info, "redis_query_not_matched", #{
                        resource => ResourceId,
                        cmd => Command,
                        keys => NKey,
                        fields => Fields
                    }),
                    ignore
            end;
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "redis_query_failed", #{
                resource => ResourceId,
                cmd => Command,
                keys => NKey,
                fields => Fields,
                reason => Reason
            }),
            ignore
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

parse_config(
    #{
        cmd := CmdStr,
        password_hash_algorithm := Algorithm
    } = Config
) ->
    case parse_cmd(CmdStr) of
        {ok, Vars, Cmd} ->
            ok = emqx_authn_password_hashing:init(Algorithm),
            ok = emqx_authn_utils:ensure_apps_started(Algorithm),
            State = maps:with([password_hash_algorithm, salt_position], Config),
            CacheKeyTemplate = emqx_auth_template:cache_key_template(Vars),
            {ok, Config, State#{cmd => Cmd, cache_key_template => CacheKeyTemplate}};
        {error, _} = Error ->
            Error
    end.

parse_cmd(CmdStr) ->
    case emqx_redis_command:split(CmdStr) of
        {ok, Cmd} ->
            case validate_cmd(Cmd) of
                ok ->
                    [CommandName, Key | Fields] = Cmd,
                    {Vars, Command} = emqx_authn_utils:parse_str(Key),
                    {ok, Vars, {CommandName, Command, Fields}};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

validate_cmd(Cmd) ->
    emqx_auth_redis_validations:validate_command(
        [
            not_empty,
            {command_name, [<<"hget">>, <<"hmget">>]},
            {allowed_fields, [<<"password_hash">>, <<"password">>, <<"salt">>, <<"is_superuser">>]},
            {required_field_one_of, [<<"password_hash">>, <<"password">>]}
        ],
        Cmd
    ).

merge(Fields, Value) when not is_list(Value) ->
    merge(Fields, [Value]);
merge(Fields, Values) ->
    maps:from_list(
        [
            {K, V}
         || {K, V} <- lists:zip(Fields, Values), V =/= undefined
        ]
    ).
