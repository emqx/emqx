%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ldap_authn).

-include_lib("emqx_authn/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("eldap/include/eldap.hrl").

-behaviour(hocon_schema).
-behaviour(emqx_authentication).

%% a compatible attribute for version 4.x
-define(ISENABLED_ATTR, "isEnabled").

-export([
    namespace/0,
    tags/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    refs/0,
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-import(proplists, [get_value/2, get_value/3]).
%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "authn".

tags() ->
    [<<"Authentication">>].

%% used for config check when the schema module is resolved
roots() ->
    [{?CONF_NS, hoconsc:mk(hoconsc:ref(?MODULE, mysql))}].

fields(ldap) ->
    [
        {mechanism, emqx_authn_schema:mechanism(password_based)},
        {backend, emqx_authn_schema:backend(ldap)},
        {password_attribute, fun password_attribute/1},
        {is_superuser_attribute, fun is_superuser_attribute/1},
        {query_timeout, fun query_timeout/1}
    ] ++ emqx_authn_schema:common_fields() ++ emqx_ldap:fields(config).

desc(ldap) ->
    ?DESC(ldap);
desc(_) ->
    undefined.

password_attribute(type) -> string();
password_attribute(desc) -> ?DESC(?FUNCTION_NAME);
password_attribute(default) -> <<"userPassword">>;
password_attribute(_) -> undefined.

is_superuser_attribute(type) -> string();
is_superuser_attribute(desc) -> ?DESC(?FUNCTION_NAME);
is_superuser_attribute(default) -> <<"isSuperuser">>;
is_superuser_attribute(_) -> undefined.

query_timeout(type) -> emqx_schema:duration_ms();
query_timeout(desc) -> ?DESC(?FUNCTION_NAME);
query_timeout(default) -> <<"5s">>;
query_timeout(_) -> undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

refs() ->
    [hoconsc:ref(?MODULE, ldap)].

create(_AuthenticatorID, Config) ->
    create(Config).

create(Config0) ->
    ResourceId = emqx_authn_utils:make_resource_id(?MODULE),
    {Config, State} = parse_config(Config0),
    {ok, _Data} = emqx_authn_utils:create_resource(ResourceId, emqx_ldap, Config),
    {ok, State#{resource_id => ResourceId}}.

update(Config0, #{resource_id := ResourceId} = _State) ->
    {Config, NState} = parse_config(Config0),
    case emqx_authn_utils:update_resource(emqx_ldap, Config, ResourceId) of
        {error, Reason} ->
            error({load_config_error, Reason});
        {ok, _} ->
            {ok, NState#{resource_id => ResourceId}}
    end.

destroy(#{resource_id := ResourceId}) ->
    _ = emqx_resource:remove_local(ResourceId),
    ok.

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(
    #{password := Password} = Credential,
    #{
        password_attribute := PasswordAttr,
        is_superuser_attribute := IsSuperuserAttr,
        query_timeout := Timeout,
        resource_id := ResourceId
    } = State
) ->
    case
        emqx_resource:simple_sync_query(
            ResourceId,
            {query, Credential, [PasswordAttr, IsSuperuserAttr, ?ISENABLED_ATTR], Timeout}
        )
    of
        {ok, []} ->
            ignore;
        {ok, [Entry | _]} ->
            is_enabled(Password, Entry, State);
        {error, Reason} ->
            ?TRACE_AUTHN_PROVIDER(error, "ldap_query_failed", #{
                resource => ResourceId,
                timeout => Timeout,
                reason => Reason
            }),
            ignore
    end.

parse_config(Config) ->
    State = lists:foldl(
        fun(Key, Acc) ->
            Value =
                case maps:get(Key, Config) of
                    Bin when is_binary(Bin) ->
                        erlang:binary_to_list(Bin);
                    Any ->
                        Any
                end,
            Acc#{Key => Value}
        end,
        #{},
        [password_attribute, is_superuser_attribute, query_timeout]
    ),
    {Config, State}.

%% To compatible v4.x
is_enabled(Password, #eldap_entry{attributes = Attributes} = Entry, State) ->
    IsEnabled = get_lower_bin_value(?ISENABLED_ATTR, Attributes, <<"true">>),
    case emqx_authn_utils:to_bool(IsEnabled) of
        true ->
            ensure_password(Password, Entry, State);
        _ ->
            {error, user_disabled}
    end.

ensure_password(
    Password,
    #eldap_entry{attributes = Attributes} = Entry,
    #{password_attribute := PasswordAttr} = State
) ->
    case get_value(PasswordAttr, Attributes) of
        undefined ->
            {error, no_password};
        [LDAPPassword | _] ->
            extract_hash_algorithm(LDAPPassword, Password, fun try_decode_passowrd/4, Entry, State)
    end.

extract_hash_algorithm(LDAPPassword, Password, OnFail, Entry, State) ->
    case
        re:run(
            LDAPPassword,
            "{([^{}]+)}(.+)",
            [{capture, all_but_first, list}, global]
        )
    of
        {match, [[HashTypeStr, PasswordHashStr]]} ->
            case emqx_utils:safe_to_existing_atom(string:to_lower(HashTypeStr)) of
                {ok, HashType} ->
                    PasswordHash = to_binary(PasswordHashStr),
                    verify_password(HashType, PasswordHash, Password, Entry, State);
                _Error ->
                    {error, invalid_hash_type}
            end;
        _ ->
            OnFail(LDAPPassword, Password, Entry, State)
    end.

%% this password is in LDIF format which is base64 encoding
try_decode_passowrd(LDAPPassword, Password, Entry, State) ->
    case safe_base64_decode(LDAPPassword) of
        {ok, Decode} ->
            extract_hash_algorithm(
                Decode,
                Password,
                fun(_, _, _, _) ->
                    {error, invalid_password}
                end,
                Entry,
                State
            );
        {error, Reason} ->
            {error, {invalid_password, Reason}}
    end.

verify_password(ssha, PasswordData, Password, Entry, State) ->
    case safe_base64_decode(PasswordData) of
        {ok, <<PasswordHash:20/binary, Salt/binary>>} ->
            verify_password(sha, PasswordHash, Salt, suffix, Password, Entry, State);
        {ok, _} ->
            {error, invalid_ssha_password};
        {error, Reason} ->
            {error, {invalid_password, Reason}}
    end;
verify_password(
    Algorithm,
    PasswordHash,
    Password,
    Entry,
    State
) ->
    verify_password(Algorithm, PasswordHash, <<>>, disable, Password, Entry, State).

verify_password(Algorithm, PasswordHash, Salt, Position, Password, Entry, State) ->
    Result = emqx_passwd:check_pass(
        #{name => Algorithm, salt_position => Position},
        Salt,
        PasswordHash,
        Password
    ),
    case Result of
        ok ->
            {ok, is_superuser(Entry, State)};
        Error ->
            Error
    end.

is_superuser(Entry, #{is_superuser_attribute := Attr} = _State) ->
    Value = get_lower_bin_value(Attr, Entry#eldap_entry.attributes, <<"false">>),
    #{is_superuser => emqx_authn_utils:to_bool(Value)}.

safe_base64_decode(Data) ->
    try
        {ok, base64:decode(Data)}
    catch
        _:Reason ->
            {error, {invalid_base64_data, Reason}}
    end.

get_lower_bin_value(Key, Proplists, Default) ->
    [Value | _] = get_value(Key, Proplists, [Default]),
    to_binary(string:to_lower(Value)).

to_binary(Value) ->
    erlang:list_to_binary(Value).
