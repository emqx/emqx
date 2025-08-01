%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_auth).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("emqx_mgmt.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-behaviour(emqx_db_backup).

%% API
-export([create_tables/0]).

-behaviour(emqx_config_handler).

-export([
    create/5,
    read/1,
    update/5,
    delete/1,
    list/0,
    try_init_bootstrap_file/0,
    format/1
]).

-export([authorize/4]).
-export([post_config_update/5]).

-export([backup_tables/0, validate_mnesia_backup/1]).

-export([delete_all_keys_from_namespace/1]).

%% Internal exports (RPC)
-export([
    do_update/5,
    do_delete/1,
    do_create_app/1,
    do_force_create_app/1
]).

-ifdef(TEST).
-export([create/7]).
-export([trans/2, force_create_app/1]).
-export([init_bootstrap_file/1]).
-endif.

-define(APP, emqx_app).

-record(?APP, {
    name = <<>> :: binary() | '_',
    api_key = <<>> :: binary() | '_',
    api_secret_hash = <<>> :: binary() | '_',
    enable = true :: boolean() | '_',
    %% Since v5.4.0 the `desc` has changed to `extra`
    %% desc = <<>> :: binary() | '_',
    extra = #{} :: binary() | map() | '_',
    expired_at = 0 :: integer() | undefined | infinity | '_',
    created_at = 0 :: integer() | '_'
}).

-define(DEFAULT_HASH_LEN, 16).

create_tables() ->
    Fields = record_info(fields, ?APP),
    ok = mria:create_table(?APP, [
        {type, set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, ?APP},
        {attributes, Fields}
    ]),
    [?APP].

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------

backup_tables() -> {<<"api_keys">>, [?APP]}.

validate_mnesia_backup({schema, _Tab, CreateList} = Schema) ->
    case emqx_mgmt_data_backup:default_validate_mnesia_backup(Schema) of
        ok ->
            ok;
        _ ->
            case proplists:get_value(attributes, CreateList) of
                %% Since v5.4.0 the `desc` has changed to `extra`
                [name, api_key, api_secret_hash, enable, desc, expired_at, created_at] ->
                    ok;
                Fields ->
                    {error, {unknow_fields, Fields}}
            end
    end;
validate_mnesia_backup(_Other) ->
    ok.

post_config_update([api_key], _Req, NewConf, _OldConf, _AppEnvs) ->
    #{bootstrap_file := File} = NewConf,
    case init_bootstrap_file(File) of
        ok ->
            ?SLOG(debug, #{msg => "init_bootstrap_api_keys_from_file_ok", file => File});
        {error, Reason} ->
            Msg = "init_bootstrap_api_keys_from_file_failed",
            ?SLOG(error, #{msg => Msg, reason => Reason, file => File})
    end,
    ok.

-spec try_init_bootstrap_file() -> ok | {error, _}.
try_init_bootstrap_file() ->
    case mria_rlog:role() of
        core ->
            File = bootstrap_file(),
            ?SLOG(debug, #{msg => "init_bootstrap_api_keys_from_file", file => File}),
            _ = init_bootstrap_file(File),
            ok;
        _ ->
            ok
    end.

create(Name, Enable, ExpiredAt, Desc, Role) ->
    ApiKey = generate_unique_api_key(Name),
    ApiSecret = generate_api_secret(),
    create(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role).

create(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role) ->
    case mnesia:table_info(?APP, size) < 100 of
        true -> create_app(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role);
        false -> {error, "Maximum number of ApiKeys reached."}
    end.

read(Name) ->
    case mnesia:dirty_read(?APP, Name) of
        [App] -> {ok, to_map(App)};
        [] -> {error, not_found}
    end.

update(Name, Enable, ExpiredAt, Desc, Role0) ->
    case parse_role(Role0) of
        {ok, ParsedRole} ->
            trans(fun ?MODULE:do_update/5, [Name, Enable, ExpiredAt, Desc, ParsedRole]);
        Error ->
            Error
    end.

do_update(Name, Enable, ExpiredAt, Desc, #{?role := Role, ?namespace := Namespace}) ->
    case mnesia:read(?APP, Name, write) of
        [] ->
            mnesia:abort(not_found);
        [App0 = #?APP{enable = Enable0, extra = Extra0}] ->
            #{desc := Desc0} = Extra1 = normalize_extra(Extra0),
            PreviousNamespace = maps:get(?namespace, Extra1, ?global_ns),
            maybe
                true ?= PreviousNamespace /= Namespace,
                mnesia:abort(<<"changing_namespace_is_forbidden">>)
            end,
            Extra = emqx_utils_maps:put_if(
                Extra1#{?role => Role, desc => ensure_not_undefined(Desc, Desc0)},
                ?namespace,
                Namespace,
                is_binary(Namespace)
            ),
            App =
                App0#?APP{
                    expired_at = ExpiredAt,
                    enable = ensure_not_undefined(Enable, Enable0),
                    extra = Extra
                },
            ok = mnesia:write(App),
            to_map(App)
    end.

delete(Name) ->
    trans(fun ?MODULE:do_delete/1, [Name]).

do_delete(Name) ->
    case mnesia:read(?APP, Name) of
        [] -> mnesia:abort(not_found);
        [_App] -> mnesia:delete({?APP, Name})
    end.

delete_all_keys_from_namespace(Namespace) when is_binary(Namespace) ->
    MS = ets:fun2ms(
        fun(#?APP{name = Name, extra = #{?namespace := Ns}}) when Ns == Namespace ->
            Name
        end
    ),
    APIKeyNames = ets:select(?APP, MS),
    lists:foreach(fun delete/1, APIKeyNames).

format(App = #{expired_at := ExpiredAt, created_at := CreateAt}) ->
    format_app_extend(App#{
        expired_at => format_epoch(ExpiredAt),
        created_at => format_epoch(CreateAt)
    }).

format_epoch(infinity) ->
    <<"infinity">>;
format_epoch(Epoch) ->
    emqx_utils_calendar:epoch_to_rfc3339(Epoch, second).

list() ->
    to_map(ets:match_object(?APP, #?APP{_ = '_'})).

authorize(#{module := emqx_dashboard_api, function := user}, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>, <<"users">>};
authorize(#{module := emqx_dashboard_api, function := users}, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>, <<"users">>};
authorize(#{module := emqx_dashboard_api, function := logout}, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>, <<"logout">>};
authorize(#{module := emqx_mgmt_api_api_keys}, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>, <<"api_key">>};
authorize(HandlerInfo, Req, ApiKey, ApiSecret) ->
    Now = erlang:system_time(second),
    case find_by_api_key(ApiKey) of
        {ok, true, ExpiredAt, SecretHash, Role, Namespace} when ExpiredAt >= Now ->
            case emqx_dashboard_admin:verify_hash(ApiSecret, SecretHash) of
                ok -> check_rbac(Req, HandlerInfo, ApiKey, Role, Namespace);
                error -> {error, "secret_error"}
            end;
        {ok, true, _ExpiredAt, _SecretHash, _Role, _Namespace} ->
            {error, "secret_expired"};
        {ok, false, _ExpiredAt, _SecretHash, _Role, _Namespace} ->
            {error, "secret_disabled"};
        {error, Reason} ->
            {error, Reason}
    end.

find_by_api_key(ApiKey) ->
    Fun = fun() -> mnesia:match_object(#?APP{api_key = ApiKey, _ = '_'}) end,
    case mria:ro_transaction(?COMMON_SHARD, Fun) of
        {atomic, [
            #?APP{
                api_secret_hash = SecretHash,
                enable = Enable,
                expired_at = ExpiredAt,
                extra = Extra
            }
        ]} ->
            Role = get_role(Extra),
            Namespace = namespace_of(Extra),
            {ok, Enable, ExpiredAt, SecretHash, Role, Namespace};
        _ ->
            {error, "not_found"}
    end.

ensure_not_undefined(undefined, Old) -> Old;
ensure_not_undefined(New, _Old) -> New.

to_map(Apps) when is_list(Apps) ->
    [to_map(App) || App <- Apps];
to_map(#?APP{
    name = N, api_key = K, enable = E, expired_at = ET, created_at = CT, extra = Extra0
}) ->
    #{?role := Role, desc := Desc} = Extra = normalize_extra(Extra0),
    Namespace = maps:get(?namespace, Extra, ?global_ns),
    #{
        name => N,
        api_key => K,
        enable => E,
        expired_at => ET,
        created_at => CT,
        desc => Desc,
        expired => is_expired(ET),
        role => Role,
        namespace => Namespace
    }.

is_expired(undefined) -> false;
is_expired(ExpiredTime) -> ExpiredTime < erlang:system_time(second).

create_app(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role0) ->
    maybe
        {ok, #{?role := Role, ?namespace := Namespace}} ?= parse_role(Role0),
        ActorProps = #{?role => Role, ?namespace => Namespace},
        ok ?= emqx_hooks:run_fold('api_actor.pre_create', [ActorProps], ok),
        Extra = emqx_utils_maps:put_if(
            #{?role => Role, desc => Desc},
            ?namespace,
            Namespace,
            is_binary(Namespace)
        ),
        App =
            #?APP{
                name = Name,
                enable = Enable,
                expired_at = ExpiredAt,
                extra = Extra,
                created_at = erlang:system_time(second),
                api_secret_hash = emqx_dashboard_admin:hash(ApiSecret),
                api_key = ApiKey
            },
        {ok, Res} ?= trans(fun ?MODULE:do_create_app/1, [App]),
        {ok, Res#{api_secret => ApiSecret}}
    end.

force_create_app(App) ->
    trans(fun ?MODULE:do_force_create_app/1, [App]).

do_create_app(App = #?APP{api_key = ApiKey, name = Name}) ->
    case mnesia:read(?APP, Name) of
        [_] ->
            mnesia:abort(name_already_exists);
        [] ->
            case mnesia:match_object(?APP, #?APP{api_key = ApiKey, _ = '_'}, read) of
                [] ->
                    ok = mnesia:write(App),
                    to_map(App);
                _ ->
                    mnesia:abort(api_key_already_exists)
            end
    end.

do_force_create_app(App) ->
    _ = maybe_cleanup_api_key(App),
    ok = mnesia:write(App).

maybe_cleanup_api_key(#?APP{name = Name, api_key = ApiKey}) ->
    case mnesia:match_object(?APP, #?APP{api_key = ApiKey, _ = '_'}, read) of
        [] ->
            ok;
        [#?APP{name = Name}] ->
            ?SLOG(debug, #{
                msg => "same_apikey_detected",
                info => <<"The last `KEY:SECRET` in bootstrap file will be used.">>
            }),
            ok;
        Existed ->
            %% Duplicated or upgraded from old version:
            %% Which `Name` and `ApiKey` are not related in old version.
            %% So delete it/(them) and write a new record with a name strongly related to the apikey.
            %% The apikeys generated from the file do not have names.
            %% Generate a name for the apikey from the apikey itself by rule:
            %% Use `from_bootstrap_file_` as the prefix, and the first 16 digits of the
            %% sha512 hexadecimal value of the `ApiKey` as the suffix to form the name of the apikey.
            %% e.g. The name of the apikey: `example-api-key:secret_xxxx` is `from_bootstrap_file_53280fb165b6cd37`

            %% Note for EMQX-11844:
            %% emqx.conf has the highest priority
            %% if there is a key conflict, delete the old one and keep the key which from the bootstrap file
            ?SLOG(info, #{
                msg => "duplicated_apikey_detected",
                info => <<"Delete duplicated apikeys and write a new one from bootstrap file">>,
                keys => [EName || #?APP{name = EName} <- Existed]
            }),
            lists:foreach(
                fun(#?APP{name = N}) -> ok = mnesia:delete({?APP, N}) end, Existed
            ),
            ok
    end.

hash_string_from_seed(Seed, PrefixLen) ->
    <<Integer:512>> = crypto:hash(sha512, Seed),
    list_to_binary(string:slice(io_lib:format("~128.16.0b", [Integer]), 0, PrefixLen)).

%% Form Dashboard API Key pannel, only `Name` provided for users
generate_unique_api_key(Name) ->
    hash_string_from_seed(Name, ?DEFAULT_HASH_LEN).

%% Form BootStrap File, only `ApiKey` provided from file, no `Name`
generate_unique_name(NamePrefix, ApiKey) ->
    <<NamePrefix/binary, (hash_string_from_seed(ApiKey, ?DEFAULT_HASH_LEN))/binary>>.

trans(Fun, Args) ->
    case mria:sync_transaction(?COMMON_SHARD, Fun, Args) of
        {atomic, Res} -> {ok, Res};
        {aborted, Error} -> {error, Error}
    end.

generate_api_secret() ->
    Random = crypto:strong_rand_bytes(32),
    emqx_base62:encode(Random).

bootstrap_file() ->
    emqx:get_config([api_key, bootstrap_file], <<>>).

init_bootstrap_file(<<>>) ->
    ok;
init_bootstrap_file(File) ->
    case file:open(File, [read, binary]) of
        {ok, Dev} ->
            {ok, MP} = re:compile(<<"(\.+):(\.+)(?::(\.+))?$">>, [ungreedy]),
            init_bootstrap_file(File, Dev, MP);
        {error, Reason0} ->
            Reason = emqx_utils:explain_posix(Reason0),

            ?SLOG(
                error,
                #{
                    msg => "failed_to_open_the_bootstrap_file",
                    file => File,
                    reason => Reason
                }
            ),

            {error, Reason}
    end.

init_bootstrap_file(File, Dev, MP) ->
    try
        add_bootstrap_file(File, Dev, MP, 1)
    catch
        throw:Error -> {error, Error};
        Type:Reason:Stacktrace -> {error, {Type, Reason, Stacktrace}}
    after
        file:close(Dev)
    end.

-define(BOOTSTRAP_TAG, <<"Bootstrapped From File">>).
-define(FROM_BOOTSTRAP_FILE_PREFIX, <<"from_bootstrap_file_">>).

add_bootstrap_file(File, Dev, MP, Line) ->
    case read_line(Dev) of
        {ok, Bin} ->
            case parse_bootstrap_line(Bin, MP) of
                {ok, [ApiKey, ApiSecret, Role, Namespace]} ->
                    Extra = emqx_utils_maps:put_if(
                        #{desc => ?BOOTSTRAP_TAG, role => Role},
                        namespace,
                        Namespace,
                        is_binary(Namespace)
                    ),
                    App =
                        #?APP{
                            name = generate_unique_name(?FROM_BOOTSTRAP_FILE_PREFIX, ApiKey),
                            api_key = ApiKey,
                            api_secret_hash = emqx_dashboard_admin:hash(ApiSecret),
                            enable = true,
                            extra = Extra,
                            created_at = erlang:system_time(second),
                            expired_at = infinity
                        },
                    case force_create_app(App) of
                        {ok, ok} ->
                            add_bootstrap_file(File, Dev, MP, Line + 1);
                        {error, Reason} ->
                            throw(#{file => File, line => Line, content => Bin, reason => Reason})
                    end;
                {error, Reason} ->
                    ?SLOG(
                        error,
                        #{
                            msg => "failed_to_load_bootstrap_file",
                            file => File,
                            line => Line,
                            content => Bin,
                            reason => Reason
                        }
                    ),
                    throw(#{file => File, line => Line, content => Bin, reason => Reason})
            end;
        skip ->
            add_bootstrap_file(File, Dev, MP, Line + 1);
        eof ->
            ok;
        {error, Reason} ->
            throw(#{file => File, line => Line, reason => Reason})
    end.

read_line(Dev) ->
    case file:read_line(Dev) of
        {ok, Bin} ->
            case string:trim(Bin) of
                <<>> ->
                    skip;
                Result ->
                    {ok, Result}
            end;
        Result ->
            Result
    end.

parse_bootstrap_line(Bin, MP) ->
    case re:run(Bin, MP, [global, {capture, all_but_first, binary}]) of
        {match, [[_ApiKey, _ApiSecret] = Args]} ->
            Namespace = ?global_ns,
            {ok, Args ++ [?ROLE_API_DEFAULT, Namespace]};
        {match, [[ApiKey, ApiSecret, Role0]]} ->
            case parse_role(Role0) of
                {ok, #{?role := Role, ?namespace := Namespace}} ->
                    {ok, [ApiKey, ApiSecret, Role, Namespace]};
                _Error ->
                    {error, {"invalid_role", Role0}}
            end;
        _ ->
            {error, "invalid_format"}
    end.

get_role(#{?role := Role}) ->
    Role;
%% Before v5.4.0,
%% the field in the position of the `extra` is `desc` which is a binary for description
get_role(_Desc) ->
    ?ROLE_API_DEFAULT.

namespace_of(#{?namespace := Namespace}) when is_binary(Namespace) ->
    Namespace;
namespace_of(_) ->
    ?global_ns.

normalize_extra(Map) when is_map(Map) ->
    Map;
normalize_extra(Desc) ->
    #{desc => Desc, ?role => ?ROLE_API_DEFAULT, ?namespace => ?global_ns}.

check_rbac(Req, HandlerInfo, ApiKey, Role, Namespace) ->
    ActorContext = actor_context_of(ApiKey, Role, Namespace),
    case emqx_dashboard_rbac:check_rbac(Req, HandlerInfo, ActorContext) of
        {ok, ActorContextFinal} ->
            {ok, ActorContextFinal};
        false ->
            {error, unauthorized_role}
    end.

format_app_extend(App) ->
    App.

parse_role(Role) ->
    emqx_dashboard_rbac:parse_api_role(Role).

actor_context_of(ApiKey, Role, Namespace) ->
    #{
        ?actor => ApiKey,
        ?namespace => Namespace,
        ?role => Role
    }.
