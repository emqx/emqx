%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_auth).
-include_lib("emqx_mgmt.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").

-behaviour(emqx_db_backup).

%% API
-export([create_tables/0]).

-behaviour(emqx_config_handler).

-export([
    create/5,
    create/6,
    create/7,
    read/1,
    update/5,
    update/6,
    delete/1,
    list/0,
    try_init_bootstrap_file/0,
    format/1,
    check_scopes/3
]).

-export([authorize/4]).
-export([post_config_update/5]).

-export([backup_tables/0, validate_mnesia_backup/1]).

%% Internal exports (RPC)
-export([
    do_update/6,
    do_delete/1,
    do_create_app/1,
    do_force_create_app/1
]).

-ifdef(TEST).
-export([create/8]).
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
    create(Name, Enable, ExpiredAt, Desc, Role, undefined).

create(Name, Enable, ExpiredAt, Desc, Role, Scopes) ->
    ApiKey = generate_unique_api_key(Name),
    ApiSecret = generate_api_secret(),
    create(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role, Scopes).

create(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role) ->
    create(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role, undefined).

create(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role, Scopes) ->
    case mnesia:table_info(?APP, size) < 100 of
        true -> create_app(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role, Scopes);
        false -> {error, "Maximum number of ApiKeys reached."}
    end.

read(Name) ->
    case mnesia:dirty_read(?APP, Name) of
        [App] -> {ok, to_map(App)};
        [] -> {error, not_found}
    end.

update(Name, Enable, ExpiredAt, Desc, Role) ->
    update(Name, Enable, ExpiredAt, Desc, Role, undefined).

update(Name, Enable, ExpiredAt, Desc, Role, Scopes) ->
    case valid_role(Role) of
        ok ->
            trans(fun ?MODULE:do_update/6, [Name, Enable, ExpiredAt, Desc, Role, Scopes]);
        Error ->
            Error
    end.

do_update(Name, Enable, ExpiredAt, Desc, Role, Scopes) ->
    case mnesia:read(?APP, Name, write) of
        [] ->
            mnesia:abort(not_found);
        [App0 = #?APP{enable = Enable0, extra = Extra0}] ->
            #{desc := Desc0} = Extra = normalize_extra(Extra0),
            Extra1 = Extra#{desc := ensure_not_undefined(Desc, Desc0), role := Role},
            Extra2 = maybe_set_scopes(Extra1, Scopes),
            App =
                App0#?APP{
                    expired_at = ExpiredAt,
                    enable = ensure_not_undefined(Enable, Enable0),
                    extra = Extra2
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
authorize(#{module := emqx_dashboard_api, function := change_pwd}, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>, <<"users">>};
authorize(#{module := emqx_dashboard_api, function := change_mfa}, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>, <<"users">>};
authorize(#{module := emqx_mgmt_api_api_keys}, _Req, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>, <<"api_key">>};
authorize(HandlerInfo, Req, ApiKey, ApiSecret) ->
    Now = erlang:system_time(second),
    case find_by_api_key(ApiKey) of
        {ok, true, ExpiredAt, SecretHash, Role, Extra} when ExpiredAt >= Now ->
            case emqx_dashboard_admin:verify_hash(ApiSecret, SecretHash) of
                ok ->
                    case check_rbac(Req, ApiKey, Role) of
                        ok ->
                            check_scopes(Extra, HandlerInfo);
                        Error ->
                            Error
                    end;
                error ->
                    {error, "secret_error"}
            end;
        {ok, true, _ExpiredAt, _SecretHash, _Role, _Extra} ->
            {error, "secret_expired"};
        {ok, false, _ExpiredAt, _SecretHash, _Role, _Extra} ->
            {error, "secret_disable"};
        {error, Reason} ->
            {error, Reason}
    end.

find_by_api_key(ApiKey) ->
    Fun = fun() -> mnesia:match_object(#?APP{api_key = ApiKey, _ = '_'}) end,
    case mria:ro_transaction(?COMMON_SHARD, Fun) of
        {atomic, [
            #?APP{
                api_secret_hash = SecretHash, enable = Enable, expired_at = ExpiredAt, extra = Extra
            }
        ]} ->
            {ok, Enable, ExpiredAt, SecretHash, get_role(Extra), normalize_extra(Extra)};
        _ ->
            {error, "not_found"}
    end.

%% @doc Check if the request path is within the allowed scopes for this API key.
%% Denied scopes are always blocked, regardless of configuration.
%% If no scopes are configured (key not present or undefined), all non-denied paths are allowed.
%% If scopes is an empty list, all *mapped* paths are denied; unmapped paths
%% (e.g. status, prometheus data) remain allowed so an operator can still
%% reach the node and reconfigure the key via the UI.
%% Publisher role skips scope check (has its own hardcoded path restrictions).
%%
%% Path is obtained from minirest HandlerInfo (the route template, e.g., "/clients/:clientid"),
%% which is the same path registered by the API module's paths/0 callback.
%% This ensures scope lookup uses the exact route that cowboy matched,
%% eliminating the need for a parallel path matching implementation.
-spec check_scopes(map(), map()) -> ok | {error, unauthorized_role}.
check_scopes(Extra, HandlerInfo) ->
    Path = list_to_binary(maps:get(path, HandlerInfo)),
    case is_denied_path(Path) of
        true ->
            {error, unauthorized_role};
        false ->
            check_scopes_for_path(Extra, Path)
    end.

%% @doc Check scopes with explicit path and method (for external callers).
-spec check_scopes(map(), binary(), binary()) -> ok | {error, unauthorized_role}.
check_scopes(Extra, Path, _Method) ->
    case is_denied_path(Path) of
        true ->
            {error, unauthorized_role};
        false ->
            check_scopes_for_path(Extra, Path)
    end.

%% @doc Internal: check scopes for a path that has already passed denied check.
check_scopes_for_path(Extra, Path) ->
    case get_scopes(Extra) of
        undefined ->
            %% No scopes configured = all non-denied paths allowed (backward compat)
            ok;
        Scopes ->
            Role = maps:get(role, Extra, ?ROLE_API_DEFAULT),
            case Role of
                ?ROLE_API_PUBLISHER ->
                    %% Publisher role has its own hardcoded path restrictions,
                    %% skip scope check
                    ok;
                _ ->
                    check_path_in_scopes(Path, Scopes)
            end
    end.

check_path_in_scopes(Path, Scopes) ->
    case emqx_mgmt_api_key_scopes:path_to_scope(Path) of
        undefined ->
            %% Path not mapped to any scope — allow access regardless of
            %% whether `Scopes' is empty. This keeps unmapped public
            %% endpoints (status, prometheus data, etc.) reachable for
            %% keys whose scope list was filtered down to `[]' by the
            %% bootstrap loader, so an operator can still observe the
            %% node and reconfigure the key via the UI.
            ok;
        PathScope ->
            case lists:member(PathScope, Scopes) of
                true -> ok;
                false -> {error, unauthorized_role}
            end
    end.

%% @doc Check if a path belongs to a denied scope.
is_denied_path(Path) ->
    case emqx_mgmt_api_key_scopes:path_to_scope(Path) of
        undefined -> false;
        Scope -> emqx_mgmt_api_key_scopes:is_denied_scope(Scope)
    end.

get_scopes(#{scopes := Scopes}) when is_list(Scopes) ->
    Scopes;
get_scopes(#{scopes := undefined}) ->
    undefined;
get_scopes(_) ->
    %% No scopes key in extra = backward compatible, allow all
    undefined.

%% @doc Set scopes in extra map. undefined means "don't change".
maybe_set_scopes(Extra, undefined) ->
    Extra;
maybe_set_scopes(Extra, Scopes) when is_list(Scopes) ->
    Extra#{scopes => Scopes}.

ensure_not_undefined(undefined, Old) -> Old;
ensure_not_undefined(New, _Old) -> New.

to_map(Apps) when is_list(Apps) ->
    [to_map(App) || App <- Apps];
to_map(#?APP{
    name = N, api_key = K, enable = E, expired_at = ET, created_at = CT, extra = Extra0
}) ->
    #{role := Role, desc := Desc} = Extra = normalize_extra(Extra0),
    Base = #{
        name => N,
        api_key => K,
        enable => E,
        expired_at => ET,
        created_at => CT,
        desc => Desc,
        expired => is_expired(ET),
        role => Role
    },
    maybe_add_scopes(Base, Extra).

maybe_add_scopes(Map, #{scopes := Scopes}) when is_list(Scopes) ->
    Map#{scopes => Scopes};
maybe_add_scopes(Map, _) ->
    Map.

is_expired(undefined) -> false;
is_expired(ExpiredTime) -> ExpiredTime < erlang:system_time(second).

create_app(Name, ApiKey, ApiSecret, Enable, ExpiredAt, Desc, Role, Scopes) ->
    Extra0 = #{desc => Desc, role => Role},
    Extra = maybe_set_scopes(Extra0, Scopes),
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
    case create_app(App) of
        {ok, Res} ->
            {ok, Res#{api_secret => ApiSecret}};
        Error ->
            Error
    end.

create_app(App = #?APP{extra = #{role := Role}}) ->
    case valid_role(Role) of
        ok ->
            trans(fun ?MODULE:do_create_app/1, [App]);
        Error ->
            Error
    end.

force_create_app(App) ->
    trans(fun ?MODULE:do_force_create_app/1, [App]).

do_create_app(App = #?APP{api_key = ApiKey, name = Name}) ->
    case mnesia:read(?APP, Name) of
        [_] ->
            mnesia:abort(name_already_existed);
        [] ->
            case mnesia:match_object(?APP, #?APP{api_key = ApiKey, _ = '_'}, read) of
                [] ->
                    ok = mnesia:write(App),
                    to_map(App);
                _ ->
                    mnesia:abort(api_key_already_existed)
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
            {ok, MP} = re:compile(<<"([^:]+):([^:]+)(?::([^:]+))?(?::(.*))?$">>),
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
                {ok, {ApiKey, ApiSecret, Role, Scopes, Rejected}} ->
                    maybe_warn_rejected_scopes(File, Line, ApiKey, Rejected),
                    Extra0 = #{desc => ?BOOTSTRAP_TAG, role => Role},
                    Extra = maybe_set_scopes(Extra0, Scopes),
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
        {match, [[ApiKey, ApiSecret]]} ->
            {ok, {ApiKey, ApiSecret, ?ROLE_API_DEFAULT, undefined, []}};
        {match, [[ApiKey, ApiSecret, Role]]} ->
            case valid_role(Role) of
                ok ->
                    {ok, {ApiKey, ApiSecret, Role, undefined, []}};
                _Error ->
                    {error, {"invalid_role", Role}}
            end;
        {match, [[ApiKey, ApiSecret, Role, ScopesStr]]} ->
            case valid_role(Role) of
                ok ->
                    parse_bootstrap_scopes(ApiKey, ApiSecret, Role, ScopesStr);
                _Error ->
                    {error, {"invalid_role", Role}}
            end;
        _ ->
            {error, "invalid_format"}
    end.

%% @doc Parse scopes for a 4-segment line.
%%
%% Lenient by design: an unknown scope name is silently dropped and
%% reported via the second tuple element so the caller can warn. The
%% bootstrap file is treated as ops configuration — a typo in one
%% scope must not abort loading the whole file.
%%
%% Resulting `Scopes' semantics:
%%   `[]'  — explicit deny-all (e.g. `key:secret:role:' or all scope
%%           names were rejected as unknown).
%%   `[Bin, ...]' — the validated scopes, in original order.
parse_bootstrap_scopes(ApiKey, ApiSecret, Role, ScopesStr) ->
    Parsed = parse_scopes_str(ScopesStr),
    {Valid0, Rejected0} = emqx_mgmt_api_key_scopes:filter_valid_scopes(Parsed),
    %% Publisher API keys can only hold the `publish' scope. Drop any
    %% other scope names with a warning — same lenient policy as
    %% unknown scopes (typo in ops config must not abort the whole load).
    {Valid, Rejected} = filter_publisher_scopes(Role, Valid0, Rejected0),
    {ok, {ApiKey, ApiSecret, Role, Valid, Rejected}}.

%% Restrict publisher role to the `publish' scope only. Other roles
%% pass through unchanged. Returns updated {Valid, Rejected}.
filter_publisher_scopes(?ROLE_API_PUBLISHER, Valid, Rejected) ->
    {Keep, Drop} = lists:partition(
        fun(S) -> S =:= ?SCOPE_PUBLISH end,
        Valid
    ),
    {Keep, Rejected ++ Drop};
filter_publisher_scopes(_OtherRole, Valid, Rejected) ->
    {Valid, Rejected}.

%% @doc Parse the comma-separated scope string from the 4th bootstrap
%% segment. Each token is trimmed and lowercased; empty tokens are
%% dropped. Returns `[]' for an empty/whitespace-only input — that
%% maps to deny-all when written to the api_key record.
parse_scopes_str(<<>>) ->
    [];
parse_scopes_str(ScopesStr) ->
    Scopes = binary:split(ScopesStr, <<",">>, [global, trim_all]),
    [string:lowercase(string:trim(S)) || S <- Scopes, string:trim(S) =/= <<>>].

maybe_warn_rejected_scopes(_File, _Line, _ApiKey, []) ->
    ok;
maybe_warn_rejected_scopes(File, Line, ApiKey, Rejected) ->
    ?SLOG(warning, #{
        msg => "bootstrap_file_unknown_scopes_dropped",
        info => <<"Unknown scope names in bootstrap file were dropped; valid scopes still apply.">>,
        file => File,
        line => Line,
        api_key => ApiKey,
        rejected_scopes => Rejected
    }).

get_role(#{role := Role}) ->
    Role;
%% Before v5.4.0,
%% the field in the position of the `extra` is `desc` which is a binary for description
get_role(_Desc) ->
    ?ROLE_API_DEFAULT.

normalize_extra(Map) when is_map(Map) ->
    Map;
normalize_extra(Desc) ->
    #{desc => Desc, role => ?ROLE_API_DEFAULT}.

-if(?EMQX_RELEASE_EDITION == ee).
check_rbac(Req, ApiKey, Role) ->
    case emqx_dashboard_rbac:check_rbac(Req, ApiKey, Role) of
        true ->
            ok;
        _ ->
            {error, unauthorized_role}
    end.

format_app_extend(App) ->
    App.

valid_role(Role) ->
    emqx_dashboard_rbac:valid_api_role(Role).

-else.

check_rbac(_Req, _ApiKey, _Role) ->
    ok.

format_app_extend(App) ->
    maps:remove(role, App).

valid_role(?ROLE_API_DEFAULT) ->
    ok;
valid_role(_) ->
    {error, <<"Role does not exist">>}.

-endif.
