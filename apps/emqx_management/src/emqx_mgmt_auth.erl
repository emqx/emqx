%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_mgmt_auth).
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(emqx_db_backup).

%% API
-export([mnesia/1]).
-boot_mnesia({mnesia, [boot]}).
-behaviour(emqx_config_handler).

-export([
    create/4,
    read/1,
    update/4,
    delete/1,
    list/0,
    init_bootstrap_file/0,
    format/1
]).

-export([authorize/3]).
-export([post_config_update/5]).

-export([backup_tables/0]).

%% Internal exports (RPC)
-export([
    do_update/4,
    do_delete/1,
    do_create_app/3,
    do_force_create_app/3
]).

-ifdef(TEST).
-export([create/5]).
-endif.

-define(APP, emqx_app).

-record(?APP, {
    name = <<>> :: binary() | '_',
    api_key = <<>> :: binary() | '_',
    api_secret_hash = <<>> :: binary() | '_',
    enable = true :: boolean() | '_',
    desc = <<>> :: binary() | '_',
    expired_at = 0 :: integer() | undefined | infinity | '_',
    created_at = 0 :: integer() | '_'
}).

mnesia(boot) ->
    ok = mria:create_table(?APP, [
        {type, set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, ?APP},
        {attributes, record_info(fields, ?APP)}
    ]).

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------

backup_tables() -> [?APP].

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

-spec init_bootstrap_file() -> ok | {error, _}.
init_bootstrap_file() ->
    File = bootstrap_file(),
    ?SLOG(debug, #{msg => "init_bootstrap_api_keys_from_file", file => File}),
    init_bootstrap_file(File).

create(Name, Enable, ExpiredAt, Desc) ->
    ApiSecret = generate_api_secret(),
    create(Name, ApiSecret, Enable, ExpiredAt, Desc).

create(Name, ApiSecret, Enable, ExpiredAt, Desc) ->
    case mnesia:table_info(?APP, size) < 100 of
        true -> create_app(Name, ApiSecret, Enable, ExpiredAt, Desc);
        false -> {error, "Maximum ApiKey"}
    end.

read(Name) ->
    case mnesia:dirty_read(?APP, Name) of
        [App] -> {ok, to_map(App)};
        [] -> {error, not_found}
    end.

update(Name, Enable, ExpiredAt, Desc) ->
    trans(fun ?MODULE:do_update/4, [Name, Enable, ExpiredAt, Desc]).

do_update(Name, Enable, ExpiredAt, Desc) ->
    case mnesia:read(?APP, Name, write) of
        [] ->
            mnesia:abort(not_found);
        [App0 = #?APP{enable = Enable0, desc = Desc0}] ->
            App =
                App0#?APP{
                    expired_at = ExpiredAt,
                    enable = ensure_not_undefined(Enable, Enable0),
                    desc = ensure_not_undefined(Desc, Desc0)
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

format(App = #{expired_at := ExpiredAt0, created_at := CreateAt}) ->
    ExpiredAt =
        case ExpiredAt0 of
            infinity -> <<"infinity">>;
            _ -> emqx_utils_calendar:epoch_to_rfc3339(ExpiredAt0, second)
        end,
    App#{
        expired_at => ExpiredAt,
        created_at => emqx_utils_calendar:epoch_to_rfc3339(CreateAt, second)
    }.

list() ->
    to_map(ets:match_object(?APP, #?APP{_ = '_'})).

authorize(<<"/api/v5/users", _/binary>>, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>};
authorize(<<"/api/v5/api_key", _/binary>>, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>};
authorize(<<"/api/v5/logout", _/binary>>, _ApiKey, _ApiSecret) ->
    {error, <<"not_allowed">>};
authorize(_Path, ApiKey, ApiSecret) ->
    Now = erlang:system_time(second),
    case find_by_api_key(ApiKey) of
        {ok, true, ExpiredAt, SecretHash} when ExpiredAt >= Now ->
            case emqx_dashboard_admin:verify_hash(ApiSecret, SecretHash) of
                ok -> ok;
                error -> {error, "secret_error"}
            end;
        {ok, true, _ExpiredAt, _SecretHash} ->
            {error, "secret_expired"};
        {ok, false, _ExpiredAt, _SecretHash} ->
            {error, "secret_disable"};
        {error, Reason} ->
            {error, Reason}
    end.

find_by_api_key(ApiKey) ->
    Fun = fun() -> mnesia:match_object(#?APP{api_key = ApiKey, _ = '_'}) end,
    case mria:ro_transaction(?COMMON_SHARD, Fun) of
        {atomic, [#?APP{api_secret_hash = SecretHash, enable = Enable, expired_at = ExpiredAt}]} ->
            {ok, Enable, ExpiredAt, SecretHash};
        _ ->
            {error, "not_found"}
    end.

ensure_not_undefined(undefined, Old) -> Old;
ensure_not_undefined(New, _Old) -> New.

to_map(Apps) when is_list(Apps) ->
    [to_map(App) || App <- Apps];
to_map(#?APP{name = N, api_key = K, enable = E, expired_at = ET, created_at = CT, desc = D}) ->
    #{
        name => N,
        api_key => K,
        enable => E,
        expired_at => ET,
        created_at => CT,
        desc => D,
        expired => is_expired(ET)
    }.

is_expired(undefined) -> false;
is_expired(ExpiredTime) -> ExpiredTime < erlang:system_time(second).

create_app(Name, ApiSecret, Enable, ExpiredAt, Desc) ->
    App =
        #?APP{
            name = Name,
            enable = Enable,
            expired_at = ExpiredAt,
            desc = Desc,
            created_at = erlang:system_time(second),
            api_secret_hash = emqx_dashboard_admin:hash(ApiSecret),
            api_key = list_to_binary(emqx_utils:gen_id(16))
        },
    case create_app(App) of
        {ok, Res} ->
            {ok, Res#{api_secret => ApiSecret}};
        Error ->
            Error
    end.

create_app(App = #?APP{api_key = ApiKey, name = Name}) ->
    trans(fun ?MODULE:do_create_app/3, [App, ApiKey, Name]).

force_create_app(NamePrefix, App = #?APP{api_key = ApiKey}) ->
    trans(fun ?MODULE:do_force_create_app/3, [App, ApiKey, NamePrefix]).

do_create_app(App, ApiKey, Name) ->
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

do_force_create_app(App, ApiKey, NamePrefix) ->
    case mnesia:match_object(?APP, #?APP{api_key = ApiKey, _ = '_'}, read) of
        [] ->
            NewName = generate_unique_name(NamePrefix),
            ok = mnesia:write(App#?APP{name = NewName});
        [#?APP{name = Name}] ->
            ok = mnesia:write(App#?APP{name = Name})
    end.

generate_unique_name(NamePrefix) ->
    New = list_to_binary(NamePrefix ++ emqx_utils:gen_id(16)),
    case mnesia:read(?APP, New) of
        [] -> New;
        _ -> generate_unique_name(NamePrefix)
    end.

trans(Fun, Args) ->
    case mria:transaction(?COMMON_SHARD, Fun, Args) of
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
            {ok, MP} = re:compile(<<"(\.+):(\.+$)">>, [ungreedy]),
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

add_bootstrap_file(File, Dev, MP, Line) ->
    case file:read_line(Dev) of
        {ok, Bin} ->
            case re:run(Bin, MP, [global, {capture, all_but_first, binary}]) of
                {match, [[AppKey, ApiSecret]]} ->
                    App =
                        #?APP{
                            enable = true,
                            expired_at = infinity,
                            desc = ?BOOTSTRAP_TAG,
                            created_at = erlang:system_time(second),
                            api_secret_hash = emqx_dashboard_admin:hash(ApiSecret),
                            api_key = AppKey
                        },
                    case force_create_app("from_bootstrap_file_", App) of
                        {ok, ok} ->
                            add_bootstrap_file(File, Dev, MP, Line + 1);
                        {error, Reason} ->
                            throw(#{file => File, line => Line, content => Bin, reason => Reason})
                    end;
                _ ->
                    Reason = "invalid_format",
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
        eof ->
            ok;
        {error, Reason} ->
            throw(#{file => File, line => Line, reason => Reason})
    end.
