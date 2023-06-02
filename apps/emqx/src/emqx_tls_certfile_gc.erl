%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Orphaned TLS certificates / keyfiles garbage collector
%%
%% This module is a worker process that periodically scans the mutable
%% certificates directory and removes any files that are not referenced
%% by _any_ TLS configuration in _any_ of the config roots. Such files
%% are called "orphans".
%%
%% In order to ensure safety, GC considers a file to be a candidate for
%% deletion (a "convict") only if it was considered an orphan twice in
%% a row. This should help avoid deleting files that are not yet in the
%% config but was already materialized on disk (e.g. during
%% `pre_config_update/3`).

-module(emqx_tls_certfile_gc).

-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% API
-export([
    start_link/0,
    start_link/1,
    run/0,
    force/0
]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Testing & maintenance
-export([
    orphans/0,
    orphans/1,
    convicts/2,
    collect_files/2
]).

-define(GC_INVERVAL, 5 * 60 * 1000).

-define(HAS_OWNER_READ(Mode), ((Mode band 8#00400) > 0)).
-define(HAS_OWNER_WRITE(Mode), ((Mode band 8#00200) > 0)).

-type filename() :: string().
-type fileinfo() :: #file_info{}.

-type st() :: #{
    orphans => #{filename() => fileinfo()},
    gc_interval => pos_integer(),
    next_gc_timer => reference()
}.

-type event() ::
    {collect, filename(), ok | {error, file:posix()}}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() ->
    {ok, pid()}.
start_link() ->
    start_link(?GC_INVERVAL).

-spec start_link(_Interval :: pos_integer()) ->
    {ok, pid()}.
start_link(Interval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Interval, []).

-spec run() ->
    {ok, _Events :: [event()]}.
run() ->
    gen_server:call(?MODULE, collect).

-spec force() ->
    {ok, _Events :: [event()]}.
force() ->
    % NOTE
    % Simulate a complete GC cycle by running it twice. Mostly useful in tests.
    {ok, Events1} = run(),
    {ok, Events2} = run(),
    {ok, Events1 ++ Events2}.

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

-spec init(_) ->
    {ok, st()}.
init(Interval) ->
    {ok, start_timer(#{gc_interval => Interval})}.

-spec handle_call(collect | _Call, gen_server:from(), st()) ->
    {reply, {ok, [event()]}, st()} | {noreply, st()}.
handle_call(collect, From, St) ->
    {ok, Events, StNext} = ?tp_span(
        tls_certfile_gc_manual,
        #{caller => From},
        collect(St, #{evhandler => {fun emqx_utils:cons/2, []}})
    ),
    {reply, {ok, Events}, restart_timer(StNext)};
handle_call(Call, From, St) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Call, from => From}),
    {noreply, St}.

-spec handle_cast(_Cast, st()) ->
    {noreply, st()}.
handle_cast(Cast, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Cast}),
    {noreply, State}.

-spec handle_info({timeout, reference(), collect}, st()) ->
    {noreply, st()}.
handle_info({timeout, TRef, collect}, St = #{next_gc_timer := TRef}) ->
    {ok, _, StNext} = ?tp_span(
        tls_certfile_gc_periodic,
        #{},
        collect(St, #{evhandler => {fun log_event/2, []}})
    ),
    {noreply, start_timer(StNext)}.

start_timer(St = #{gc_interval := Interval}) ->
    TRef = erlang:start_timer(Interval, self(), collect),
    St#{next_gc_timer => TRef}.

restart_timer(St = #{next_gc_timer := TRef}) ->
    ok = emqx_utils:cancel_timer(TRef),
    start_timer(St).

log_event({collect, Filename, ok}, _) ->
    ?tp(debug, "tls_certfile_gc_collected", #{filename => Filename});
log_event({collect, Filename, {error, Reason}}, _) ->
    ?tp(warning, "tls_certfile_gc_collect_error", #{filename => Filename, reason => Reason}).

%%--------------------------------------------------------------------
%% Internal functions
%% -------------------------------------------------------------------

collect(St, Opts) ->
    RootDir = emqx_utils_fs:canonicalize(emqx:mutable_certs_dir()),
    Orphans = orphans(RootDir),
    OrphansLast = maps:get(orphans, St, #{}),
    Convicts = convicts(Orphans, OrphansLast),
    Result = collect_files(Convicts, RootDir, Opts),
    {ok, Result, St#{orphans => maps:without(Convicts, Orphans)}}.

orphans() ->
    Dir = emqx_utils_fs:canonicalize(emqx:mutable_certs_dir()),
    orphans(Dir).

orphans(Dir) ->
    Certfiles = find_managed_files(fun is_managed_file/2, Dir),
    lists:foldl(
        fun(Root, Acc) ->
            References = find_references(Root),
            maps:without(References, Acc)
        end,
        Certfiles,
        emqx_config:get_root_names()
    ).

convicts(Orphans, OrphansLast) ->
    maps:fold(
        fun(AbsPath, #file_info{mtime = MTime, inode = Inode}, Acc) ->
            case maps:get(AbsPath, Orphans, undefined) of
                #file_info{mtime = MTime, inode = Inode} ->
                    % Certfile was not changed / recreated in the meantime
                    [AbsPath | Acc];
                _ ->
                    % Certfile was changed / created / recreated in the meantime
                    Acc
            end
        end,
        [],
        OrphansLast
    ).

find_managed_files(Filter, Dir) ->
    emqx_utils_fs:traverse_dir(
        fun
            (AbsPath, Info = #file_info{}, Acc) ->
                case Filter(AbsPath, Info) of
                    true -> Acc#{AbsPath => Info};
                    false -> Acc
                end;
            (AbsPath, {error, Reason}, Acc) ->
                ?SLOG(notice, "filesystem_object_inaccessible", #{
                    abspath => AbsPath,
                    reason => Reason
                }),
                Acc
        end,
        #{},
        Dir
    ).

is_managed_file(AbsPath, #file_info{type = Type, mode = Mode}) ->
    % NOTE
    % We consider a certfile is managed if: this is a regular file, owner has RW permission
    % and the filename looks like a managed filename.
    Type == regular andalso
        ?HAS_OWNER_READ(Mode) andalso
        ?HAS_OWNER_WRITE(Mode) andalso
        emqx_tls_lib:is_managed_ssl_file(AbsPath).

find_references(Root) ->
    Config = emqx_config:get_raw([Root]),
    fold_config(
        fun(Stack, Value, Acc) ->
            case is_file_reference(Stack) andalso is_string(Value) of
                true ->
                    Filename = emqx_schema:naive_env_interpolation(Value),
                    {stop, [emqx_utils_fs:canonicalize(Filename) | Acc]};
                false ->
                    {cont, Acc}
            end
        end,
        [],
        Config
    ).

is_file_reference(Stack) ->
    lists:any(
        fun(KP) -> lists:prefix(lists:reverse(KP), Stack) end,
        emqx_tls_lib:ssl_file_conf_keypaths()
    ).

is_string(Value) ->
    is_list(Value) orelse is_binary(Value).

%%

fold_config(FoldFun, AccIn, Config) ->
    fold_config(FoldFun, AccIn, [], Config).

fold_config(FoldFun, AccIn, Stack, Config) when is_map(Config) ->
    maps:fold(
        fun(K, SubConfig, Acc) ->
            fold_subconf(FoldFun, Acc, [K | Stack], SubConfig)
        end,
        AccIn,
        Config
    );
fold_config(FoldFun, Acc, Stack, []) ->
    fold_confval(FoldFun, Acc, Stack, []);
fold_config(FoldFun, Acc, Stack, String = [C | _]) when is_integer(C), C >= 0, C < 16#10FFFF ->
    fold_confval(FoldFun, Acc, Stack, String);
fold_config(FoldFun, Acc, Stack, Config) when is_list(Config) ->
    fold_confarray(FoldFun, Acc, Stack, 1, Config);
fold_config(FoldFun, Acc, Stack, Config) ->
    fold_confval(FoldFun, Acc, Stack, Config).

fold_confarray(FoldFun, AccIn, StackIn, I, [H | T]) ->
    Stack = [I | StackIn],
    case FoldFun(Stack, H, AccIn) of
        {cont, Acc} ->
            AccOut = fold_config(FoldFun, Acc, Stack, H),
            fold_confarray(FoldFun, AccOut, StackIn, I + 1, T);
        {stop, Acc} ->
            fold_confarray(FoldFun, Acc, StackIn, I + 1, T)
    end;
fold_confarray(_FoldFun, Acc, _Stack, _, []) ->
    Acc.

fold_subconf(FoldFun, AccIn, Stack, SubConfig) ->
    case FoldFun(Stack, SubConfig, AccIn) of
        {cont, Acc} ->
            fold_config(FoldFun, Acc, Stack, SubConfig);
        {stop, Acc} ->
            Acc
    end.

fold_confval(FoldFun, AccIn, Stack, ConfVal) ->
    case FoldFun(Stack, ConfVal, AccIn) of
        {_, Acc} ->
            Acc
    end.

%%

-spec collect_files([filename()], filename()) ->
    [event()].
collect_files(Filenames, RootDir) ->
    collect_files(Filenames, RootDir, #{evhandler => {fun emqx_utils:cons/2, []}}).

collect_files(Filenames, RootDir, Opts) ->
    {Handler, AccIn} = maps:get(evhandler, Opts),
    lists:foldl(
        fun(Filename, Acc) -> collect_file(Filename, RootDir, Handler, Acc) end,
        AccIn,
        Filenames
    ).

collect_file(Filename, RootDir, Handler, AccIn) ->
    case file:delete(Filename) of
        ok ->
            Acc = Handler({collect, Filename, ok}, AccIn),
            collect_parents(filename:dirname(Filename), RootDir, Handler, Acc);
        {error, _} = Error ->
            Handler({collect, Filename, Error}, AccIn)
    end.

collect_parents(RootDir, RootDir, _Handler, Acc) ->
    Acc;
collect_parents(ParentDir, RootDir, Handler, AccIn) ->
    case file:del_dir(ParentDir) of
        ok ->
            Acc = Handler({collect, ParentDir, ok}, AccIn),
            collect_parents(filename:dirname(ParentDir), RootDir, Handler, Acc);
        {error, eexist} ->
            AccIn;
        {error, _} = Error ->
            Handler({collect, ParentDir, Error}, AccIn)
    end.
