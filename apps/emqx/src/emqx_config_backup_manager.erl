%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_config_backup_manager).

-behaviour(gen_server).

-include("logger.hrl").

%% API
-export([
    start_link/0,

    backup_and_write/2
]).

%% Internal API (tests, debug)
-export([
    flush/0
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,

    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(MAX_KEEP_BACKUP_CONFIGS, 10).

-define(backup, backup).
-define(backup_tref, backup_tref).

%% Calls/Casts/Infos
-record(backup, {contents :: iodata(), filename :: file:filename()}).
-record(flush_backup, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec backup_and_write(file:filename(), iodata()) -> ok.
backup_and_write(DestFilename, NewContents) ->
    %% this may fail, but we don't care
    %% e.g. read-only file system
    _ = filelib:ensure_dir(DestFilename),
    TmpFilename = DestFilename ++ ".tmp",
    case file:write_file(TmpFilename, NewContents) of
        ok ->
            backup_and_replace(DestFilename, TmpFilename);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_save_conf_file",
                hint =>
                    "The updated cluster config is not saved on this node, please check the file system.",
                filename => TmpFilename,
                reason => Reason
            }),
            %% e.g. read-only, it's not the end of the world
            ok
    end.

%%------------------------------------------------------------------------------
%% Internal API
%%------------------------------------------------------------------------------

%% For tests/debugging
flush() ->
    gen_server:call(?MODULE, #flush_backup{}).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_) ->
    process_flag(trap_exit, true),
    State = #{
        ?backup => undefined,
        ?backup_tref => undefined
    },
    {ok, State}.

terminate(_Reason, State) ->
    handle_flush_backup(State).

handle_call(#flush_backup{}, _From, State0) ->
    State = handle_flush_backup(State0),
    {reply, ok, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(#backup{contents = Contents, filename = DestFilename}, State0) ->
    State = handle_backup_request(State0, Contents, DestFilename),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#flush_backup{}, State0) ->
    State = handle_flush_backup(State0),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

handle_backup_request(#{?backup := {_DestFilename, _OlderContents}} = State, _, _) ->
    %% Older contents already enqueued
    State;
handle_backup_request(#{?backup := undefined} = State0, OldContents, DestFilename) ->
    State = State0#{?backup := {DestFilename, OldContents}},
    ensure_backup_timer(State).

handle_flush_backup(#{?backup := undefined} = State) ->
    %% Impossible, except if flush provoked manually
    State;
handle_flush_backup(#{?backup := {DestFilename, OldContents}} = State0) ->
    dump_backup(DestFilename, OldContents),
    State0#{?backup_tref := undefined, ?backup := undefined}.

ensure_backup_timer(#{?backup_tref := undefined} = State0) ->
    Timeout = emqx:get_config([config_backup_interval]),
    TRef = erlang:send_after(Timeout, self(), #flush_backup{}),
    State0#{?backup_tref := TRef};
ensure_backup_timer(State) ->
    State.

backup_and_replace(DestFilename, TmpFilename) ->
    case file:read_file(DestFilename) of
        {ok, OriginalContents} ->
            gen_server:cast(?MODULE, #backup{filename = DestFilename, contents = OriginalContents}),
            ok = file:rename(TmpFilename, DestFilename);
        {error, enoent} ->
            %% not created yet
            ok = file:rename(TmpFilename, DestFilename);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_read_current_conf_file",
                filename => DestFilename,
                reason => Reason
            }),
            ok
    end.

dump_backup(DestFilename, OldContents) ->
    BackupFilename = DestFilename ++ "." ++ now_time() ++ ".bak",
    case file:write_file(BackupFilename, OldContents) of
        ok ->
            ok = prune_backup_files(DestFilename);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_backup_conf_file",
                filename => BackupFilename,
                reason => Reason
            }),
            ok
    end.

prune_backup_files(Path) ->
    Files0 = filelib:wildcard(Path ++ ".*"),
    Re = "\\.[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{3}\\.bak$",
    Files = lists:filter(fun(F) -> re:run(F, Re) =/= nomatch end, Files0),
    Sorted = lists:reverse(lists:sort(Files)),
    {_Keeps, Deletes} = lists:split(min(?MAX_KEEP_BACKUP_CONFIGS, length(Sorted)), Sorted),
    lists:foreach(
        fun(F) ->
            case file:delete(F) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "failed_to_delete_backup_conf_file",
                        filename => F,
                        reason => Reason
                    }),
                    ok
            end
        end,
        Deletes
    ).

%% @private This is the same human-readable timestamp format as
%% hocon-cli generated app.<time>.config file name.
now_time() ->
    Ts = os:system_time(millisecond),
    {{Y, M, D}, {HH, MM, SS}} = calendar:system_time_to_local_time(Ts, millisecond),
    Res = io_lib:format(
        "~0p.~2..0b.~2..0b.~2..0b.~2..0b.~2..0b.~3..0b",
        [Y, M, D, HH, MM, SS, Ts rem 1000]
    ),
    lists:flatten(Res).
