%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rsf_store).

-moduledoc """
Stores the programs in a `mria` table, which replicates them to every node.

The table is the source of truth: the hot path and the uploads read it. Each node
also keeps a copy of every program as a file in
`<data_dir>/plugins/emqx_rsf/programs/<name>`. The node that writes a program
refreshes its own files at once. Every node checks its files against the table
every `?SYNC_INTERVAL` milliseconds, so the other nodes follow within that time.
""".

-behaviour(gen_server).

-export([
    create_tables/0,
    start_link/0,
    list/0,
    lookup/1,
    put/4,
    delete/1,
    programs_dir/0,
    validate_name/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("emqx_rsf.hrl").

-define(SYNC_INTERVAL, 5000).
-define(CALL_TIMEOUT, 15_000).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(?PROGRAM_TAB, [
        {type, set},
        {rlog_shard, ?DB_SHARD},
        {storage, disc_copies},
        {record_name, ?PROGRAM_TAB},
        {attributes, record_info(fields, ?PROGRAM_TAB)}
    ]),
    ok = mria:wait_for_tables([?PROGRAM_TAB]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-doc "Return all programs, sorted by name.".
-spec list() -> [#?PROGRAM_TAB{}].
list() ->
    lists:keysort(#?PROGRAM_TAB.name, mnesia:dirty_match_object(#?PROGRAM_TAB{_ = '_'})).

-doc "Read one program from the local copy of the table. The hot path calls this.".
-spec lookup(binary()) -> {ok, #?PROGRAM_TAB{}} | {error, not_found}.
lookup(Name) ->
    case mnesia:dirty_read(?PROGRAM_TAB, Name) of
        [Program] -> {ok, Program};
        [] -> {error, not_found}
    end.

-doc "Create or replace a program. The caller must validate the name and the source first.".
-spec put(binary(), binary(), binary(), binary()) -> {ok, #?PROGRAM_TAB{}}.
put(Name, Runtime, Source, UpdatedBy) ->
    gen_server:call(?MODULE, {put, Name, Runtime, Source, UpdatedBy}, ?CALL_TIMEOUT).

-spec delete(binary()) -> ok | {error, not_found}.
delete(Name) ->
    gen_server:call(?MODULE, {delete, Name}, ?CALL_TIMEOUT).

-spec programs_dir() -> file:filename().
programs_dir() ->
    filename:join([emqx:data_dir(), "plugins", "emqx_rsf", "programs"]).

-doc "Accept a program name that matches the emqx-rsf/1 name rule (`PROTOCOL.md`, section 4).".
-spec validate_name(term()) -> ok | {error, binary()}.
validate_name(Name) when is_binary(Name) ->
    case re:run(Name, ?NAME_RE, [{capture, none}]) of
        match ->
            ok;
        nomatch ->
            {error, <<
                "a name has 1 to 64 characters from A-Z a-z 0-9 . _ - "
                "and must not start with '.'"
            >>}
    end;
validate_name(_) ->
    {error, <<"name must be a string">>}.

%%--------------------------------------------------------------------
%% gen_server
%%--------------------------------------------------------------------

init([]) ->
    State = #{written => #{}},
    {ok, sync_files(State)}.

handle_call({put, Name, Runtime, Source, UpdatedBy}, _From, State) ->
    Program = #?PROGRAM_TAB{
        name = Name,
        runtime = Runtime,
        source = Source,
        sha256 = emqx_rsf_client:version(Runtime, Source),
        updated_at_ms = erlang:system_time(millisecond),
        updated_by = UpdatedBy
    },
    {atomic, ok} = mria:transaction(?DB_SHARD, fun() -> mnesia:write(Program) end),
    ?LOG(info, #{msg => "rsf_program_saved", name => Name, sha256 => Program#?PROGRAM_TAB.sha256}),
    {reply, {ok, Program}, sync_files(State)};
handle_call({delete, Name}, _From, State) ->
    case lookup(Name) of
        {ok, _} ->
            {atomic, ok} = mria:transaction(?DB_SHARD, fun() ->
                mnesia:delete({?PROGRAM_TAB, Name})
            end),
            ?LOG(info, #{msg => "rsf_program_deleted", name => Name}),
            {reply, ok, sync_files(State)};
        {error, not_found} = Error ->
            {reply, Error, State}
    end;
handle_call(_Req, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sync, State) ->
    {noreply, sync_files(State)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% File cache
%%--------------------------------------------------------------------

%% Make the files match the table: write new and changed programs, and delete
%% files that have no program. `written' maps each name to the version of its
%% file, so an unchanged program costs no file I/O.
sync_files(#{written := Written0} = State) ->
    erlang:send_after(?SYNC_INTERVAL, self(), sync),
    Dir = programs_dir(),
    case filelib:ensure_path(Dir) of
        ok ->
            Programs = list(),
            Written1 = lists:foldl(fun(P, Acc) -> write_file(Dir, P, Acc) end, Written0, Programs),
            Names = [N || #?PROGRAM_TAB{name = N} <- Programs],
            ok = delete_stray_files(Dir, Names),
            State#{written => maps:with(Names, Written1)};
        {error, Reason} ->
            ?LOG(error, #{msg => "rsf_programs_dir_failed", dir => Dir, reason => Reason}),
            State
    end.

write_file(_Dir, #?PROGRAM_TAB{name = Name, sha256 = Sha}, Written) when
    map_get(Name, Written) =:= Sha
->
    Written;
write_file(Dir, #?PROGRAM_TAB{name = Name, sha256 = Sha, source = Source}, Written) ->
    %% A program name never starts with '.', so it stays in Dir, and a temp file
    %% name that starts with '.' cannot be the name of a program.
    Path = filename:join(Dir, binary_to_list(Name)),
    Tmp = filename:join(Dir, "." ++ binary_to_list(Name) ++ ".tmp"),
    maybe
        ok ?= file:write_file(Tmp, Source),
        ok ?= file:rename(Tmp, Path),
        Written#{Name => Sha}
    else
        {error, Reason} ->
            ?LOG(error, #{msg => "rsf_program_file_write_failed", name => Name, reason => Reason}),
            Written
    end.

delete_stray_files(Dir, Names) ->
    Keep = sets:from_list([binary_to_list(N) || N <- Names], [{version, 2}]),
    case file:list_dir(Dir) of
        {ok, Files} ->
            lists:foreach(
                fun(F) ->
                    sets:is_element(F, Keep) orelse file:delete(filename:join(Dir, F))
                end,
                Files
            );
        {error, _} ->
            ok
    end.
