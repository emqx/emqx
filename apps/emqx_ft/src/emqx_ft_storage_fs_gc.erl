%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Filesystem storage GC
%%
%% This is conceptually a part of the Filesystem storage backend, even
%% though it's tied to the backend module with somewhat narrow interface.

-module(emqx_ft_storage_fs_gc).

-include_lib("emqx_ft/include/emqx_ft_storage_fs.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([start_link/1]).

-export([collect/0]).
-export([collect/3]).
-export([reset/0]).
-export([reset/1]).

-behaviour(gen_server).
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).

-record(st, {
    next_gc_timer :: option(reference()),
    last_gc :: option(gcstats())
}).

-type gcstats() :: #gcstats{}.

-define(IS_ENABLED(INTERVAL), (is_integer(INTERVAL) andalso INTERVAL > 0)).

%%

start_link(Storage) ->
    gen_server:start_link(mk_server_ref(global), ?MODULE, Storage, []).

-spec collect() -> gcstats().
collect() ->
    gen_server:call(mk_server_ref(global), {collect, erlang:system_time()}, infinity).

-spec reset() -> ok | {error, _}.
reset() ->
    emqx_ft_storage:with_storage_type(local, fun reset/1).

-spec reset(emqx_ft_storage_fs:storage()) -> ok.
reset(Storage) ->
    gen_server:cast(mk_server_ref(global), {reset, gc_interval(Storage)}).

collect(Storage, Transfer, Nodes) ->
    gc_enabled(Storage) andalso cast_collect(mk_server_ref(global), Storage, Transfer, Nodes).

mk_server_ref(Name) ->
    % TODO
    {via, gproc, {n, l, {?MODULE, Name}}}.

%%

init(Storage) ->
    St = #st{},
    {ok, start_timer(gc_interval(Storage), St)}.

handle_call({collect, CalledAt}, _From, St) ->
    StNext = maybe_collect_garbage(CalledAt, St),
    {reply, StNext#st.last_gc, StNext};
handle_call(Call, From, St) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Call, from => From}),
    {noreply, St}.

handle_cast({collect, Storage, Transfer, [Node | Rest]}, St) ->
    ok = do_collect_transfer(Storage, Transfer, Node, St),
    case Rest of
        [_ | _] ->
            cast_collect(self(), Storage, Transfer, Rest);
        [] ->
            ok
    end,
    {noreply, St};
handle_cast({reset, Interval}, St) ->
    {noreply, start_timer(Interval, cancel_timer(St))};
handle_cast(Cast, St) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Cast}),
    {noreply, St}.

handle_info({timeout, TRef, collect}, St = #st{next_gc_timer = TRef}) ->
    StNext = do_collect_garbage(St),
    {noreply, start_timer(StNext#st{next_gc_timer = undefined})}.

do_collect_transfer(Storage, Transfer, Node, St = #st{}) when Node == node() ->
    Stats = try_collect_transfer(Storage, Transfer, complete, init_gcstats()),
    ok = maybe_report(Stats, St),
    ok;
do_collect_transfer(_Storage, _Transfer, _Node, _St = #st{}) ->
    % TODO
    ok.

cast_collect(Ref, Storage, Transfer, Nodes) ->
    gen_server:cast(Ref, {collect, Storage, Transfer, Nodes}).

maybe_collect_garbage(_CalledAt, St = #st{last_gc = undefined}) ->
    do_collect_garbage(St);
maybe_collect_garbage(CalledAt, St = #st{last_gc = #gcstats{finished_at = FinishedAt}}) ->
    case FinishedAt > CalledAt of
        true ->
            St;
        false ->
            start_timer(do_collect_garbage(cancel_timer(St)))
    end.

do_collect_garbage(St = #st{}) ->
    emqx_ft_storage:with_storage_type(local, fun(Storage) ->
        Stats = collect_garbage(Storage),
        ok = maybe_report(Stats, Storage),
        St#st{last_gc = Stats}
    end).

maybe_report(#gcstats{errors = Errors}, Storage) when map_size(Errors) > 0 ->
    ?tp(warning, "garbage_collection_errors", #{errors => Errors, storage => Storage});
maybe_report(#gcstats{} = _Stats, _Storage) ->
    ?tp(garbage_collection, #{stats => _Stats, storage => _Storage}).

start_timer(St) ->
    Interval = emqx_ft_storage:with_storage_type(local, fun gc_interval/1),
    start_timer(Interval, St).

start_timer(Interval, St = #st{next_gc_timer = undefined}) when ?IS_ENABLED(Interval) ->
    St#st{next_gc_timer = emqx_utils:start_timer(Interval, collect)};
start_timer(Interval, St) ->
    ?SLOG(warning, #{msg => "periodic_gc_disabled", interval => Interval}),
    St.

cancel_timer(St = #st{next_gc_timer = undefined}) ->
    St;
cancel_timer(St = #st{next_gc_timer = TRef}) ->
    ok = emqx_utils:cancel_timer(TRef),
    St#st{next_gc_timer = undefined}.

gc_enabled(Storage) ->
    ?IS_ENABLED(gc_interval(Storage)).

gc_interval(Storage) ->
    emqx_ft_conf:gc_interval(Storage).

%%

collect_garbage(Storage) ->
    Stats = init_gcstats(),
    {ok, Transfers} = emqx_ft_storage_fs:transfers(Storage),
    collect_garbage(Storage, Transfers, Stats).

collect_garbage(Storage, Transfers, Stats) ->
    finish_gcstats(
        maps:fold(
            fun(Transfer, TransferInfo, StatsAcc) ->
                % TODO: throttling?
                try_collect_transfer(Storage, Transfer, TransferInfo, StatsAcc)
            end,
            Stats,
            Transfers
        )
    ).

try_collect_transfer(Storage, Transfer, TransferInfo = #{}, Stats) ->
    % File transfer might still be incomplete.
    % Any outdated fragments and temporary files should be collectable. As a kind of
    % heuristic we only delete transfer directory itself only if it is also outdated
    % _and was empty at the start of GC_, as a precaution against races between
    % writers and GCs.
    Cutoff =
        case get_segments_ttl(Storage, TransferInfo) of
            TTL when is_integer(TTL) ->
                erlang:system_time(second) - TTL;
            undefined ->
                0
        end,
    {FragCleaned, Stats1} = collect_outdated_fragments(Storage, Transfer, Cutoff, Stats),
    {TempCleaned, Stats2} = collect_outdated_tempfiles(Storage, Transfer, Cutoff, Stats1),
    % TODO: collect empty directories separately
    case FragCleaned and TempCleaned of
        true ->
            collect_transfer_directory(Storage, Transfer, Cutoff, Stats2);
        false ->
            Stats2
    end;
try_collect_transfer(Storage, Transfer, complete, Stats) ->
    % File transfer is complete.
    % We should be good to delete fragments and temporary files with their respective
    % directories altogether.
    {_, Stats1} = collect_fragments(Storage, Transfer, Stats),
    {_, Stats2} = collect_tempfiles(Storage, Transfer, Stats1),
    Stats2.

collect_fragments(Storage, Transfer, Stats) ->
    Dirname = emqx_ft_storage_fs:get_subdir(Storage, Transfer, fragment),
    maybe_collect_directory(Dirname, true, Stats).

collect_tempfiles(Storage, Transfer, Stats) ->
    Dirname = emqx_ft_storage_fs:get_subdir(Storage, Transfer, temporary),
    maybe_collect_directory(Dirname, true, Stats).

collect_outdated_fragments(Storage, Transfer, Cutoff, Stats) ->
    Dirname = emqx_ft_storage_fs:get_subdir(Storage, Transfer, fragment),
    maybe_collect_directory(Dirname, filter_older_than(Cutoff), Stats).

collect_outdated_tempfiles(Storage, Transfer, Cutoff, Stats) ->
    Dirname = emqx_ft_storage_fs:get_subdir(Storage, Transfer, temporary),
    maybe_collect_directory(Dirname, filter_older_than(Cutoff), Stats).

collect_transfer_directory(Storage, Transfer, Cutoff, Stats) ->
    Dirname = emqx_ft_storage_fs:get_subdir(Storage, Transfer),
    Filter =
        case Stats of
            #gcstats{directories = 0} ->
                % Nothing were collected, this is a leftover from a past complete transfer GC.
                filter_older_than(Cutoff);
            #gcstats{} ->
                % Usual incomplete transfer GC, collect directories unconditionally.
                true
        end,
    case collect_empty_directory(Dirname, Filter, Stats) of
        {true, StatsNext} ->
            collect_parents(Dirname, get_segments_root(Storage), StatsNext);
        {false, StatsNext} ->
            StatsNext
    end.

filter_older_than(Cutoff) ->
    fun(_Filepath, #file_info{mtime = ModifiedAt}) -> ModifiedAt =< Cutoff end.

collect_parents(Dirname, Until, Stats) ->
    Parent = filename:dirname(Dirname),
    case is_same_filepath(Parent, Until) orelse file:del_dir(Parent) of
        true ->
            Stats;
        ok ->
            ?tp(garbage_collected_directory, #{path => Dirname}),
            collect_parents(Parent, Until, account_gcstat_directory(Stats));
        {error, eexist} ->
            Stats;
        {error, Reason} ->
            register_gcstat_error({directory, Parent}, Reason, Stats)
    end.

maybe_collect_directory(Dirpath, Filter, Stats) ->
    case filelib:is_dir(Dirpath) of
        true ->
            collect_filepath(Dirpath, Filter, Stats);
        false ->
            {true, Stats}
    end.

-spec collect_filepath(file:name(), Filter, gcstats()) -> {boolean(), gcstats()} when
    Filter :: boolean() | fun((file:name(), file:file_info()) -> boolean()).
collect_filepath(Filepath, Filter, Stats) ->
    case file:read_link_info(Filepath, [{time, posix}, raw]) of
        {ok, Fileinfo} ->
            collect_filepath(Filepath, Fileinfo, Filter, Stats);
        {error, Reason} ->
            {Reason == enoent, register_gcstat_error({path, Filepath}, Reason, Stats)}
    end.

collect_filepath(Filepath, #file_info{type = directory} = Fileinfo, Filter, Stats) ->
    collect_directory(Filepath, Fileinfo, Filter, Stats);
collect_filepath(Filepath, #file_info{type = regular} = Fileinfo, Filter, Stats) ->
    case filter_filepath(Filter, Filepath, Fileinfo) andalso file:delete(Filepath, [raw]) of
        false ->
            {false, Stats};
        ok ->
            ?tp(garbage_collected_file, #{path => Filepath}),
            {true, account_gcstat(Fileinfo, Stats)};
        {error, Reason} ->
            {Reason == enoent, register_gcstat_error({file, Filepath}, Reason, Stats)}
    end;
collect_filepath(Filepath, Fileinfo, _Filter, Stats) ->
    {false, register_gcstat_error({file, Filepath}, {unexpected, Fileinfo}, Stats)}.

collect_directory(Dirpath, Fileinfo, Filter, Stats) ->
    case file:list_dir(Dirpath) of
        {ok, Filenames} ->
            {Clean, StatsNext} = collect_files(Dirpath, Filenames, Filter, Stats),
            case Clean of
                true ->
                    collect_empty_directory(Dirpath, Fileinfo, Filter, StatsNext);
                false ->
                    {false, StatsNext}
            end;
        {error, Reason} ->
            {false, register_gcstat_error({directory, Dirpath}, Reason, Stats)}
    end.

collect_files(Dirname, Filenames, Filter, Stats) ->
    lists:foldl(
        fun(Filename, {Complete, StatsAcc}) ->
            Filepath = filename:join(Dirname, Filename),
            {Collected, StatsNext} = collect_filepath(Filepath, Filter, StatsAcc),
            {Collected andalso Complete, StatsNext}
        end,
        {true, Stats},
        Filenames
    ).

collect_empty_directory(Dirpath, Filter, Stats) ->
    case file:read_link_info(Dirpath, [{time, posix}, raw]) of
        {ok, Dirinfo} ->
            collect_empty_directory(Dirpath, Dirinfo, Filter, Stats);
        {error, Reason} ->
            {Reason == enoent, register_gcstat_error({directory, Dirpath}, Reason, Stats)}
    end.

collect_empty_directory(Dirpath, Dirinfo, Filter, Stats) ->
    case filter_filepath(Filter, Dirpath, Dirinfo) andalso file:del_dir(Dirpath) of
        false ->
            {false, Stats};
        ok ->
            ?tp(garbage_collected_directory, #{path => Dirpath}),
            {true, account_gcstat_directory(Stats)};
        {error, Reason} ->
            {false, register_gcstat_error({directory, Dirpath}, Reason, Stats)}
    end.

filter_filepath(Filter, _, _) when is_boolean(Filter) ->
    Filter;
filter_filepath(Filter, Filepath, Fileinfo) when is_function(Filter) ->
    Filter(Filepath, Fileinfo).

is_same_filepath(P1, P2) when is_binary(P1) andalso is_binary(P2) ->
    filename:absname(P1) == filename:absname(P2);
is_same_filepath(P1, P2) when is_list(P1) andalso is_list(P2) ->
    filename:absname(P1) == filename:absname(P2);
is_same_filepath(P1, P2) when is_binary(P1) ->
    is_same_filepath(P1, filepath_to_binary(P2)).

filepath_to_binary(S) ->
    unicode:characters_to_binary(S, unicode, file:native_name_encoding()).

get_segments_ttl(Storage, TransferInfo) ->
    clamp(emqx_ft_conf:segments_ttl(Storage), try_get_filemeta_ttl(TransferInfo)).

try_get_filemeta_ttl(#{filemeta := Filemeta}) ->
    maps:get(segments_ttl, Filemeta, undefined);
try_get_filemeta_ttl(#{}) ->
    undefined.

clamp({Min, Max}, V) ->
    min(Max, max(Min, V));
clamp(undefined, V) ->
    V.

%%

init_gcstats() ->
    #gcstats{started_at = erlang:system_time()}.

finish_gcstats(Stats) ->
    Stats#gcstats{finished_at = erlang:system_time()}.

account_gcstat(Fileinfo, Stats = #gcstats{files = Files, space = Space}) ->
    Stats#gcstats{
        files = Files + 1,
        space = Space + Fileinfo#file_info.size
    }.

account_gcstat_directory(Stats = #gcstats{directories = Directories}) ->
    Stats#gcstats{
        directories = Directories + 1
    }.

register_gcstat_error(Subject, Error, Stats = #gcstats{errors = Errors}) ->
    Stats#gcstats{errors = Errors#{Subject => Error}}.

%%

get_segments_root(Storage) ->
    emqx_ft_storage_fs:get_root(Storage).
