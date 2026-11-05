%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_streams_delete_worker).
-moduledoc "Remove stream data before completing registry deletion".
-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/2, enqueue/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(delete, {name :: emqx_streams_types:stream_name(), id :: emqx_streams_types:stream_id()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(atom(), pos_integer()) -> gen_server:start_ret().
start_link(Pool, Index) -> gen_server:start_link(?MODULE, {Pool, Index}, []).

-doc "Schedule deletion without waiting for data removal".
-spec enqueue(emqx_streams_types:stream_name(), emqx_streams_types:stream_id()) ->
    ok | {error, no_delete_worker}.
enqueue(Name, Id) ->
    case pick_worker(Id) of
        {ok, Pid} -> gen_server:cast(Pid, #delete{name = Name, id = Id});
        {error, _} = Error -> Error
    end.

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init({Pool, Index}) ->
    process_flag(trap_exit, true),
    true = gproc_pool:connect_worker(Pool, {Pool, Index}),
    {ok, #{pool => Pool, index => Index}}.

handle_call(_, _, State) -> {reply, {error, unknown_request}, State}.

handle_cast(#delete{name = Name, id = Id}, State) ->
    delete(Name, Id),
    {noreply, State}.

handle_info(_, State) -> {noreply, State}.

terminate(_, #{pool := Pool, index := Index}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Index}).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

%% The pool is missing when the services are stopped, and has no workers while they restart.
pick_worker(Id) ->
    try gproc_pool:pick_worker(?MODULE, Id) of
        false -> {error, no_delete_worker};
        Pid -> {ok, Pid}
    catch
        error:badarg -> {error, no_delete_worker}
    end.

delete(Name, Id) ->
    try
        do_delete(Name, Id)
    catch
        Class:Error:Stacktrace ->
            ?SLOG(error, #{
                msg => "streams_delete_failed",
                id => Id,
                class => Class,
                reason => Error,
                stacktrace => Stacktrace
            })
    after
        ?tp(streams_delete_finished, #{id => Id})
    end.

do_delete(Name, Id) ->
    case emqx_streams_registry:find(Name) of
        {ok, #{id := Id} = Stream} ->
            ok = emqx_streams_message_db:drop(Stream),
            ?tp(streams_delete_bulk_done, #{id => Id}),
            case emqx_streams_registry:delete(Name, Id) of
                ok -> ok;
                not_found -> ok;
                {error, Reason} -> error(Reason)
            end;
        _ ->
            ok
    end.
