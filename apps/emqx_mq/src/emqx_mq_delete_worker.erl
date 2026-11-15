%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mq_delete_worker).
-moduledoc "Remove queue data before completing registry deletion".
-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/2, enqueue/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(delete, {id :: emqx_mq_types:mqid()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(atom(), pos_integer()) -> gen_server:start_ret().
start_link(Pool, Index) -> gen_server:start_link(?MODULE, {Pool, Index}, []).

-doc "Schedule deletion without waiting for data removal".
-spec enqueue(emqx_mq_types:mqid()) -> ok | {error, no_delete_worker}.
enqueue(Id) ->
    case pick_worker(Id) of
        {ok, Pid} -> gen_server:cast(Pid, #delete{id = Id});
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

handle_cast(#delete{id = Id}, State) ->
    delete(Id),
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

delete(Id) ->
    try
        do_delete(Id)
    catch
        Class:Error:Stacktrace ->
            ?SLOG(error, #{
                msg => "mq_delete_failed",
                id => Id,
                class => Class,
                reason => Error,
                stacktrace => Stacktrace
            })
    after
        ?tp(mq_delete_finished, #{id => Id})
    end.

do_delete(Id) ->
    case emqx_mq_registry:find_by_id(Id) of
        not_found ->
            ok;
        {ok, MQ} ->
            ok = emqx_mq_message_db:drop(MQ),
            ?tp(mq_delete_bulk_done, #{id => Id}),
            %% Remove routing and clean up writes that arrived during the first pass.
            case emqx_mq_registry:delete_by_id(Id) of
                ok -> ok;
                not_found -> ok;
                {error, Reason} -> error(Reason)
            end
    end.
