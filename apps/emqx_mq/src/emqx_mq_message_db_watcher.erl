%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_message_db_watcher).

-moduledoc """
Module monitoring new generation appearance for the message database.
The generations are monitored via very reduced `emqx_ds_client` usage.

IMPORTANT:
This is not a production-ready solution.
We should wait for `generation => last` support in `emqx_ds:trans/2`. 
""".

-include("emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-behaviour(gen_server).
-behaviour(emqx_ds_client).

-export([
    start_link/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% emqx_ds_client callbacks
-export([
    get_current_generation/3,
    on_advance_generation/4,
    get_iterator/4,
    on_new_iterator/5,
    on_unrecoverable_error/5,
    on_subscription_down/4
]).

-record(state, {
    ds_client :: emqx_ds_client:t(),
    gens :: #{emqx_ds:shard() => emqx_ds:generation()}
}).

-define(SUB_ID, []).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    DSClient0 = emqx_ds_client:new(?MODULE, #{}),
    {ok, DSClient, Gens} = emqx_mq_message_db:subscribe_regular_db_streams(DSClient0, ?SUB_ID, #{}),
    {ok, #state{ds_client = DSClient, gens = Gens}}.

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(Info, #state{ds_client = DSClient0, gens = Gens0} = State) ->
    case emqx_ds_client:dispatch_message(Info, DSClient0, Gens0) of
        ignore ->
            {noreply, State};
        {DSClient, Gens} ->
            {noreply, State#state{ds_client = DSClient, gens = Gens}};
        {data, ?SUB_ID, _Stream, _Handle, #ds_sub_reply{ref = SRef}} ->
            {DSClient, Gens} = emqx_ds_client:complete_stream(DSClient0, SRef, Gens0),
            {noreply, State#state{ds_client = DSClient, gens = Gens}}
    end.

%%--------------------------------------------------------------------
%% emqx_ds_client callbacks
%%--------------------------------------------------------------------

get_current_generation(?SUB_ID, Shard, Gens) ->
    Res =
        case Gens of
            #{Shard := Generation} ->
                Generation;
            _ ->
                0
        end,
    ?tp(warning, emqx_mq_message_db_watcher_get_current_generation, #{
        shard => Shard, generation => Res
    }),
    Res.

on_advance_generation(?SUB_ID, Shard, Generation, Gens) ->
    ?tp(warning, emqx_mq_message_db_watcher_on_advance_generation, #{
        shard => Shard, generation => Generation
    }),
    case Gens of
        #{Shard := LastGeneration} when LastGeneration < Generation ->
            ok = emqx_mq_message_db:set_last_regular_db_generation(Shard, Generation),
            Gens#{Shard => Generation};
        #{Shard := _Generation} ->
            Gens;
        _ ->
            ok = emqx_mq_message_db:set_last_regular_db_generation(Shard, Generation),
            Gens#{Shard => Generation}
    end.

get_iterator(?SUB_ID, Slab, _Stream, _Gens) ->
    ?tp(warning, emqx_mq_message_db_watcher_get_iterator, #{slab => Slab}),
    undefined.

on_new_iterator(?SUB_ID, _Slab, _Stream, _It, Gens) ->
    ?tp(warning, emqx_mq_message_db_watcher_on_new_iterator, #{slab => _Slab}),
    {subscribe, Gens}.

on_subscription_down(?SUB_ID, _Slab, _Stream, Gens) ->
    Gens.

on_unrecoverable_error(?SUB_ID, Slab, Stream, Error, Gens) ->
    ?tp(error, emqx_mq_message_db_watcher_unrecoverable_error, #{
        slab => Slab, stream => Stream, error => Error
    }),
    Gens.
