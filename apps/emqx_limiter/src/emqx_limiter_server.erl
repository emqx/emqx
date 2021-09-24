%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% A hierachical token bucket algorithm
%% Note: this is not the linux HTB algorithm(http://luxik.cdi.cz/~devik/qos/htb/manual/theory.htm)
%% Algorithm:
%% 1. the root node periodically generates tokens and then distributes them
%% just like the oscillation of water waves
%% 2. the leaf node has a counter, which is the place where the token is actually held.
%% 3. other nodes only play the role of transmission, and the rate of the node is like a valve,
%% limiting the oscillation transmitted from the parent node

-module(emqx_limiter_server).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-export([ start_link/1, connect/2, info/2
        , name/1]).

-record(root, { rate :: rate()             %% number of tokens generated per period
              , period :: pos_integer()    %% token generation interval(second)
              , childs :: list(node_id())  %% node children
              , consumed :: non_neg_integer()
              }).

-record(zone, { id :: pos_integer()
              , name :: zone_name()
              , rate :: rate()
              , obtained :: non_neg_integer()       %% number of tokens obtained
              , childs :: list(node_id())
              }).

-record(bucket, { id :: pos_integer()
                , name :: bucket_name()
                , rate :: rate()
                , obtained :: non_neg_integer()
                , correction :: emqx_limiter_decimal:zero_or_float() %% token correction value
                , capacity :: capacity()
                , counter :: counters:counters_ref()
                , index :: index()
                }).

-record(state, { root :: undefined | root()
               , counter :: undefined | counters:counters_ref() %% current counter to alloc
               , index :: index()
               , zones :: #{zone_name() => node_id()}
               , nodes :: nodes()
               , type :: limiter_type()
               }).

%% maybe use maps is better, but record is fastter
-define(FIELD_OBTAINED, #zone.obtained).
-define(GET_FIELD(F, Node), element(F, Node)).
-define(CALL(Type, Msg), gen_server:call(name(Type), {?FUNCTION_NAME, Msg})).

-type node_id() :: pos_integer().
-type root() :: #root{}.
-type zone() :: #zone{}.
-type bucket() :: #bucket{}.
-type node_data() :: zone() | bucket().
-type nodes() :: #{node_id() => node_data()}.
-type zone_name() :: emqx_limiter_schema:zone_name().
-type limiter_type() :: emqx_limiter_schema:limiter_type().
-type bucket_name() :: emqx_limiter_schema:bucket_name().
-type rate() :: decimal().
-type flow() :: decimal().
-type capacity() :: decimal().
-type decimal() :: emqx_limiter_decimal:decimal().
-type state() :: #state{}.
-type index() :: pos_integer().

-export_type([index/0]).
-import(emqx_limiter_decimal, [add/2, sub/2, mul/2, add_to_counter/3, put_to_counter/3]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec connect(limiter_type(), bucket_name()) -> emqx_limiter_client:client().
connect(Type, Bucket) ->
    #{zone := Zone,
      aggregated := [Aggr, Capacity],
      per_client := [Client, ClientCapa]} = emqx:get_config([emqx_limiter, Type, bucket, Bucket]),
    case emqx_limiter_manager:find_counter(Type, Zone, Bucket) of
        {ok, Counter, Idx, Rate} ->
            if Client =/= infinity andalso (Client < Aggr orelse ClientCapa < Capacity) ->
                    emqx_limiter_client:create(Client, ClientCapa, Counter, Idx, Rate);
               true ->
                    emqx_limiter_client:make_ref(Counter, Idx, Rate)
            end;
        _ ->
            ?LOG(error, "can't find the bucket:~p which type is:~p~n", [Bucket, Type]),
            throw("invalid bucket")
    end.

-spec info(limiter_type(), atom()) -> term().
info(Type, Info) ->
    ?CALL(Type, Info).

-spec name(limiter_type()) -> atom().
name(Type) ->
    erlang:list_to_atom(io_lib:format("~s_~s", [?MODULE, Type])).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link(limiter_type()) -> _.
start_link(Type) ->
    gen_server:start_link({local, name(Type)}, ?MODULE, [Type], []).

%%--------------------------------------------------------------------
%%% gen_server callbacks
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, State :: term()} |
          {ok, State :: term(), Timeout :: timeout()} |
          {ok, State :: term(), hibernate} |
          {stop, Reason :: term()} |
          ignore.
init([Type]) ->
    State = #state{zones = #{},
                   nodes = #{},
                   type = Type,
                   index = 1},
    State2 = init_tree(Type, State),
    oscillate(State2#state.root#root.period),
    {ok, State2}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
          {reply, Reply :: term(), NewState :: term()} |
          {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()} |
          {reply, Reply :: term(), NewState :: term(), hibernate} |
          {noreply, NewState :: term()} |
          {noreply, NewState :: term(), Timeout :: timeout()} |
          {noreply, NewState :: term(), hibernate} |
          {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
          {stop, Reason :: term(), NewState :: term()}.
handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
          {noreply, NewState :: term()} |
          {noreply, NewState :: term(), Timeout :: timeout()} |
          {noreply, NewState :: term(), hibernate} |
          {stop, Reason :: term(), NewState :: term()}.
handle_cast(Req, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Req]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
          {noreply, NewState :: term()} |
          {noreply, NewState :: term(), Timeout :: timeout()} |
          {noreply, NewState :: term(), hibernate} |
          {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(oscillate, State) ->
    {noreply, oscillation(State)};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
                State :: term()) -> any().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()},
                  State :: term(),
                  Extra :: term()) -> {ok, NewState :: term()} |
          {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
%%--------------------------------------------------------------------
-spec format_status(Opt :: normal | terminate,
                    Status :: list()) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
oscillate(Interval) ->
    erlang:send_after(Interval, self(), ?FUNCTION_NAME).

%% @doc generate tokens, and then spread to leaf nodes
-spec oscillation(state()) -> state().
oscillation(#state{root = #root{rate = Flow,
                                period = Interval,
                                childs = ChildIds,
                                consumed = Consumed} = Root,
                   nodes = Nodes} = State) ->
    oscillate(Interval),
    Childs = get_orderd_childs(ChildIds, Nodes),
    {Alloced, Nodes2} = transverse(Childs, Flow, 0, Nodes),
    State#state{nodes = Nodes2,
                root = Root#root{consumed = Consumed + Alloced}}.

%% @doc horizontal spread
-spec transverse(list(node_data()),
                 flow(),
                 non_neg_integer(),
                 nodes()) -> {non_neg_integer(), nodes()}.
transverse([H | T], InFlow, Alloced, Nodes) when InFlow > 0 ->
    {NodeAlloced, Nodes2} = longitudinal(H, InFlow, Nodes),
    InFlow2 = sub(InFlow, NodeAlloced),
    Alloced2 = Alloced + NodeAlloced,
    transverse(T, InFlow2, Alloced2, Nodes2);

transverse(_, _, Alloced, Nodes) ->
    {Alloced, Nodes}.

%% @doc vertical spread
-spec longitudinal(node_data(), flow(), nodes()) ->
          {non_neg_integer(), nodes()}.
longitudinal(#zone{id = Id,
                   rate = Rate,
                   obtained = Obtained,
                   childs = ChildIds} = Node, InFlow, Nodes) ->
    Flow = erlang:min(InFlow, Rate),

    if Flow > 0 ->
            Childs = get_orderd_childs(ChildIds, Nodes),
            {Alloced, Nodes2} = transverse(Childs, Flow, 0, Nodes),
            if Alloced > 0 ->
                    {Alloced,
                     Nodes2#{Id => Node#zone{obtained = Obtained + Alloced}}};
               true ->
                    %% childs are empty or all counter childs are full
                    {0, Nodes}
            end;
       true ->
            {0, Nodes}
    end;

longitudinal(#bucket{id = Id,
                     rate = Rate,
                     capacity = Capacity,
                     correction = Correction,
                     counter = Counter,
                     index = Index,
                     obtained = Obtained} = Node, InFlow, Nodes) ->
    Flow = add(erlang:min(InFlow, Rate), Correction),

    Tokens = counters:get(Counter, Index),
    %% toknes's value mayb be a negative value(stolen from the future)
    Avaiable = erlang:min(if Tokens < 0 ->
                                  add(Capacity, Tokens);
                             true ->
                                  sub(Capacity, Tokens)
                          end, Flow),
    FixAvaiable = erlang:min(Capacity, Avaiable),
    if FixAvaiable > 0 ->
            {Alloced, Decimal} = add_to_counter(Counter, Index, FixAvaiable),

            {Alloced,
             Nodes#{Id => Node#bucket{obtained = Obtained + Alloced,
                                      correction = Decimal}}};
       true ->
            {0, Nodes}
    end.

-spec get_orderd_childs(list(node_id()), nodes()) -> list(node_data()).
get_orderd_childs(Ids, Nodes) ->
    Childs = [maps:get(Id, Nodes) || Id <- Ids],

    %% sort by obtained, avoid node goes hungry
    lists:sort(fun(A, B) ->
                       ?GET_FIELD(?FIELD_OBTAINED, A) < ?GET_FIELD(?FIELD_OBTAINED, B)
               end,
               Childs).

-spec init_tree(emqx_limiter_schema:limiter_type(), state()) -> state().
init_tree(Type, State) ->
    #{global := Global,
      zone := Zone,
      bucket := Bucket} = emqx:get_config([emqx_limiter, Type]),
    {Factor, Root} = make_root(Global, Zone),
    State2 = State#state{root = Root},
    {NodeId, State3} = make_zone(maps:to_list(Zone), Factor, 1, State2),
    State4 = State3#state{counter = counters:new(maps:size(Bucket),
                                                 [write_concurrency])},
    make_bucket(maps:to_list(Bucket), Factor, NodeId, State4).

-spec make_root(decimal(), hocon:config()) -> {number(), root()}.
make_root(Rate, Zone) ->
    ZoneNum = maps:size(Zone),
    Childs = lists:seq(1, ZoneNum),
    MiniPeriod = emqx_limiter_schema:minimum_period(),
    if Rate >= 1 ->
            {1, #root{rate = Rate,
                      period = MiniPeriod,
                      childs = Childs,
                      consumed = 0}};
       true ->
            Factor = 1 / Rate,
            {Factor, #root{rate = 1,
                           period = erlang:floor(Factor * MiniPeriod),
                           childs = Childs,
                           consumed = 0}}
    end.

make_zone([{Name, Rate} | T], Factor, NodeId, State) ->
    #state{zones = Zones, nodes = Nodes} = State,
    Zone = #zone{id = NodeId,
                 name = Name,
                 rate = mul(Rate, Factor),
                 obtained = 0,
                 childs = []},
    State2 = State#state{zones = Zones#{Name => NodeId},
                         nodes = Nodes#{NodeId => Zone}},
    make_zone(T, Factor, NodeId + 1, State2);

make_zone([], _, NodeId, State2) ->
    {NodeId, State2}.

make_bucket([{Name, Conf} | T], Factor, NodeId, State) ->
    #{zone := ZoneName,
      aggregated := [Rate, Capacity]} = Conf,
    {Counter, Idx, State2} = alloc_counter(ZoneName, Name, Rate, State),
    Node = #bucket{ id = NodeId
                  , name = Name
                  , rate = mul(Rate, Factor)
                  , obtained = 0
                  , correction = 0
                  , capacity = Capacity
                  , counter = Counter
                  , index = Idx},
    State3 = add_zone_child(NodeId, Node, ZoneName, State2),
    make_bucket(T, Factor, NodeId + 1, State3);

make_bucket([], _, _, State) ->
    State.

-spec alloc_counter(zone_name(), bucket_name(), rate(), state()) ->
          {counters:counters_ref(), pos_integer(), state()}.
alloc_counter(Zone, Bucket, Rate,
              #state{type = Type, counter = Counter, index = Index} = State) ->
    Path = emqx_limiter_manager:make_path(Type, Zone, Bucket),
    case emqx_limiter_manager:find_counter(Path) of
        undefined ->
            init_counter(Path, Counter, Index,
                         Rate, State#state{index = Index + 1});
        {ok, ECounter, EIndex, _} ->
            init_counter(Path, ECounter, EIndex, Rate, State)
    end.

init_counter(Path, Counter, Index, Rate, State) ->
    _ = put_to_counter(Counter, Index, 0),
    emqx_limiter_manager:insert_counter(Path, Counter, Index, Rate),
    {Counter, Index, State}.

-spec add_zone_child(node_id(), bucket(), zone_name(), state()) -> state().
add_zone_child(NodeId, Bucket, Name, #state{zones = Zones, nodes = Nodes} = State) ->
    ZoneId = maps:get(Name, Zones),
    #zone{childs = Childs} = Zone = maps:get(ZoneId, Nodes),
    Nodes2 = Nodes#{ZoneId => Zone#zone{childs = [NodeId | Childs]},
                    NodeId => Bucket},
    State#state{nodes = Nodes2}.
