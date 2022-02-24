%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% A hierarchical token bucket algorithm
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

-export([ start_link/1, connect/2, info/1
        , name/1, get_initial_val/1]).

-type root() :: #{ rate := rate()             %% number of tokens generated per period
                 , burst := rate()
                 , period := pos_integer()    %% token generation interval(second)
                 , childs := list(node_id())  %% node children
                 , consumed := non_neg_integer()
                 }.

-type zone() :: #{ id := node_id()
                 , name := zone_name()
                 , rate := rate()
                 , burst := rate()
                 , obtained := non_neg_integer()       %% number of tokens obtained
                 , childs := list(node_id())
                 }.

-type bucket() :: #{ id := node_id()
                   , name := bucket_name()
                   , zone := zone_name()               %% pointer to zone node, use for burst
                   , rate := rate()
                   , obtained := non_neg_integer()
                   , correction := emqx_limiter_decimal:zero_or_float() %% token correction value
                   , capacity := capacity()
                   , counter := undefined | counters:counters_ref()
                   , index := undefined | index()
                   }.

-type state() :: #{ root := undefined | root()
                  , counter := undefined | counters:counters_ref() %% current counter to alloc
                  , index := index()
                  , zones := #{zone_name() => node_id()}
                  , buckets := list(node_id())
                  , nodes := nodes()
                  , type := limiter_type()
                  }.

-type node_id() :: pos_integer().
-type node_data() :: zone() | bucket().
-type nodes() :: #{node_id() => node_data()}.
-type zone_name() :: emqx_limiter_schema:zone_name().
-type limiter_type() :: emqx_limiter_schema:limiter_type().
-type bucket_name() :: emqx_limiter_schema:bucket_name().
-type rate() :: decimal().
-type flow() :: decimal().
-type capacity() :: decimal().
-type decimal() :: emqx_limiter_decimal:decimal().
-type index() :: pos_integer().

-define(CALL(Type), gen_server:call(name(Type), ?FUNCTION_NAME)).
-define(OVERLOAD_MIN_ALLOC, 0.3).  %% minimum coefficient for overloaded limiter

-export_type([index/0]).
-import(emqx_limiter_decimal, [add/2, sub/2, mul/2, put_to_counter/3]).

-elvis([{elvis_style, no_if_expression, disable}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec connect(limiter_type(),
              bucket_name() | #{limiter_type() => bucket_name()}) -> emqx_htb_limiter:limiter().
connect(Type, BucketName) when is_atom(BucketName) ->
    Path = [limiter, Type, bucket, BucketName],
    case emqx:get_config(Path, undefined) of
        undefined ->
            ?SLOG(error, #{msg => "bucket_config_not_found", path => Path}),
            throw("bucket's config not found");
        #{zone := Zone,
          aggregated := #{rate := AggrRate, capacity := AggrSize},
          per_client := #{rate := CliRate, capacity := CliSize} = Cfg} ->
            case emqx_limiter_manager:find_bucket(Type, Zone, BucketName) of
                {ok, Bucket} ->
                    if CliRate < AggrRate orelse CliSize < AggrSize ->
                            emqx_htb_limiter:make_token_bucket_limiter(Cfg, Bucket);
                       Bucket =:= infinity ->
                            emqx_htb_limiter:make_infinity_limiter(Cfg);
                       true ->
                            emqx_htb_limiter:make_ref_limiter(Cfg, Bucket)
                    end;
                undefined ->
                    ?SLOG(error, #{msg => "bucket_not_found", path => Path}),
                    throw("invalid bucket")
            end
    end;

connect(Type, Names) ->
    connect(Type, maps:get(Type, Names, default)).

-spec info(limiter_type()) -> state().
info(Type) ->
    ?CALL(Type).

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
    State = #{root => undefined,
              counter => undefined,
              index => 1,
              zones => #{},
              nodes => #{},
              buckets => [],
              type => Type},
    State2 = init_tree(Type, State),
    #{root := #{period := Perido}} = State2,
    oscillate(Perido),
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
handle_call(info, _From, State) ->
    {reply, State, State};

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
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
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
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
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
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
oscillation(#{root := #{rate := Flow,
                        period := Interval,
                        childs := ChildIds,
                        consumed := Consumed} = Root,
              nodes := Nodes} = State) ->
    oscillate(Interval),
    Childs = get_ordered_childs(ChildIds, Nodes),
    {Alloced, Nodes2} = transverse(Childs, Flow, 0, Nodes),
    maybe_burst(State#{nodes := Nodes2,
                       root := Root#{consumed := Consumed + Alloced}}).

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
longitudinal(#{id := Id,
               rate := Rate,
               obtained := Obtained,
               childs := ChildIds} = Node, InFlow, Nodes) ->
    Flow = erlang:min(InFlow, Rate),

    if Flow > 0 ->
            Childs = get_ordered_childs(ChildIds, Nodes),
            {Alloced, Nodes2} = transverse(Childs, Flow, 0, Nodes),
            if Alloced > 0 ->
                    {Alloced,
                     Nodes2#{Id => Node#{obtained := Obtained + Alloced}}};
               true ->
                    %% childs are empty or all counter childs are full
                    {0, Nodes2}
            end;
       true ->
            {0, Nodes}
    end;

longitudinal(#{id := Id,
               rate := Rate,
               capacity := Capacity,
               counter := Counter,
               index := Index,
               obtained := Obtained} = Node,
             InFlow, Nodes) when Counter =/= undefined ->
    Flow = erlang:min(InFlow, Rate),

    ShouldAlloc =
        case counters:get(Counter, Index) of
            Tokens when Tokens < 0 ->
                %% toknes's value mayb be a negative value(stolen from the future)
                %% because ∃ x. add(Capacity, x) < 0, so here we must compare with minimum value
                erlang:max(add(Capacity, Tokens),
                           mul(Capacity, ?OVERLOAD_MIN_ALLOC));
            Tokens ->
                %% is it possible that Tokens > Capacity ???
                erlang:max(sub(Capacity, Tokens), 0)
        end,

    case lists:min([ShouldAlloc, Flow, Capacity]) of
        Available when Available > 0 ->
            %% XXX if capacity is infinity, and flow always > 0, the value in
            %% counter will be overflow at some point in the future, do we need
            %% to deal with this situation???
            {Inc, Node2} = emqx_limiter_correction:add(Available, Node),
            counters:add(Counter, Index, Inc),

            {Inc,
             Nodes#{Id := Node2#{obtained := Obtained + Inc}}};
        _ ->
            {0, Nodes}
    end;

longitudinal(_, _, Nodes) ->
    {0, Nodes}.

-spec get_ordered_childs(list(node_id()), nodes()) -> list(node_data()).
get_ordered_childs(Ids, Nodes) ->
    Childs = [maps:get(Id, Nodes) || Id <- Ids],

    %% sort by obtained, avoid node goes hungry
    lists:sort(fun(#{obtained := A}, #{obtained := B}) ->
                       A < B
               end,
               Childs).

-spec maybe_burst(state()) -> state().
maybe_burst(#{buckets := Buckets,
              zones := Zones,
              root := #{burst := Burst},
              nodes := Nodes} = State) when Burst > 0 ->
    %% find empty buckets and group by zone name
    GroupFun = fun(Id, Groups) ->
                       #{counter := Counter,
                         index := Index,
                         zone := Zone} = maps:get(Id, Nodes),
                       case counters:get(Counter, Index) of
                           Any when Any =< 0 ->
                               Group = maps:get(Zone, Groups, []),
                               maps:put(Zone, [Id | Group], Groups);
                           _ ->
                               Groups
                       end
               end,

    case lists:foldl(GroupFun, #{}, Buckets) of
        Groups when map_size(Groups) > 0 ->
            %% remove the zone which don't support burst
            Filter = fun({Name, Childs}, Acc) ->
                             ZoneId = maps:get(Name, Zones),
                             #{burst := ZoneBurst} = Zone = maps:get(ZoneId, Nodes),
                             case ZoneBurst > 0 of
                                 true ->
                                     [{Zone, Childs} | Acc];
                                 _ ->
                                     Acc
                             end
                     end,

            FilterL = lists:foldl(Filter, [], maps:to_list(Groups)),
            dispatch_burst(FilterL, State);
        _ ->
            State
    end;

maybe_burst(State) ->
    State.

-spec dispatch_burst(list({zone(), list(node_id())}), state()) -> state().
dispatch_burst([], State) ->
    State;

dispatch_burst(GroupL,
               #{root := #{burst := Burst},
                 nodes := Nodes} = State) ->
    InFlow = Burst / erlang:length(GroupL),
    Dispatch = fun({Zone, Childs}, NodeAcc) ->
                       #{id := ZoneId,
                         burst := ZoneBurst,
                         obtained := Obtained} = Zone,

                       case erlang:min(InFlow, ZoneBurst) of
                           0 -> NodeAcc;
                           ZoneFlow ->
                               EachFlow = ZoneFlow / erlang:length(Childs),
                               {Alloced, NodeAcc2} = dispatch_burst_to_buckets(Childs, EachFlow, 0, NodeAcc),
                               Zone2 = Zone#{obtained := Obtained + Alloced},
                               NodeAcc2#{ZoneId := Zone2}
                       end
               end,
    State#{nodes := lists:foldl(Dispatch, Nodes, GroupL)}.

-spec dispatch_burst_to_buckets(list(node_id()),
                                float(), non_neg_integer(), nodes()) -> {non_neg_integer(), nodes()}.
dispatch_burst_to_buckets([ChildId | T], InFlow, Alloced, Nodes) ->
    #{counter := Counter,
      index := Index,
      obtained := Obtained} = Bucket = maps:get(ChildId, Nodes),
    {Inc, Bucket2} = emqx_limiter_correction:add(InFlow, Bucket),

    counters:add(Counter, Index, Inc),

    Nodes2 = Nodes#{ChildId := Bucket2#{obtained := Obtained + Inc}},
    dispatch_burst_to_buckets(T, InFlow, Alloced + Inc, Nodes2);

dispatch_burst_to_buckets([], _, Alloced, Nodes) ->
    {Alloced, Nodes}.

-spec init_tree(emqx_limiter_schema:limiter_type(), state()) -> state().
init_tree(Type, State) ->
    case emqx:get_config([limiter, Type]) of
        #{global := Global,
          zone := Zone,
          bucket := Bucket} -> ok;
        #{bucket := Bucket} ->
            Global = default_rate_burst_cfg(),
            Zone = #{default => default_rate_burst_cfg()},
            ok
    end,
    {Factor, Root} = make_root(Global, Zone),
    State2 = State#{root := Root},
    {NodeId, State3} = make_zone(maps:to_list(Zone), Factor, 1, State2),
    State4 = State3#{counter := counters:new(maps:size(Bucket),
                                             [write_concurrency])},
    make_bucket(maps:to_list(Bucket), Global, Zone, Factor, NodeId, [], State4).

-spec make_root(hocons:confg(), hocon:config()) -> {number(), root()}.
make_root(#{rate := Rate, burst := Burst}, Zone) ->
    ZoneNum = maps:size(Zone),
    Childs = lists:seq(1, ZoneNum),
    MiniPeriod = emqx_limiter_schema:minimum_period(),
    if Rate >= 1 ->
            {1, #{rate => Rate,
                  burst => Burst,
                  period => MiniPeriod,
                  childs => Childs,
                  consumed => 0}};
       true ->
            Factor = 1 / Rate,
            {Factor, #{rate => 1,
                       burst => Burst * Factor,
                       period => erlang:floor(Factor * MiniPeriod),
                       childs => Childs,
                       consumed => 0}}
    end.

make_zone([{Name, ZoneCfg} | T], Factor, NodeId, State) ->
    #{rate := Rate, burst := Burst} = ZoneCfg,
    #{zones := Zones, nodes := Nodes} = State,
    Zone = #{id => NodeId,
             name => Name,
             rate => mul(Rate, Factor),
             burst => Burst,
             obtained => 0,
             childs => []},
    State2 = State#{zones := Zones#{Name => NodeId},
                    nodes := Nodes#{NodeId => Zone}},
    make_zone(T, Factor, NodeId + 1, State2);

make_zone([], _, NodeId, State2) ->
    {NodeId, State2}.

make_bucket([{Name, Conf} | T], Global, Zone, Factor, Id, Buckets, #{type := Type} = State) ->
    #{zone := ZoneName,
      aggregated := Aggregated} = Conf,
    Path = emqx_limiter_manager:make_path(Type, ZoneName, Name),
    case get_counter_rate(Conf, Zone, Global) of
          infinity ->
              State2 = State,
              Rate = infinity,
              Capacity = infinity,
              Counter = undefined,
              Index = undefined,
              Ref = emqx_limiter_bucket_ref:new(Counter, Index, Rate),
              emqx_limiter_manager:insert_bucket(Path, Ref);
        RawRate ->
            #{capacity := Capacity} = Aggregated,
            Initial = get_initial_val(Aggregated),
            {Counter, Index, State2} = alloc_counter(Path, RawRate, Initial, State),
            Rate = mul(RawRate, Factor)
    end,

    Node = #{ id => Id
            , name => Name
            , zone => ZoneName
            , rate => Rate
            , obtained => 0
            , correction => 0
            , capacity => Capacity
            , counter => Counter
            , index => Index},

    State3 = add_zone_child(Id, Node, ZoneName, State2),
    make_bucket(T, Global, Zone, Factor, Id + 1, [Id | Buckets], State3);

make_bucket([], _, _, _, _, Buckets, State) ->
    State#{buckets := Buckets}.

-spec alloc_counter(emqx_limiter_manager:path(), rate(), capacity(), state()) ->
          {counters:counters_ref(), pos_integer(), state()}.
alloc_counter(Path, Rate, Initial,
              #{counter := Counter, index := Index} = State) ->
    case emqx_limiter_manager:find_bucket(Path) of
        {ok, #{counter := ECounter,
               index := EIndex}} when ECounter =/= undefined ->
            init_counter(Path, ECounter, EIndex, Rate, Initial, State);
        _ ->
            init_counter(Path, Counter, Index,
                         Rate, Initial, State#{index := Index + 1})
    end.

init_counter(Path, Counter, Index, Rate, Initial, State) ->
    _ = put_to_counter(Counter, Index, Initial),
    Ref = emqx_limiter_bucket_ref:new(Counter, Index, Rate),
    emqx_limiter_manager:insert_bucket(Path, Ref),
    {Counter, Index, State}.

-spec add_zone_child(node_id(), bucket(), zone_name(), state()) -> state().
add_zone_child(NodeId, Bucket, Name, #{zones := Zones, nodes := Nodes} = State) ->
    ZoneId = maps:get(Name, Zones),
    #{childs := Childs} = Zone = maps:get(ZoneId, Nodes),
    Nodes2 = Nodes#{ZoneId => Zone#{childs := [NodeId | Childs]},
                    NodeId => Bucket},
    State#{nodes := Nodes2}.

%% @doc find first limited node
get_counter_rate(#{zone := ZoneName,
                   aggregated := Cfg}, ZoneCfg, Global) ->
    Zone = maps:get(ZoneName, ZoneCfg),
    Search = lists:search(fun(E) -> is_limited(E) end,
                          [Cfg, Zone, Global]),
    case Search of
        {value, #{rate := Rate}} ->
            Rate;
        false ->
            infinity
    end.

is_limited(#{rate := Rate, capacity := Capacity}) ->
    Rate =/= infinity orelse Capacity =/= infinity;

is_limited(#{rate := Rate}) ->
    Rate =/= infinity.

get_initial_val(#{initial := Initial,
                  rate := Rate,
                  capacity := Capacity}) ->
    %% initial will nevner be infinity(see the emqx_limiter_schema)
    if Initial > 0 ->
            Initial;
       Rate =/= infinity ->
            erlang:min(Rate, Capacity);
       Capacity =/= infinity ->
            Capacity;
       true ->
            0
    end.

default_rate_burst_cfg() ->
    #{rate => infinity, burst => 0}.
