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
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3,
    format_status/2
]).

-export([
    start_link/2,
    connect/2,
    whereis/1,
    info/1,
    name/1,
    get_initial_val/1,
    restart/1,
    update_config/2
]).

%% number of tokens generated per period
-type root() :: #{
    rate := rate(),
    burst := rate(),
    %% token generation interval(second)
    period := pos_integer(),
    produced := float()
}.

-type bucket() :: #{
    name := bucket_name(),
    rate := rate(),
    obtained := float(),
    %% token correction value
    correction := emqx_limiter_decimal:zero_or_float(),
    capacity := capacity(),
    counter := undefined | counters:counters_ref(),
    index := undefined | index()
}.

-type state() :: #{
    type := limiter_type(),
    root := undefined | root(),
    buckets := buckets(),
    %% current counter to alloc
    counter := undefined | counters:counters_ref(),
    index := index()
}.

-type buckets() :: #{bucket_name() => bucket()}.
-type limiter_type() :: emqx_limiter_schema:limiter_type().
-type bucket_name() :: emqx_limiter_schema:bucket_name().
-type rate() :: decimal().
-type flow() :: decimal().
-type capacity() :: decimal().
-type decimal() :: emqx_limiter_decimal:decimal().
-type index() :: pos_integer().

-define(CALL(Type, Msg), call(Type, Msg)).
-define(CALL(Type), ?CALL(Type, ?FUNCTION_NAME)).

%% minimum coefficient for overloaded limiter
-define(OVERLOAD_MIN_ALLOC, 0.3).
-define(CURRYING(X, F2), fun(Y) -> F2(X, Y) end).

-export_type([index/0]).
-import(emqx_limiter_decimal, [add/2, sub/2, mul/2, put_to_counter/3]).

-elvis([{elvis_style, no_if_expression, disable}]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec connect(
    limiter_type(),
    bucket_name() | #{limiter_type() => bucket_name() | undefined}
) ->
    {ok, emqx_htb_limiter:limiter()} | {error, _}.
%% If no bucket path is set in config, there will be no limit
connect(_Type, undefined) ->
    {ok, emqx_htb_limiter:make_infinity_limiter()};
connect(Type, BucketName) when is_atom(BucketName) ->
    case get_bucket_cfg(Type, BucketName) of
        undefined ->
            ?SLOG(error, #{msg => "bucket_config_not_found", type => Type, bucket => BucketName}),
            {error, config_not_found};
        #{
            rate := BucketRate,
            capacity := BucketSize,
            per_client := #{rate := CliRate, capacity := CliSize} = Cfg
        } ->
            case emqx_limiter_manager:find_bucket(Type, BucketName) of
                {ok, Bucket} ->
                    {ok,
                        if
                            CliRate < BucketRate orelse CliSize < BucketSize ->
                                emqx_htb_limiter:make_token_bucket_limiter(Cfg, Bucket);
                            true ->
                                emqx_htb_limiter:make_ref_limiter(Cfg, Bucket)
                        end};
                undefined ->
                    ?SLOG(error, #{msg => "bucket_not_found", type => Type, bucket => BucketName}),
                    {error, invalid_bucket}
            end
    end;
connect(Type, Paths) ->
    connect(Type, maps:get(Type, Paths, undefined)).

-spec info(limiter_type()) -> state() | {error, _}.
info(Type) ->
    ?CALL(Type).

-spec name(limiter_type()) -> atom().
name(Type) ->
    erlang:list_to_atom(io_lib:format("~s_~s", [?MODULE, Type])).

-spec restart(limiter_type()) -> ok | {error, _}.
restart(Type) ->
    ?CALL(Type).

-spec update_config(limiter_type(), hocons:config()) -> ok | {error, _}.
update_config(Type, Config) ->
    ?CALL(Type, {update_config, Type, Config}).

-spec whereis(limiter_type()) -> pid() | undefined.
whereis(Type) ->
    erlang:whereis(name(Type)).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link(limiter_type(), hocons:config()) -> _.
start_link(Type, Cfg) ->
    gen_server:start_link({local, name(Type)}, ?MODULE, [Type, Cfg], []).

%%--------------------------------------------------------------------
%%% gen_server callbacks
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: term()}
    | {ok, State :: term(), Timeout :: timeout()}
    | {ok, State :: term(), hibernate}
    | {stop, Reason :: term()}
    | ignore.
init([Type, Cfg]) ->
    State = init_tree(Type, Cfg),
    #{root := #{period := Perido}} = State,
    oscillate(Perido),
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
    {reply, Reply :: term(), NewState :: term()}
    | {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()}
    | {reply, Reply :: term(), NewState :: term(), hibernate}
    | {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.
handle_call(info, _From, State) ->
    {reply, State, State};
handle_call(restart, _From, #{type := Type}) ->
    NewState = init_tree(Type),
    {reply, ok, NewState};
handle_call({update_config, Type, Config}, _From, #{type := Type}) ->
    NewState = init_tree(Type, Config),
    {reply, ok, NewState};
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
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: term(), NewState :: term()}.
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
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: normal | term(), NewState :: term()}.
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
-spec terminate(
    Reason :: normal | shutdown | {shutdown, term()} | term(),
    State :: term()
) -> any().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(
    OldVsn :: term() | {down, term()},
    State :: term(),
    Extra :: term()
) ->
    {ok, NewState :: term()}
    | {error, Reason :: term()}.
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
-spec format_status(
    Opt :: normal | terminate,
    Status :: list()
) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
oscillate(Interval) ->
    erlang:send_after(Interval, self(), ?FUNCTION_NAME).

%% @doc generate tokens, and then spread to leaf nodes
-spec oscillation(state()) -> state().
oscillation(
    #{
        root := #{
            rate := Flow,
            period := Interval,
            produced := Produced
        } = Root,
        buckets := Buckets
    } = State
) ->
    oscillate(Interval),
    Ordereds = get_ordered_buckets(Buckets),
    {Alloced, Buckets2} = transverse(Ordereds, Flow, 0.0, Buckets),
    maybe_burst(State#{
        buckets := Buckets2,
        root := Root#{produced := Produced + Alloced}
    }).

%% @doc horizontal spread
-spec transverse(
    list(bucket()),
    flow(),
    float(),
    buckets()
) -> {float(), buckets()}.
transverse([H | T], InFlow, Alloced, Buckets) when InFlow > 0 ->
    {BucketAlloced, Buckets2} = longitudinal(H, InFlow, Buckets),
    InFlow2 = sub(InFlow, BucketAlloced),
    Alloced2 = Alloced + BucketAlloced,
    transverse(T, InFlow2, Alloced2, Buckets2);
transverse(_, _, Alloced, Buckets) ->
    {Alloced, Buckets}.

%% @doc vertical spread
-spec longitudinal(bucket(), flow(), buckets()) ->
    {float(), buckets()}.
longitudinal(
    #{
        name := Name,
        rate := Rate,
        capacity := Capacity,
        counter := Counter,
        index := Index,
        obtained := Obtained
    } = Bucket,
    InFlow,
    Buckets
) when Counter =/= undefined ->
    Flow = erlang:min(InFlow, Rate),

    ShouldAlloc =
        case counters:get(Counter, Index) of
            Tokens when Tokens < 0 ->
                %% toknes's value mayb be a negative value(stolen from the future)
                %% because ∃ x. add(Capacity, x) < 0, so here we must compare with minimum value
                erlang:max(
                    add(Capacity, Tokens),
                    mul(Capacity, ?OVERLOAD_MIN_ALLOC)
                );
            Tokens ->
                %% is it possible that Tokens > Capacity ???
                erlang:max(sub(Capacity, Tokens), 0)
        end,

    case lists:min([ShouldAlloc, Flow, Capacity]) of
        Available when Available > 0 ->
            {Inc, Bucket2} = emqx_limiter_correction:add(Available, Bucket),
            counters:add(Counter, Index, Inc),

            {Available, Buckets#{Name := Bucket2#{obtained := Obtained + Available}}};
        _ ->
            {0, Buckets}
    end;
longitudinal(_, _, Buckets) ->
    {0, Buckets}.

-spec get_ordered_buckets(list(bucket()) | buckets()) -> list(bucket()).
get_ordered_buckets(Buckets) when is_map(Buckets) ->
    BucketList = maps:values(Buckets),
    get_ordered_buckets(BucketList);
get_ordered_buckets(Buckets) ->
    %% sort by obtained, avoid node goes hungry
    lists:sort(
        fun(#{obtained := A}, #{obtained := B}) ->
            A < B
        end,
        Buckets
    ).

-spec maybe_burst(state()) -> state().
maybe_burst(
    #{
        buckets := Buckets,
        root := #{burst := Burst}
    } = State
) when Burst > 0 ->
    Fold = fun
        (_Name, #{counter := Cnt, index := Idx} = Bucket, Acc) when Cnt =/= undefined ->
            case counters:get(Cnt, Idx) > 0 of
                true ->
                    Acc;
                false ->
                    [Bucket | Acc]
            end;
        (_Name, _Bucket, Acc) ->
            Acc
    end,

    Empties = maps:fold(Fold, [], Buckets),
    dispatch_burst(Empties, Burst, State);
maybe_burst(State) ->
    State.

-spec dispatch_burst(list(bucket()), non_neg_integer(), state()) -> state().
dispatch_burst([], _, State) ->
    State;
dispatch_burst(
    Empties,
    InFlow,
    #{root := #{produced := Produced} = Root, buckets := Buckets} = State
) ->
    EachFlow = InFlow / erlang:length(Empties),
    {Alloced, Buckets2} = dispatch_burst_to_buckets(Empties, EachFlow, 0, Buckets),
    State#{root := Root#{produced := Produced + Alloced}, buckets := Buckets2}.

-spec dispatch_burst_to_buckets(
    list(bucket()),
    float(),
    non_neg_integer(),
    buckets()
) -> {non_neg_integer(), buckets()}.
dispatch_burst_to_buckets([Bucket | T], InFlow, Alloced, Buckets) ->
    #{
        name := Name,
        counter := Counter,
        index := Index,
        obtained := Obtained
    } = Bucket,
    {Inc, Bucket2} = emqx_limiter_correction:add(InFlow, Bucket),

    counters:add(Counter, Index, Inc),

    Buckets2 = Buckets#{Name := Bucket2#{obtained := Obtained + Inc}},
    dispatch_burst_to_buckets(T, InFlow, Alloced + Inc, Buckets2);
dispatch_burst_to_buckets([], _, Alloced, Buckets) ->
    {Alloced, Buckets}.

-spec init_tree(emqx_limiter_schema:limiter_type()) -> state().
init_tree(Type) when is_atom(Type) ->
    Cfg = emqx:get_config([limiter, Type]),
    init_tree(Type, Cfg).

init_tree(Type, #{bucket := Buckets} = Cfg) ->
    State = #{
        type => Type,
        root => undefined,
        counter => undefined,
        index => 1,
        buckets => #{}
    },

    Root = make_root(Cfg),
    {CounterNum, DelayBuckets} = make_bucket(maps:to_list(Buckets), Type, Cfg, 1, []),

    State2 = State#{
        root := Root,
        counter := counters:new(CounterNum, [write_concurrency])
    },

    lists:foldl(fun(F, Acc) -> F(Acc) end, State2, DelayBuckets).

-spec make_root(hocons:confg()) -> root().
make_root(#{rate := Rate, burst := Burst}) ->
    #{
        rate => Rate,
        burst => Burst,
        period => emqx_limiter_schema:default_period(),
        produced => 0.0
    }.

make_bucket([{Name, Conf} | T], Type, GlobalCfg, CounterNum, DelayBuckets) ->
    Path = emqx_limiter_manager:make_path(Type, Name),
    Rate = get_counter_rate(Conf, GlobalCfg),
    #{capacity := Capacity} = Conf,
    Initial = get_initial_val(Conf),
    CounterNum2 = CounterNum + 1,
    InitFun = fun(#{name := BucketName} = Bucket, #{buckets := Buckets} = State) ->
        {Counter, Idx, State2} = alloc_counter(Path, Rate, Initial, State),
        Bucket2 = Bucket#{counter := Counter, index := Idx},
        State2#{buckets := Buckets#{BucketName => Bucket2}}
    end,

    Bucket = #{
        name => Name,
        rate => Rate,
        obtained => Initial,
        correction => 0,
        capacity => Capacity,
        counter => undefined,
        index => undefined
    },

    DelayInit = ?CURRYING(Bucket, InitFun),

    make_bucket(
        T,
        Type,
        GlobalCfg,
        CounterNum2,
        [DelayInit | DelayBuckets]
    );
make_bucket([], _Type, _Global, CounterNum, DelayBuckets) ->
    {CounterNum, DelayBuckets}.

-spec alloc_counter(emqx_limiter_manager:path(), rate(), capacity(), state()) ->
    {counters:counters_ref(), pos_integer(), state()}.
alloc_counter(
    Path,
    Rate,
    Initial,
    #{counter := Counter, index := Index} = State
) ->
    case emqx_limiter_manager:find_bucket(Path) of
        {ok, #{
            counter := ECounter,
            index := EIndex
        }} when ECounter =/= undefined ->
            init_counter(Path, ECounter, EIndex, Rate, Initial, State);
        _ ->
            init_counter(
                Path,
                Counter,
                Index,
                Rate,
                Initial,
                State#{index := Index + 1}
            )
    end.

init_counter(Path, Counter, Index, Rate, Initial, State) ->
    _ = put_to_counter(Counter, Index, Initial),
    Ref = emqx_limiter_bucket_ref:new(Counter, Index, Rate),
    emqx_limiter_manager:insert_bucket(Path, Ref),
    {Counter, Index, State}.

%% @doc find first limited node
get_counter_rate(#{rate := Rate}, _GlobalCfg) when Rate =/= infinity ->
    Rate;
get_counter_rate(_Cfg, #{rate := Rate}) when Rate =/= infinity ->
    Rate;
get_counter_rate(_Cfg, _GlobalCfg) ->
    emqx_limiter_schema:infinity_value().

-spec get_initial_val(hocons:config()) -> decimal().
get_initial_val(
    #{
        initial := Initial,
        rate := Rate,
        capacity := Capacity
    }
) ->
    %% initial will nevner be infinity(see the emqx_limiter_schema)
    InfVal = emqx_limiter_schema:infinity_value(),
    if
        Initial > 0 ->
            Initial;
        Rate =/= infinity ->
            erlang:min(Rate, Capacity);
        Capacity =/= infinity andalso Capacity =/= InfVal ->
            Capacity;
        true ->
            0
    end.

-spec call(limiter_type(), any()) -> {error, _} | _.
call(Type, Msg) ->
    case ?MODULE:whereis(Type) of
        undefined ->
            {error, limiter_not_started};
        Pid ->
            gen_server:call(Pid, Msg)
    end.

-spec get_bucket_cfg(limiter_type(), bucket_name()) ->
    undefined | limiter_not_started | hocons:config().
get_bucket_cfg(Type, Bucket) ->
    Path = emqx_limiter_schema:get_bucket_cfg_path(Type, Bucket),
    emqx:get_config(Path, undefined).
