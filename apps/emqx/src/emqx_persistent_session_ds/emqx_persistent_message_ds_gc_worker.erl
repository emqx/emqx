%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_persistent_message_ds_gc_worker).

-behaviour(gen_server).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-include("session_internals.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([manage_shards/3]).
-endif.

%% API
-export([
    start_link/0,
    gc/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% call/cast/info records
-record(gc, {}).

-type slabs() :: #{emqx_ds:slab() => emqx_ds:slab_info()}.

%%--------------------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% For testing or manual ops
gc() ->
    gen_server:call(?MODULE, #gc{}, infinity).

%%--------------------------------------------------------------------------------
%% `gen_server' API
%%--------------------------------------------------------------------------------

init(_Opts) ->
    ensure_gc_timer(),
    State = #{},
    {ok, State}.

handle_call(#gc{}, _From, State) ->
    manage_shards(),
    {reply, ok, State};
handle_call(_Call, _From, State) ->
    {reply, error, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#gc{}, State) ->
    try_gc(),
    ensure_gc_timer(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------------------
%% Internal fns
%%--------------------------------------------------------------------------------

ensure_gc_timer() ->
    Timeout = 5_000,
    _ = erlang:send_after(Timeout, self(), #gc{}),
    ok.

try_gc() ->
    %% Only cores should run GC.
    CoreNodes = emqx_cluster:running_core_nodelist(),
    Res = global:trans(
        {?MODULE, self()},
        fun manage_shards/0,
        CoreNodes,
        %% Note: we set retries to 1 here because, in rare occasions, GC might start at the
        %% same time in more than one node, and each one will abort the other.  By allowing
        %% one retry, at least one node will (hopefully) get to enter the transaction and
        %% the other will abort.  If GC runs too fast, both nodes might run in sequence.
        %% But, in that case, GC is clearly not too costly, and that shouldn't be a problem,
        %% resource-wise.
        _Retries = 1
    ),
    case Res of
        aborted ->
            ?tp(ds_message_gc_lock_taken, #{}),
            ok;
        ok ->
            ok
    end.

manage_shards() ->
    Now = erlang:system_time(microsecond),
    RetentionPeriod = emqx_persistent_session_ds:to_ds_time(
        emqx_config:get([durable_sessions, message_retention_period])
    ),
    %% How many generations per retention period to create:
    NGens = 4,
    manage_shards(Now, RetentionPeriod, NGens).

-spec manage_shards(emqx_ds:time(), pos_integer(), pos_integer()) -> ok.
manage_shards(Now, RetentionPeriod, NGens) ->
    lists:foreach(
        fun(Shard) ->
            maybe
                {Slabs, Errors} = emqx_ds:list_slabs(?PERSISTENT_MESSAGE_DB, #{shard => Shard}),
                [] ?= Errors,
                ok ?= manage_shard(RetentionPeriod, NGens, Now, Shard, Slabs)
            else
                Err ->
                    ?tp(
                        warning,
                        "Failed to rotate DS generations",
                        #{shard => Shard, reason => Err}
                    )
            end
        end,
        emqx_ds:list_shards(?PERSISTENT_MESSAGE_DB)
    ).

-spec manage_shard(pos_integer(), pos_integer(), emqx_ds:time(), emqx_ds:shard(), slabs()) ->
    ok | emqx_ds:error(_).
manage_shard(RetentionPeriod, NGens, Now, Shard, Slabs) ->
    maybe
        ok ?= ensure_new_generation(Now, RetentionPeriod, NGens, Shard, Slabs),
        RmSlabs = expired_slabs(Now, RetentionPeriod, Slabs),
        ?tp(info, "Removing obsolete slabs", #{slabs => RmSlabs}),
        _ = [emqx_ds:drop_slab(?PERSISTENT_MESSAGE_DB, I) || I <- RmSlabs],
        ok
    end.

-spec ensure_new_generation(integer(), pos_integer(), pos_integer(), emqx_ds:shard(), slabs()) ->
    ok | {error, _}.
ensure_new_generation(NowMS, RetentionPeriod, NGens, Shard, Slabs) ->
    case need_new_gen(NowMS, RetentionPeriod, NGens, Slabs) of
        true ->
            case emqx_ds:add_generation(?PERSISTENT_MESSAGE_DB, Shard) of
                ok ->
                    ok;
                {error, Class, Reason} ->
                    {error, Class, {add_generation, Reason}}
            end;
        false ->
            ok
    end.

%% Return `true' if all generations were created too long ago
-spec need_new_gen(integer(), pos_integer(), pos_integer(), slabs()) ->
    boolean().
need_new_gen(Now, RetentionPeriod, NGens, Slabs) ->
    Threshold = Now - RetentionPeriod div NGens,
    try
        maps:foreach(
            fun(_Slab, #{created_at := CreatedAt}) ->
                CreatedAt >= Threshold andalso throw(found)
            end,
            Slabs
        ),
        true
    catch
        found ->
            false
    end.

-spec expired_slabs(integer(), pos_integer(), slabs()) -> [emqx_ds:slab()].
expired_slabs(Now, RetentionPeriod, Slabs) ->
    TimeThresholdUs = Now - RetentionPeriod,
    maps:fold(
        fun(Slab, #{until := Until}, Acc) ->
            case is_number(Until) andalso Until < TimeThresholdUs of
                true ->
                    [Slab | Acc];
                false ->
                    Acc
            end
        end,
        [],
        Slabs
    ).

-ifdef(TEST).

test_data() ->
    #{
        {<<"0">>, 6} =>
            #{
                until => 1791722300540565,
                since => 1791722150333803,
                created_at => 1791722151499524
            },
        {<<"0">>, 7} =>
            #{
                until => 1791722458476798,
                since => 1791722300540565,
                created_at => 1791722303092013
            },
        {<<"0">>, 8} =>
            #{
                until => undefined,
                since => 1791722458476798,
                created_at => 1791722459489493
            }
    }.

need_new_gen_test() ->
    LastCreateTime = 1791722459489493,
    %% 1 generation per retention period:
    ?assert(
        need_new_gen(LastCreateTime + 1001, 1000, 1, test_data())
    ),
    ?assertNot(
        need_new_gen(LastCreateTime + 1000, 1000, 1, test_data())
    ),
    %% 2 generations per retention period:
    ?assert(
        need_new_gen(LastCreateTime + 501, 1000, 2, test_data())
    ),
    ?assertNot(
        need_new_gen(LastCreateTime + 500, 1000, 2, test_data())
    ).

expired_slabs_test() ->
    Until6 = 1791722300540565,
    Now = 1791722459489493,
    ?assertEqual(
        [],
        expired_slabs(Now, Now - Until6, test_data())
    ),
    ?assertEqual(
        [{<<"0">>, 6}],
        expired_slabs(Now + 1, Now - Until6, test_data())
    ),
    %% Current generation is always kept:
    ?assertEqual(
        [{<<"0">>, 6}, {<<"0">>, 7}],
        lists:sort(expired_slabs(Now + 1, 0, test_data()))
    ).

-endif.
