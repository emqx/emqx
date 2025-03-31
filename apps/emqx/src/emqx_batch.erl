%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_batch).

%% APIs
-export([
    init/1,
    push/2,
    commit/1,
    size/1,
    items/1
]).

-export_type([options/0, batch/0]).

-record(batch, {
    batch_size :: non_neg_integer(),
    batch_q :: list(any()),
    linger_ms :: pos_integer(),
    linger_timer :: reference() | undefined,
    commit_fun :: function()
}).

-type options() :: #{
    batch_size => non_neg_integer(),
    linger_ms => pos_integer(),
    commit_fun := function()
}.

-opaque batch() :: #batch{}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec init(options()) -> batch().
init(Opts) when is_map(Opts) ->
    #batch{
        batch_size = maps:get(batch_size, Opts, 1000),
        batch_q = [],
        linger_ms = maps:get(linger_ms, Opts, 1000),
        commit_fun = maps:get(commit_fun, Opts)
    }.

-spec push(any(), batch()) -> batch().
push(
    El,
    Batch = #batch{
        batch_q = Q,
        linger_ms = Ms,
        linger_timer = undefined
    }
) when
    length(Q) == 0
->
    TRef = erlang:send_after(Ms, self(), batch_linger_expired),
    Batch#batch{batch_q = [El], linger_timer = TRef};
%% no limit.
push(El, Batch = #batch{batch_size = 0, batch_q = Q}) ->
    Batch#batch{batch_q = [El | Q]};
push(El, Batch = #batch{batch_size = MaxSize, batch_q = Q}) when
    length(Q) >= MaxSize
->
    commit(Batch#batch{batch_q = [El | Q]});
push(El, Batch = #batch{batch_q = Q}) ->
    Batch#batch{batch_q = [El | Q]}.

-spec commit(batch()) -> batch().
commit(Batch = #batch{batch_q = Q, commit_fun = Commit}) ->
    _ = Commit(lists:reverse(Q)),
    reset(Batch).

reset(Batch = #batch{linger_timer = TRef}) ->
    _ = emqx_utils:cancel_timer(TRef),
    Batch#batch{batch_q = [], linger_timer = undefined}.

-spec size(batch()) -> non_neg_integer().
size(#batch{batch_q = Q}) ->
    length(Q).

-spec items(batch()) -> list(any()).
items(#batch{batch_q = Q}) ->
    lists:reverse(Q).
