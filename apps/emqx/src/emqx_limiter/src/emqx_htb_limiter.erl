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

-module(emqx_htb_limiter).

%% @doc the limiter of the hierarchical token limiter system
%% this module provides api for creating limiters, consume tokens, check tokens and retry
%% @end

%% API
-export([
    make_token_bucket_limiter/2,
    make_ref_limiter/2,
    check/2,
    consume/2,
    set_retry/2,
    retry/1,
    make_infinity_limiter/0,
    make_future/1,
    available/1
]).
-export_type([token_bucket_limiter/0]).

%% a token bucket limiter with a limiter server's bucket reference

%% the number of tokens currently available
-type token_bucket_limiter() :: #{
    tokens := non_neg_integer(),
    rate := decimal(),
    capacity := decimal(),
    lasttime := millisecond(),
    %% @see emqx_limiter_schema
    max_retry_time := non_neg_integer(),
    %% @see emqx_limiter_schema
    failure_strategy := failure_strategy(),
    %% @see emqx_limiter_schema
    divisible := boolean(),
    %% @see emqx_limiter_schema
    low_watermark := non_neg_integer(),
    %% the limiter server's bucket
    bucket := bucket(),

    %% retry contenxt
    %% undefined meaning no retry context or no need to retry
    retry_ctx =>
        undefined
        %% the retry context
        | retry_context(token_bucket_limiter()),
    %% allow to add other keys
    atom => any()
}.

%% a limiter server's bucket reference
-type ref_limiter() :: #{
    max_retry_time := non_neg_integer(),
    failure_strategy := failure_strategy(),
    divisible := boolean(),
    low_watermark := non_neg_integer(),
    bucket := bucket(),

    retry_ctx => undefined | retry_context(ref_limiter()),
    %% allow to add other keys
    atom => any()
}.

-type retry_fun(Limiter) :: fun((pos_integer(), Limiter) -> inner_check_result(Limiter)).
-type acquire_type(Limiter) :: integer() | retry_context(Limiter).
-type retry_context(Limiter) :: #{
    continuation := undefined | retry_fun(Limiter),
    %% how many tokens are left to obtain
    diff := non_neg_integer(),

    need => pos_integer(),
    start => millisecond()
}.

-type bucket() :: emqx_limiter_bucket_ref:bucket_ref().
-type limiter() :: token_bucket_limiter() | ref_limiter() | infinity.
-type millisecond() :: non_neg_integer().

-type pause_type() :: pause | partial.
-type check_result_ok(Limiter) :: {ok, Limiter}.
-type check_result_pause(Limiter) :: {pause_type(), millisecond(), retry_context(Limiter), Limiter}.
-type result_drop(Limiter) :: {drop, Limiter}.

-type check_result(Limiter) ::
    check_result_ok(Limiter)
    | check_result_pause(Limiter)
    | result_drop(Limiter).

-type inner_check_result(Limiter) ::
    check_result_ok(Limiter)
    | check_result_pause(Limiter).

-type consume_result(Limiter) ::
    check_result_ok(Limiter)
    | result_drop(Limiter).

-type decimal() :: emqx_limiter_decimal:decimal().
-type failure_strategy() :: emqx_limiter_schema:failure_strategy().

-type limiter_bucket_cfg() :: #{
    rate := decimal(),
    initial := non_neg_integer(),
    low_watermark := non_neg_integer(),
    capacity := decimal(),
    divisible := boolean(),
    max_retry_time := non_neg_integer(),
    failure_strategy := failure_strategy()
}.

-type future() :: pos_integer().

-define(NOW, erlang:monotonic_time(millisecond)).
-define(MINIMUM_PAUSE, 50).
-define(MAXIMUM_PAUSE, 5000).

-import(emqx_limiter_decimal, [sub/2, mul/2, floor_div/2, add/2]).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------
%%@doc create a limiter
-spec make_token_bucket_limiter(limiter_bucket_cfg(), bucket()) -> _.
make_token_bucket_limiter(Cfg, Bucket) ->
    Cfg#{
        tokens => emqx_limiter_server:get_initial_val(Cfg),
        lasttime => ?NOW,
        bucket => Bucket
    }.

%%@doc create a limiter server's reference
-spec make_ref_limiter(limiter_bucket_cfg(), bucket()) -> ref_limiter().
make_ref_limiter(Cfg, Bucket) when Bucket =/= infinity ->
    Cfg#{bucket => Bucket}.

-spec make_infinity_limiter() -> infinity.
make_infinity_limiter() ->
    infinity.

%% @doc request some tokens
%% it will automatically retry when failed until the maximum retry time is reached
%% @end
-spec consume(integer(), Limiter) -> consume_result(Limiter) when
    Limiter :: limiter().
consume(Need, #{max_retry_time := RetryTime} = Limiter) when Need > 0 ->
    try_consume(RetryTime, Need, Limiter);
consume(_, Limiter) ->
    {ok, Limiter}.

%% @doc try to request the token and return the result without automatically retrying
-spec check(acquire_type(Limiter), Limiter) -> check_result(Limiter) when
    Limiter :: limiter().
check(_, infinity) ->
    {ok, infinity};
check(Need, Limiter) when is_integer(Need), Need > 0 ->
    case do_check(Need, Limiter) of
        {ok, _} = Done ->
            Done;
        {PauseType, Pause, Ctx, Limiter2} ->
            {PauseType, Pause, Ctx#{start => ?NOW, need => Need}, Limiter2}
    end;
%% check with retry context.
%% when continuation = undefined, the diff will be 0
%% so there is no need to check continuation here
check(
    #{
        continuation := Cont,
        diff := Diff,
        start := Start
    } = Retry,
    #{
        failure_strategy := Failure,
        max_retry_time := RetryTime
    } = Limiter
) when Diff > 0 ->
    case Cont(Diff, Limiter) of
        {ok, _} = Done ->
            Done;
        {PauseType, Pause, Ctx, Limiter2} ->
            IsFailed = ?NOW - Start >= RetryTime,
            Retry2 = maps:merge(Retry, Ctx),
            case IsFailed of
                false ->
                    {PauseType, Pause, Retry2, Limiter2};
                _ ->
                    on_failure(Failure, try_restore(Retry2, Limiter2))
            end
    end;
check(_, Limiter) ->
    {ok, Limiter}.

%% @doc pack the retry context into the limiter data
-spec set_retry(retry_context(Limiter), Limiter) -> Limiter when
    Limiter :: limiter().
set_retry(Retry, Limiter) ->
    Limiter#{retry_ctx => Retry}.

%% @doc check if there is a retry context, and try again if there is
-spec retry(Limiter) -> check_result(Limiter) when Limiter :: limiter().
retry(#{retry_ctx := Retry} = Limiter) when is_map(Retry) ->
    check(Retry, Limiter#{retry_ctx := undefined});
retry(Limiter) ->
    {ok, Limiter}.

%% @doc make a future value
%% this similar to retry context, but represents a value that will be checked in the future
%% @end
-spec make_future(pos_integer()) -> future().
make_future(Need) ->
    Need.

%% @doc get the number of tokens currently available
-spec available(limiter()) -> decimal().
available(#{
    tokens := Tokens,
    rate := Rate,
    lasttime := LastTime,
    capacity := Capacity,
    bucket := Bucket
}) ->
    Tokens2 = apply_elapsed_time(Rate, ?NOW - LastTime, Tokens, Capacity),
    erlang:min(Tokens2, emqx_limiter_bucket_ref:available(Bucket));
available(#{bucket := Bucket}) ->
    emqx_limiter_bucket_ref:available(Bucket);
available(infinity) ->
    infinity.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
-spec try_consume(
    millisecond(),
    acquire_type(Limiter),
    Limiter
) -> consume_result(Limiter) when Limiter :: limiter().
try_consume(LeftTime, Retry, #{failure_strategy := Failure} = Limiter) when
    LeftTime =< 0, is_map(Retry)
->
    on_failure(Failure, try_restore(Retry, Limiter));
try_consume(LeftTime, Need, Limiter) when is_integer(Need) ->
    case do_check(Need, Limiter) of
        {ok, _} = Done ->
            Done;
        {_, Pause, Ctx, Limiter2} ->
            timer:sleep(erlang:min(LeftTime, Pause)),
            try_consume(LeftTime - Pause, Ctx#{need => Need}, Limiter2)
    end;
try_consume(
    LeftTime,
    #{
        continuation := Cont,
        diff := Diff
    } = Retry,
    Limiter
) when Diff > 0 ->
    case Cont(Diff, Limiter) of
        {ok, _} = Done ->
            Done;
        {_, Pause, Ctx, Limiter2} ->
            timer:sleep(erlang:min(LeftTime, Pause)),
            try_consume(LeftTime - Pause, maps:merge(Retry, Ctx), Limiter2)
    end;
try_consume(_, _, Limiter) ->
    {ok, Limiter}.

-spec do_check(acquire_type(Limiter), Limiter) -> inner_check_result(Limiter) when
    Limiter :: limiter().
do_check(Need, #{tokens := Tokens} = Limiter) when Need =< Tokens ->
    do_check_with_parent_limiter(Need, Limiter);
do_check(Need, #{tokens := _} = Limiter) ->
    do_reset(Need, Limiter);
do_check(
    Need,
    #{
        divisible := Divisible,
        bucket := Bucket
    } = Ref
) ->
    case emqx_limiter_bucket_ref:check(Need, Bucket, Divisible) of
        {ok, Tokens} ->
            may_return_or_pause(Tokens, Ref);
        {PauseType, Rate, Obtained} ->
            return_pause(
                Rate,
                PauseType,
                fun ?FUNCTION_NAME/2,
                Need - Obtained,
                Ref
            )
    end.

on_failure(force, Limiter) ->
    {ok, Limiter};
on_failure(drop, Limiter) ->
    {drop, Limiter};
on_failure(throw, Limiter) ->
    Message = io_lib:format("limiter consume failed, limiter:~p~n", [Limiter]),
    erlang:throw({rate_check_fail, Message}).

-spec do_check_with_parent_limiter(pos_integer(), token_bucket_limiter()) ->
    inner_check_result(token_bucket_limiter()).
do_check_with_parent_limiter(
    Need,
    #{
        tokens := Tokens,
        divisible := Divisible,
        bucket := Bucket
    } = Limiter
) ->
    case emqx_limiter_bucket_ref:check(Need, Bucket, Divisible) of
        {ok, RefLeft} ->
            Left = sub(Tokens, Need),
            may_return_or_pause(erlang:min(RefLeft, Left), Limiter#{tokens := Left});
        {PauseType, Rate, Obtained} ->
            return_pause(
                Rate,
                PauseType,
                fun ?FUNCTION_NAME/2,
                Need - Obtained,
                Limiter#{tokens := sub(Tokens, Obtained)}
            )
    end.

-spec do_reset(pos_integer(), token_bucket_limiter()) -> inner_check_result(token_bucket_limiter()).
do_reset(
    Need,
    #{
        tokens := Tokens,
        rate := Rate,
        lasttime := LastTime,
        divisible := Divisible,
        capacity := Capacity
    } = Limiter
) ->
    Now = ?NOW,
    Tokens2 = apply_elapsed_time(Rate, Now - LastTime, Tokens, Capacity),

    case erlang:floor(Tokens2) of
        Available when Available >= Need ->
            Limiter2 = Limiter#{tokens := Tokens2, lasttime := Now},
            do_check_with_parent_limiter(Need, Limiter2);
        Available when Divisible andalso Available > 0 ->
            %% must be allocated here, because may be Need > Capacity
            return_pause(
                Rate,
                partial,
                fun do_reset/2,
                Need - Available,
                Limiter#{tokens := 0, lasttime := Now}
            );
        _ ->
            return_pause(Rate, pause, fun do_reset/2, Need, Limiter)
    end.

-spec return_pause(decimal(), pause_type(), retry_fun(Limiter), pos_integer(), Limiter) ->
    check_result_pause(Limiter)
when
    Limiter :: limiter().
return_pause(infinity, PauseType, Fun, Diff, Limiter) ->
    %% workaround when emqx_limiter_server's rate is infinity
    {PauseType, ?MINIMUM_PAUSE, make_retry_context(Fun, Diff), Limiter};
return_pause(Rate, PauseType, Fun, Diff, Limiter) ->
    Val = erlang:round(Diff * emqx_limiter_schema:default_period() / Rate),
    Pause = emqx_misc:clamp(Val, ?MINIMUM_PAUSE, ?MAXIMUM_PAUSE),
    {PauseType, Pause, make_retry_context(Fun, Diff), Limiter}.

-spec make_retry_context(undefined | retry_fun(Limiter), non_neg_integer()) ->
    retry_context(Limiter)
when
    Limiter :: limiter().
make_retry_context(Fun, Diff) ->
    #{continuation => Fun, diff => Diff}.

-spec try_restore(retry_context(Limiter), Limiter) -> Limiter when
    Limiter :: limiter().
try_restore(
    #{need := Need, diff := Diff},
    #{tokens := Tokens, capacity := Capacity, bucket := Bucket} = Limiter
) ->
    Back = Need - Diff,
    Tokens2 = erlang:min(Capacity, Back + Tokens),
    emqx_limiter_bucket_ref:try_restore(Back, Bucket),
    Limiter#{tokens := Tokens2};
try_restore(#{need := Need, diff := Diff}, #{bucket := Bucket} = Limiter) ->
    emqx_limiter_bucket_ref:try_restore(Need - Diff, Bucket),
    Limiter.

-spec may_return_or_pause(non_neg_integer(), Limiter) -> check_result(Limiter) when
    Limiter :: limiter().
may_return_or_pause(Left, #{low_watermark := Mark} = Limiter) when Left >= Mark ->
    {ok, Limiter};
may_return_or_pause(_, Limiter) ->
    {pause, ?MINIMUM_PAUSE, make_retry_context(undefined, 0), Limiter}.

%% @doc apply the elapsed time to the limiter
apply_elapsed_time(Rate, Elapsed, Tokens, Capacity) ->
    Inc = floor_div(mul(Elapsed, Rate), emqx_limiter_schema:default_period()),
    erlang:min(add(Tokens, Inc), Capacity).
