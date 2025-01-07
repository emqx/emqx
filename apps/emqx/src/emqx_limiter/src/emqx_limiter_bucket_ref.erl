%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_bucket_ref).

%% @doc limiter bucket reference
%% this module is used to manage the bucket reference of the limiter server
%% @end

%% API
-export([
    new/3,
    infinity_bucket/0,
    check/3,
    try_restore/2,
    available/1
]).

-export_type([bucket_ref/0]).

-type infinity_bucket_ref() :: infinity.
-type finite_bucket_ref() :: #{
    counter := counters:counters_ref(),
    index := index(),
    rate := rate()
}.

-type bucket_ref() ::
    infinity_bucket_ref()
    | finite_bucket_ref().

-type index() :: emqx_limiter_server:index().
-type rate() :: emqx_limiter_decimal:decimal().
-type check_failure_type() :: partial | pause.

-elvis([{elvis_style, no_if_expression, disable}]).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------
-spec new(counters:counters_ref(), index(), rate()) -> bucket_ref().
new(Counter, Index, Rate) ->
    #{
        counter => Counter,
        index => Index,
        rate => Rate
    }.

-spec infinity_bucket() -> bucket_ref().
infinity_bucket() ->
    infinity.

%% @doc check tokens
-spec check(pos_integer(), bucket_ref(), Disivisble :: boolean()) ->
    HasToken ::
        {ok, emqx_limiter_decimal:decimal()}
        | {check_failure_type(), rate(), pos_integer()}.
check(_, infinity, _) ->
    {ok, infinity};
check(
    Need,
    #{
        counter := Counter,
        index := Index,
        rate := Rate
    },
    Divisible
) ->
    RefToken = counters:get(Counter, Index),
    if
        RefToken >= Need ->
            counters:sub(Counter, Index, Need),
            {ok, RefToken - Need};
        Divisible andalso RefToken > 0 ->
            counters:sub(Counter, Index, RefToken),
            {partial, Rate, RefToken};
        true ->
            {pause, Rate, 0}
    end.

%% @doc try to restore token when consume failed
-spec try_restore(non_neg_integer(), bucket_ref()) -> ok.
try_restore(0, _) ->
    ok;
try_restore(_, infinity) ->
    ok;
try_restore(Inc, #{counter := Counter, index := Index}) ->
    case counters:get(Counter, Index) of
        Tokens when Tokens =< 0 ->
            counters:add(Counter, Index, Inc);
        _ ->
            ok
    end.

%% @doc get the number of tokens currently available
-spec available(bucket_ref()) -> emqx_limiter_decimal:decimal().
available(#{counter := Counter, index := Index}) ->
    counters:get(Counter, Index);
available(infinity) ->
    infinity.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
