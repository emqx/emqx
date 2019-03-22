%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module manages an opaque collection of statistics data used to
%% force garbage collection on `self()' process when hitting thresholds.
%% Namely:
%% (1) Total number of messages passed through
%% (2) Total data volume passed through
%% @end

-module(emqx_gc).

-include("types.hrl").

-export([ init/1
        , run/3
        , info/1
        , reset/1
        ]).

-type(opts() :: #{count => integer(),
                  bytes => integer()}).

-type(st() :: #{cnt => {integer(), integer()},
                oct => {integer(), integer()}}).

-opaque(gc_state() :: {?MODULE, st()}).

-export_type([gc_state/0]).

-define(GCS(St), {?MODULE, St}).

-define(disabled, disabled).
-define(ENABLED(X), (is_integer(X) andalso X > 0)).

%% @doc Initialize force GC state.
-spec(init(opts() | false) -> maybe(gc_state())).
init(#{count := Count, bytes := Bytes}) ->
    Cnt = [{cnt, {Count, Count}} || ?ENABLED(Count)],
    Oct = [{oct, {Bytes, Bytes}} || ?ENABLED(Bytes)],
    ?GCS(maps:from_list(Cnt ++ Oct));
init(false) -> undefined.

%% @doc Try to run GC based on reduntions of count or bytes.
-spec(run(pos_integer(), pos_integer(), gc_state())
      -> {boolean(), gc_state()}).
run(Cnt, Oct, ?GCS(St)) ->
    {Res, St1} = run([{cnt, Cnt}, {oct, Oct}], St),
    {Res, ?GCS(St1)};
run(_Cnt, _Oct, undefined) ->
    {false, undefined}.

run([], St) ->
    {false, St};
run([{K, N}|T], St) ->
    case dec(K, N, St) of
        {true, St1} ->
            {true, do_gc(St1)};
        {false, St1} ->
            run(T, St1)
    end.

%% @doc Info of GC state.
-spec(info(gc_state()) -> maybe(map())).
info(?GCS(St)) ->
    St;
info(undefined) ->
    undefined.

%% @doc Reset counters to zero.
-spec(reset(gc_state()) -> gc_state()).
reset(?GCS(St)) ->
    ?GCS(do_reset(St));
reset(undefined) ->
    undefined.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

-spec(dec(cnt | oct, pos_integer(), st()) -> {boolean(), st()}).
dec(Key, Num, St) ->
    case maps:get(Key, St, ?disabled) of
        ?disabled ->
            {false, St};
        {Init, Remain} when Remain > Num ->
            {false, maps:put(Key, {Init, Remain - Num}, St)};
        _ ->
            {true, St}
    end.

do_gc(St) ->
    true = erlang:garbage_collect(),
    do_reset(St).

do_reset(St) ->
    do_reset(cnt, do_reset(oct, St)).

%% Reset counters to zero.
do_reset(Key, St) ->
    case maps:get(Key, St, ?disabled) of
        ?disabled -> St;
        {Init, _} -> maps:put(Key, {Init, Init}, St)
    end.

