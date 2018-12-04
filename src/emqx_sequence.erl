%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sequence).

-export([create/0, create/1]).
-export([generate/1, generate/2]).
-export([reclaim/1, reclaim/2]).

-type(key() :: term()).
-type(seqid() :: non_neg_integer()).

-define(DEFAULT_TAB, ?MODULE).

%% @doc Create a sequence.
-spec(create() -> ok).
create() ->
    create(?DEFAULT_TAB).

-spec(create(atom()) -> ok).
create(Tab) ->
    _ = ets:new(Tab, [set, public, named_table, {write_concurrency, true}]),
    ok.

%% @doc Generate a sequence id.
-spec(generate(key()) -> seqid()).
generate(Key) ->
    generate(?DEFAULT_TAB, Key).

-spec(generate(atom(), key()) -> seqid()).
generate(Tab, Key) ->
    ets:update_counter(Tab, Key, {2, 1}, {Key, 0}).

%% @doc Reclaim a sequence id.
-spec(reclaim(key()) -> seqid()).
reclaim(Key) ->
    reclaim(?DEFAULT_TAB, Key).

-spec(reclaim(atom(), key()) -> seqid()).
reclaim(Tab, Key) ->
    try ets:update_counter(Tab, Key, {2, -1, 0, 0}) of
        0 -> ets:delete_object(Tab, {Key, 0}), 0;
        I -> I
    catch
        error:badarg -> 0
    end.

