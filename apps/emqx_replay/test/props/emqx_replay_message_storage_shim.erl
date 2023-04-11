%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_replay_message_storage_shim).

-export([open/0]).
-export([close/1]).
-export([store/5]).
-export([iterate/2]).

-type topic() :: list(binary()).
-type time() :: integer().

-opaque t() :: ets:tid().

-spec open() -> t().
open() ->
    ets:new(?MODULE, [ordered_set, {keypos, 1}]).

-spec close(t()) -> ok.
close(Tab) ->
    true = ets:delete(Tab),
    ok.

-spec store(t(), emqx_guid:guid(), time(), topic(), binary()) ->
    ok | {error, _TODO}.
store(Tab, MessageID, PublishedAt, Topic, Payload) ->
    true = ets:insert(Tab, {{PublishedAt, MessageID}, Topic, Payload}),
    ok.

-spec iterate(t(), emqx_replay:replay()) ->
    [binary()].
iterate(Tab, {TopicFilter, StartTime}) ->
    ets:foldr(
        fun({{PublishedAt, _}, Topic, Payload}, Acc) ->
            case emqx_topic:match(Topic, TopicFilter) of
                true when PublishedAt >= StartTime ->
                    [Payload | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        Tab
    ).
