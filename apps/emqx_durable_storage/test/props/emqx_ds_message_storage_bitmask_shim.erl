%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ds_message_storage_bitmask_shim).

-include_lib("emqx/include/emqx.hrl").

-export([open/0]).
-export([close/1]).
-export([store/2]).
-export([iterate/2]).

-opaque t() :: ets:tid().

-export_type([t/0]).

-spec open() -> t().
open() ->
    ets:new(?MODULE, [ordered_set, {keypos, 1}]).

-spec close(t()) -> ok.
close(Tab) ->
    true = ets:delete(Tab),
    ok.

-spec store(t(), emqx_types:message()) ->
    ok | {error, _TODO}.
store(Tab, Msg = #message{id = MessageID, timestamp = PublishedAt}) ->
    true = ets:insert(Tab, {{PublishedAt, MessageID}, Msg}),
    ok.

-spec iterate(t(), emqx_ds:replay()) ->
    [binary()].
iterate(Tab, {TopicFilter0, StartTime}) ->
    TopicFilter = iolist_to_binary(lists:join("/", TopicFilter0)),
    ets:foldr(
        fun({{PublishedAt, _}, Msg = #message{topic = Topic}}, Acc) ->
            case emqx_topic:match(Topic, TopicFilter) of
                true when PublishedAt >= StartTime ->
                    [Msg | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        Tab
    ).
