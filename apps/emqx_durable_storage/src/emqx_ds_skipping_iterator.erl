%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ds_skipping_iterator).

-include("emqx_ds_skipping_iterator.hrl").
-include("emqx/include/emqx_mqtt.hrl").

-type t() :: ?skipping_iterator(emqx_ds:iterator(), non_neg_integer(), non_neg_integer()).

-export([
    update_or_new/3,
    update_iterator/3,
    next/3
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec update_or_new(t() | emqx_ds:iterator(), non_neg_integer(), non_neg_integer()) -> t().
update_or_new(?skipping_iterator_match(Iterator, Q1Skip0, Q2Skip0), Q1Skip, Q2Skip) when
    Q1Skip >= 0 andalso Q2Skip >= 0
->
    ?skipping_iterator(Iterator, Q1Skip0 + Q1Skip, Q2Skip0 + Q2Skip);
update_or_new(Iterator, Q1Skip, Q2Skip) when Q1Skip >= 0 andalso Q2Skip >= 0 ->
    ?skipping_iterator(Iterator, Q1Skip, Q2Skip).

-spec next(emqx_ds:db(), t(), pos_integer()) -> emqx_ds:next_result(t()).
next(DB, ?skipping_iterator_match(Iterator0, Q1Skip0, Q2Skip0), Count) ->
    case emqx_ds:next(DB, Iterator0, Count) of
        {error, _, _} = Error ->
            Error;
        {ok, end_of_stream} ->
            {ok, end_of_stream};
        {ok, Iterator1, Messages0} ->
            {Messages1, Q1Skip1, Q2Skip1} = skip(Messages0, Q1Skip0, Q2Skip0),
            case {Q1Skip1, Q2Skip1} of
                {0, 0} -> {ok, Iterator1, Messages1};
                _ -> {ok, ?skipping_iterator(Iterator1, Q1Skip1, Q2Skip1)}
            end
    end.

-spec update_iterator(emqx_ds:db(), emqx_ds:iterator(), emqx_ds:message_key()) ->
    emqx_ds:make_iterator_result().
update_iterator(DB, ?skipping_iterator_match(Iterator0, Q1Skip0, Q2Skip0), Key) ->
    case emqx_ds:update_iterator(DB, Iterator0, Key) of
        {error, _, _} = Error -> Error;
        {ok, Iterator1} -> {ok, ?skipping_iterator(Iterator1, Q1Skip0, Q2Skip0)}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

skip(Messages, Q1Skip, Q2Skip) ->
    skip(Messages, Q1Skip, Q2Skip, []).

skip([], Q1Skip, Q2Skip, Agg) ->
    {lists:reverse(Agg), Q1Skip, Q2Skip};
skip([{Key, Message} | Messages], Q1Skip, Q2Skip, Agg) ->
    Qos = emqx_message:qos(Message),
    skip({Key, Message}, Qos, Messages, Q1Skip, Q2Skip, Agg).

skip(_KeyMessage, ?QOS_0, Messages, Q1Skip, Q2Skip, Agg) ->
    skip(Messages, Q1Skip, Q2Skip, Agg);
skip(_KeyMessage, ?QOS_1, Messages, Q1Skip, Q2Skip, Agg) when Q1Skip > 0 ->
    skip(Messages, Q1Skip - 1, Q2Skip, Agg);
skip(KeyMessage, ?QOS_1, Messages, 0, Q2Skip, Agg) ->
    skip(Messages, 0, Q2Skip, [KeyMessage | Agg]);
skip(_KeyMessage, ?QOS_2, Messages, Q1Skip, Q2Skip, Agg) when Q2Skip > 0 ->
    skip(Messages, Q1Skip, Q2Skip - 1, Agg);
skip(KeyMessage, ?QOS_2, Messages, Q1Skip, 0, Agg) ->
    skip(Messages, Q1Skip, 0, [KeyMessage | Agg]).
