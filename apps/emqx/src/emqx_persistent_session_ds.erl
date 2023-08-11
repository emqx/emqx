%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_ds).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([init/0]).

-export([
    persist_message/1,
    open_session/1,
    add_subscription/2
]).

-export([
    serialize_message/1,
    deserialize_message/1
]).

%% RPC
-export([do_open_iterator/3]).

%% FIXME
-define(DS_SHARD, <<"local">>).

-define(WHEN_ENABLED(DO),
    case is_store_enabled() of
        true -> DO;
        false -> {skipped, disabled}
    end
).

%%

init() ->
    ?WHEN_ENABLED(
        ok = emqx_ds:ensure_shard(?DS_SHARD, #{
            dir => filename:join([emqx:data_dir(), ds, messages, ?DS_SHARD])
        })
    ).

%%

-spec persist_message(emqx_types:message()) ->
    ok | {skipped, _Reason} | {error, _TODO}.
persist_message(Msg) ->
    ?WHEN_ENABLED(
        case needs_persistence(Msg) andalso find_subscribers(Msg) of
            [_ | _] ->
                store_message(Msg);
            % [] ->
            %     {skipped, no_subscribers};
            false ->
                {skipped, needs_no_persistence}
        end
    ).

needs_persistence(Msg) ->
    not (emqx_message:get_flag(dup, Msg) orelse emqx_message:is_sys(Msg)).

store_message(Msg) ->
    ID = emqx_message:id(Msg),
    Timestamp = emqx_guid:timestamp(ID),
    Topic = emqx_topic:words(emqx_message:topic(Msg)),
    emqx_ds_storage_layer:store(?DS_SHARD, ID, Timestamp, Topic, serialize_message(Msg)).

find_subscribers(_Msg) ->
    [node()].

open_session(ClientID) ->
    ?WHEN_ENABLED(emqx_ds:session_open(ClientID)).

-spec add_subscription(emqx_types:topic(), emqx_ds:session_id()) ->
    {ok, emqx_ds:iterator_id(), _IsNew :: boolean()} | {skipped, disabled}.
add_subscription(TopicFilterBin, DSSessionID) ->
    ?WHEN_ENABLED(
        begin
            TopicFilter = emqx_topic:words(TopicFilterBin),
            {ok, IteratorID, StartMS, IsNew} = emqx_ds:session_add_iterator(
                DSSessionID, TopicFilter
            ),
            case IsNew of
                true ->
                    ok = open_iterator_on_all_nodes(TopicFilter, StartMS, IteratorID);
                false ->
                    ok
            end,
            {ok, IteratorID, IsNew}
        end
    ).

-spec open_iterator_on_all_nodes(emqx_topic:words(), emqx_ds:time(), emqx_ds:iterator_id()) -> ok.
open_iterator_on_all_nodes(TopicFilter, StartMS, IteratorID) ->
    Nodes = emqx:running_nodes(),
    Results = emqx_persistent_session_ds_proto_v1:open_iterator(Nodes, TopicFilter, StartMS, IteratorID),
    %% TODO: handle errors
    true = lists:all(fun(Res) -> Res =:= {ok, ok} end, Results),
    ok.

-spec do_open_iterator(emqx_topic:words(), emqx_ds:time(), emqx_ds:iterator_id()) -> ok.
do_open_iterator(TopicFilter, StartMS, IteratorID) ->
    Replay = {TopicFilter, StartMS},
    %% FIXME: choose DS shard based on ...?
    {ok, It} = emqx_ds_storage_layer:make_iterator(?DS_SHARD, Replay),
    ok = emqx_ds_storage_layer:preserve_iterator(It, IteratorID),
    ok.

%%

serialize_message(Msg) ->
    term_to_binary(emqx_message:to_map(Msg)).

deserialize_message(Bin) ->
    emqx_message:from_map(binary_to_term(Bin)).

%%

is_store_enabled() ->
    emqx_config:get([persistent_session_store, ds]).
