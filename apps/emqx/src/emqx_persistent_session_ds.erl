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

-export([init/0]).

-export([persist_message/1]).

-export([
    serialize_message/1,
    deserialize_message/1
]).

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

%%

serialize_message(Msg) ->
    term_to_binary(emqx_message:to_map(Msg)).

deserialize_message(Bin) ->
    emqx_message:from_map(binary_to_term(Bin)).

%%

is_store_enabled() ->
    emqx_config:get([persistent_session_store, ds]).
