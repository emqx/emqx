%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Conf modules for emqx-ocpp gateway
-module(emqx_ocpp_conf).

-export([
    load/1,
    unload/0,
    get_env/1,
    get_env/2
]).

-export([
    default_heartbeat_interval/0,
    heartbeat_checking_times_backoff/0,
    retry_interval/0,
    awaiting_timeout/0,
    message_format_checking/0,
    max_mqueue_len/0,
    strit_mode/1,
    uptopic/1,
    up_reply_topic/0,
    up_error_topic/0,
    dntopic/0
]).

-define(KEY(Key), {?MODULE, Key}).

load(Confs) ->
    lists:foreach(fun({K, V}) -> store(K, V) end, Confs).

get_env(K) ->
    get_env(K, undefined).

get_env(K, Default) ->
    try
        persistent_term:get(?KEY(K))
    catch
        error:badarg ->
            Default
    end.

-spec default_heartbeat_interval() -> pos_integer().
default_heartbeat_interval() ->
    get_env(default_heartbeat_interval, 600).

-spec heartbeat_checking_times_backoff() -> pos_integer().
heartbeat_checking_times_backoff() ->
    get_env(heartbeat_checking_times_backoff, 1).

-spec strit_mode(upstream | dnstream) -> boolean().
strit_mode(dnstream) ->
    dnstream(strit_mode, false);
strit_mode(upstream) ->
    upstream(strit_mode, false).

-spec retry_interval() -> pos_integer().
retry_interval() ->
    dnstream(retry_interval, 30).

-spec max_mqueue_len() -> pos_integer().
max_mqueue_len() ->
    dnstream(max_mqueue_len, 10).

-spec awaiting_timeout() -> pos_integer().
awaiting_timeout() ->
    upstream(awaiting_timeout, 30).

-spec message_format_checking() ->
    all
    | upstream_only
    | dnstream_only
    | disable.
message_format_checking() ->
    get_env(message_format_checking, all).

uptopic(Action) ->
    Topic = upstream(topic),
    Mapping = upstream(mapping, #{}),
    maps:get(Action, Mapping, Topic).

up_reply_topic() ->
    upstream(reply_topic).

up_error_topic() ->
    upstream(error_topic).

dntopic() ->
    dnstream(topic).

-spec unload() -> ok.
unload() ->
    lists:foreach(
        fun
            ({?KEY(K), _}) -> persistent_term:erase(?KEY(K));
            (_) -> ok
        end,
        persistent_term:get()
    ).

%%--------------------------------------------------------------------
%% internal funcs
%%--------------------------------------------------------------------

dnstream(K) ->
    dnstream(K, undefined).

dnstream(K, Def) ->
    L = get_env(dnstream, []),
    proplists:get_value(K, L, Def).

upstream(K) ->
    upstream(K, undefined).

upstream(K, Def) ->
    L = get_env(upstream, []),
    proplists:get_value(K, L, Def).

store(upstream, L) ->
    L1 = preproc([topic, reply_topic, error_topic], L),
    Mapping = proplists:get_value(mapping, L1, #{}),
    NMappings = maps:map(
        fun(_, V) -> emqx_placeholder:preproc_tmpl(V) end,
        Mapping
    ),
    L2 = lists:keyreplace(mapping, 1, L1, {mapping, NMappings}),
    persistent_term:put(?KEY(upstream), L2);
store(dnstream, L) ->
    L1 = preproc([topic], L),
    persistent_term:put(?KEY(dnstream), L1);
store(K, V) ->
    persistent_term:put(?KEY(K), V).

preproc([], L) ->
    L;
preproc([Key | More], L) ->
    Val0 = proplists:get_value(Key, L),
    Val = emqx_placeholder:preproc_tmpl(Val0),
    preproc(More, lists:keyreplace(Key, 1, L, {Key, Val})).
