%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_subscription).

-behaviour(emqx_gen_mod).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        , description/0
        ]).

%% APIs
-export([on_client_connected/3]).

%%--------------------------------------------------------------------
%% Load/Unload Hook
%%--------------------------------------------------------------------

load(Topics) ->
    emqx_hooks:add('client.connected', {?MODULE, on_client_connected, [Topics]}).

on_client_connected(#{clientid := ClientId, username := Username}, _ConnInfo = #{proto_ver := ProtoVer}, Topics) ->

    OptFun =  case ProtoVer of
                  ?MQTT_PROTO_V5 -> fun(X) -> X end;
                  _ -> fun(#{qos := Qos}) -> #{qos => Qos} end
              end,

    Fold = fun({Topic, SubOpts}, Acc) ->
                   case rep(Topic, ClientId, Username) of
                       {error, Reason} ->
                           ?LOG(warning, "auto subscribe ignored, topic filter:~ts reason:~p~n",
                                [Topic, Reason]),
                           Acc;
                       <<>> ->
                           ?LOG(warning, "auto subscribe ignored, topic filter:~ts"
                                " reason: topic can't be empty~n",
                                [Topic]),
                           Acc;
                       NewTopic ->
                           [{NewTopic, OptFun(SubOpts)} | Acc]
                   end
           end,

    case lists:foldl(Fold, [], Topics) of
        [] -> ok;
        TopicFilters ->
            self() ! {subscribe, TopicFilters}
    end.

unload(_) ->
    emqx_hooks:del('client.connected', {?MODULE, on_client_connected}).

description() ->
    "EMQX Subscription Module".
%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

rep(Topic, ClientId, Username) ->
    Words = emqx_topic:words(Topic),
    rep(Words, ClientId, Username, []).

rep([<<"%c">> | T], ClientId, Username, Acc) ->
    rep(T,
        ClientId,
        Username,
        [ClientId | Acc]);
rep([<<"%u">> | _], _, undefined, _) ->
    {error, username_undefined};
rep([<<"%u">> | T], ClientId, Username, Acc) ->
    rep(T,
        ClientId,
        Username,
        [Username | Acc]);
rep([H | T], ClientId, UserName, Acc) ->
    rep(T, ClientId, UserName, [H | Acc]);

rep([], _, _, Acc) ->
    emqx_topic:join(lists:reverse(Acc)).
