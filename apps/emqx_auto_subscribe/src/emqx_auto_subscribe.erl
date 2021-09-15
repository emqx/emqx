%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auto_subscribe).

-define(HOOK_POINT, 'client.connected').

-define(MAX_AUTO_SUBSCRIBE, 20).

-export([load/0]).

-export([ max_limit/0
        , list/0
        , update/1
        , test/1
        ]).

%% hook callback
-export([on_client_connected/3]).

load() ->
    update_hook().

max_limit() ->
    ?MAX_AUTO_SUBSCRIBE.

list() ->
    format(emqx:get_config([auto_subscribe, topics], [])).

update(Topics) ->
    update_(Topics).

test(_) ->
%% TODO: test rule with info map
    ok.

% test(Topic) when is_map(Topic) ->
%     test([Topic]);

% test(Topics) when is_list(Topics) ->
%     PlaceHolders = emqx_auto_subscribe_placeholder:generate(Topics),
%     ClientInfo = #{},
%     ConnInfo = #{},
%     emqx_auto_subscribe_placeholder:to_topic_table([PlaceHolders], ClientInfo, ConnInfo).

%%--------------------------------------------------------------------
%% hook

on_client_connected(ClientInfo, ConnInfo, {TopicHandler, Options}) ->
    TopicTables = erlang:apply(TopicHandler, handle, [ClientInfo, ConnInfo, Options]),
    self() ! {subscribe, TopicTables};
on_client_connected(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% internal

format(Rules) when is_list(Rules) ->
    [format(Rule) || Rule <- Rules];
format(Rule = #{topic := Topic}) when is_map(Rule) ->
    #{
        topic   => Topic,
        qos     => maps:get(qos, Rule, 0),
        rh      => maps:get(rh, Rule, 0),
        rap     => maps:get(rap, Rule, 0),
        nl      => maps:get(nl, Rule, 0)
    }.

update_(Topics) when length(Topics) =< ?MAX_AUTO_SUBSCRIBE ->
    {ok, _} = emqx:update_config([auto_subscribe, topics], Topics),
    update_hook();
update_(_Topics) ->
    {error, quota_exceeded}.

update_hook() ->
    {TopicHandler, Options} = emqx_auto_subscribe_handler:init(),
    emqx_hooks:put(?HOOK_POINT, {?MODULE, on_client_connected, [{TopicHandler, Options}]}),
    ok.
