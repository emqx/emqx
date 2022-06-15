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

-module(emqx_auto_subscribe).

-include_lib("emqx/include/emqx_hooks.hrl").

-define(HOOK_POINT, 'client.connected').

-define(MAX_AUTO_SUBSCRIBE, 20).

%
-export([load/0, unload/0]).

-export([
    max_limit/0,
    list/0,
    update/1,
    post_config_update/5
]).

%% hook callback
-export([on_client_connected/3]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

load() ->
    ok = emqx_conf:add_handler([auto_subscribe, topics], ?MODULE),
    update_hook().

unload() ->
    emqx_conf:remove_handler([auto_subscribe, topics]).

max_limit() ->
    ?MAX_AUTO_SUBSCRIBE.

list() ->
    format(emqx_conf:get([auto_subscribe, topics], [])).

update(Topics) ->
    update_(Topics).

post_config_update(_KeyPath, _Req, NewTopics, _OldConf, _AppEnvs) ->
    Config = emqx_conf:get([auto_subscribe], #{}),
    update_hook(Config#{topics => NewTopics}).

%%--------------------------------------------------------------------
%% hook

on_client_connected(ClientInfo, ConnInfo, {TopicHandler, Options}) ->
    case erlang:apply(TopicHandler, handle, [ClientInfo, ConnInfo, Options]) of
        [] ->
            ok;
        TopicTables ->
            _ = self() ! {subscribe, TopicTables},
            ok
    end;
on_client_connected(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% Telemetry
%%--------------------------------------------------------------------

-spec get_basic_usage_info() -> #{auto_subscribe_count => non_neg_integer()}.
get_basic_usage_info() ->
    AutoSubscribe = emqx_conf:get([auto_subscribe, topics], []),
    #{auto_subscribe_count => length(AutoSubscribe)}.

%%--------------------------------------------------------------------
%% internal

format(Rules) when is_list(Rules) ->
    [format(Rule) || Rule <- Rules];
format(Rule = #{topic := Topic}) when is_map(Rule) ->
    #{
        topic => Topic,
        qos => maps:get(qos, Rule, 0),
        rh => maps:get(rh, Rule, 0),
        rap => maps:get(rap, Rule, 0),
        nl => maps:get(nl, Rule, 0)
    }.

update_(Topics) when length(Topics) =< ?MAX_AUTO_SUBSCRIBE ->
    case
        emqx_conf:update(
            [auto_subscribe, topics],
            Topics,
            #{rawconf_with_defaults => true, override_to => cluster}
        )
    of
        {ok, #{raw_config := NewTopics}} ->
            {ok, NewTopics};
        {error, Reason} ->
            {error, Reason}
    end;
update_(_Topics) ->
    {error, quota_exceeded}.

update_hook() ->
    update_hook(emqx_conf:get([auto_subscribe], #{})).

update_hook(Config) ->
    {TopicHandler, Options} = emqx_auto_subscribe_handler:init(Config),
    emqx_hooks:put(
        ?HOOK_POINT, {?MODULE, on_client_connected, [{TopicHandler, Options}]}, ?HP_AUTO_SUB
    ),
    ok.
