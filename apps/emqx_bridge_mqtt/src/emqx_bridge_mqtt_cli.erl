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

-module(emqx_bridge_mqtt_cli).

-include("emqx_bridge_mqtt.hrl").

-import(lists, [foreach/2]).

-export([cli/1]).

cli(["list"]) ->
    foreach(fun({Name, State0}) ->
                State = case State0 of
                            connected -> <<"Running">>;
                            _ -> <<"Stopped">>
                        end,
                emqx_ctl:print("name: ~s     status: ~s~n", [Name, State])
            end, emqx_bridge_mqtt_sup:bridges());

cli(["start", Name]) ->
    emqx_ctl:print("~s.~n", [try emqx_bridge_worker:ensure_started(Name) of
                                 ok -> <<"Start bridge successfully">>;
                                 connected -> <<"Bridge already started">>;
                                 _ -> <<"Start bridge failed">>
                             catch
                                 _Error:_Reason ->
                                     <<"Start bridge failed">>
                             end]);

cli(["stop", Name]) ->
    emqx_ctl:print("~s.~n", [try emqx_bridge_worker:ensure_stopped(Name) of
                                 ok -> <<"Stop bridge successfully">>;
                                 _ -> <<"Stop bridge failed">>
                             catch
                                 _Error:_Reason ->
                                     <<"Stop bridge failed">>
                             end]);

cli(["forwards", Name]) ->
    foreach(fun(Topic) ->
                emqx_ctl:print("topic:   ~s~n", [Topic])
            end, emqx_bridge_worker:get_forwards(Name));

cli(["add-forward", Name, Topic]) ->
    ok = emqx_bridge_worker:ensure_forward_present(Name, iolist_to_binary(Topic)),
    emqx_ctl:print("Add-forward topic successfully.~n");

cli(["del-forward", Name, Topic]) ->
    ok = emqx_bridge_worker:ensure_forward_absent(Name, iolist_to_binary(Topic)),
    emqx_ctl:print("Del-forward topic successfully.~n");

cli(["subscriptions", Name]) ->
    foreach(fun({Topic, Qos}) ->
                emqx_ctl:print("topic: ~s, qos: ~p~n", [Topic, Qos])
            end, emqx_bridge_worker:get_subscriptions(Name));

cli(["add-subscription", Name, Topic, Qos]) ->
    case emqx_bridge_worker:ensure_subscription_present(Name, Topic, list_to_integer(Qos)) of
        ok -> emqx_ctl:print("Add-subscription topic successfully.~n");
        {error, Reason} -> emqx_ctl:print("Add-subscription failed reason: ~p.~n", [Reason])
    end;

cli(["del-subscription", Name, Topic]) ->
    ok = emqx_bridge_worker:ensure_subscription_absent(Name, Topic),
    emqx_ctl:print("Del-subscription topic successfully.~n");

cli(_) ->
    emqx_ctl:usage([{"bridges list",           "List bridges"},
                    {"bridges start <Name>",   "Start a bridge"},
                    {"bridges stop <Name>",    "Stop a bridge"},
                    {"bridges forwards <Name>", "Show a bridge forward topic"},
                    {"bridges add-forward <Name> <Topic>", "Add bridge forward topic"},
                    {"bridges del-forward <Name> <Topic>", "Delete bridge forward topic"},
                    {"bridges subscriptions <Name>", "Show a bridge subscriptions topic"},
                    {"bridges add-subscription <Name> <Topic> <Qos>", "Add bridge subscriptions topic"},
                    {"bridges del-subscription <Name> <Topic>", "Delete bridge subscriptions topic"}]).


