%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqttd_cli_SUITE).

-compile(export_all).

-include("emqttd.hrl").

-include_lib("eunit/include/eunit.hrl").

all() ->
    [{group, subscriptions}].

groups() ->
    [{subscriptions, [sequence],
      [t_subsciptions_list,
       t_subsciptions_show,
       t_subsciptions_add,
       t_subsciptions_del]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    todo.

t_subsciptions_list(_) ->
    todo.

t_subsciptions_show(_) ->
    todo.

t_subsciptions_add(_) ->
    todo.

t_subsciptions_del(_) ->
    todo.

