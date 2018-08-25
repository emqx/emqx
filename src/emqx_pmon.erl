%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_pmon).

-export([new/0]).
-export([monitor/2, monitor/3]).
-export([demonitor/2]).
-export([find/2]).
-export([erase/2]).

-compile({no_auto_import,[monitor/3]}).

-type(pmon() :: {?MODULE, map()}).
-export_type([pmon/0]).

-spec(new() -> pmon()).
new() -> {?MODULE, maps:new()}.

-spec(monitor(pid(), pmon()) -> pmon()).
monitor(Pid, PM) ->
    monitor(Pid, undefined, PM).

monitor(Pid, Val, {?MODULE, PM}) ->
    {?MODULE, case maps:is_key(Pid, PM) of
                  true  -> PM;
                  false -> Ref = erlang:monitor(process, Pid),
                           maps:put(Pid, {Ref, Val}, PM)
              end}.

-spec(demonitor(pid(), pmon()) -> pmon()).
demonitor(Pid, {?MODULE, PM}) ->
    {?MODULE, case maps:find(Pid, PM) of
                  {ok, {Ref, _Val}} ->
                      %% Don't flush
                      _ = erlang:demonitor(Ref),
                      maps:remove(Pid, PM);
                  error -> PM
              end}.

-spec(find(pid(), pmon()) -> undefined | term()).
find(Pid, {?MODULE, PM}) ->
    case maps:find(Pid, PM) of
        {ok, {_Ref, Val}} ->
            Val;
        error -> undefined
    end.

-spec(erase(pid(), pmon()) -> pmon()).
erase(Pid, {?MODULE, PM}) ->
    {?MODULE, maps:remove(Pid, PM)}.

