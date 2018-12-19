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

-module(emqx_ctl_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-define(TAB, emqx_command).
-define(State, {seq = 0}).

all() ->
    [register_command_test].

register_command_test() ->
       emqx_ctl:register_command(test1,{?MODULE, test1},[]),
       Reasult = ets:lookup(?TAB, {test0}),
       ct:pal("test result: ~p ~n",Reasult).

  % {setup,
    %     fun() ->
    %         {ok, InitState} = emqx_ctl:init([]),
    %         InitState
    %     end,0
    %     fun(State) ->
    %         ok = emqx_ctl:terminate(shutdown, State)
    %     end,
    %     fun(State = #state{seq = Seq}) ->
    %         emqx_ctl:handle_cast({register_command, test0, {?MODULE, test0}, []}, State),
    %         [?_assertMatch([{{0,test0},{?MODULE, test0}, []}], ets:lookup(?TAB, {Seq,test0}))]
    %     end
    % }.

% unregister_command_test() ->
%         emqx_ctl:register_command(test1,{?MODULE, test1},[]),
%         [?_assertMatch([{{0,test0},{?MODULE, test0}, []}], ets:lookup(?TAB, {?State,test0}))].

% run_command_test() ->
%         ct:pal("start run commad test"),
%            _Msg = emqx_ctl:run_command(),
%         ct:pal("test result: ~p ~n",_Msg).


