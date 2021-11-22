%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_test_resource).

-include_lib("typerefl/include/types.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        , on_config_merge/3
        ]).

%% callbacks for emqx_resource config schema
-export([roots/0]).

roots() -> [{"name", fun name/1}].

name(type) -> binary();
name(nullable) -> false;
name(_) -> undefined.

on_start(InstId, #{name := Name}) ->
    {ok, #{name => Name,
           id => InstId,
           pid => spawn_dummy_process()}}.

on_stop(_InstId, #{pid := Pid}) ->
    erlang:exit(Pid, shutdown),
    ok.

on_query(_InstId, get_state, AfterQuery, State) ->
    emqx_resource:query_success(AfterQuery),
    State.

on_health_check(_InstId, State = #{pid := Pid}) ->
    case is_process_alive(Pid) of
        true -> {ok, State};
        false -> {error, dead, State}
    end.

on_config_merge(OldConfig, NewConfig, _Params) ->
    maps:merge(OldConfig, NewConfig).

spawn_dummy_process() ->
    spawn(
      fun() ->
              Ref = make_ref(),
              receive
                  Ref -> ok
              end
      end).
