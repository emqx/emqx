%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([
    on_start/2,
    on_stop/2,
    on_query/4,
    on_get_status/2
]).

%% callbacks for emqx_resource config schema
-export([roots/0]).

roots() ->
    [
        {name, fun name/1},
        {register, fun register/1}
    ].

name(type) -> atom();
name(required) -> true;
name(_) -> undefined.

register(type) -> boolean();
register(required) -> true;
register(default) -> false;
register(_) -> undefined.

on_start(_InstId, #{create_error := true}) ->
    error("some error");
on_start(InstId, #{name := Name, stop_error := true} = Opts) ->
    Register = maps:get(register, Opts, false),
    {ok, #{
        name => Name,
        id => InstId,
        stop_error => true,
        pid => spawn_dummy_process(Name, Register)
    }};
on_start(InstId, #{name := Name, health_check_error := true} = Opts) ->
    Register = maps:get(register, Opts, false),
    {ok, #{
        name => Name,
        id => InstId,
        health_check_error => true,
        pid => spawn_dummy_process(Name, Register)
    }};
on_start(InstId, #{name := Name} = Opts) ->
    Register = maps:get(register, Opts, false),
    {ok, #{
        name => Name,
        id => InstId,
        pid => spawn_dummy_process(Name, Register)
    }}.

on_stop(_InstId, #{stop_error := true}) ->
    {error, stop_error};
on_stop(_InstId, #{pid := Pid}) ->
    erlang:exit(Pid, shutdown),
    ok.

on_query(_InstId, get_state, AfterQuery, State) ->
    emqx_resource:query_success(AfterQuery),
    State;
on_query(_InstId, get_state_failed, AfterQuery, State) ->
    emqx_resource:query_failed(AfterQuery),
    State.

on_get_status(_InstId, #{health_check_error := true}) ->
    disconnected;
on_get_status(_InstId, #{pid := Pid}) ->
    timer:sleep(300),
    case is_process_alive(Pid) of
        true -> connected;
        false -> connecting
    end.

spawn_dummy_process(Name, Register) ->
    spawn(
        fun() ->
            true =
                case Register of
                    true -> register(Name, self());
                    _ -> true
                end,
            Ref = make_ref(),
            receive
                Ref -> ok
            end
        end
    ).
