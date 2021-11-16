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

roots() -> [{"name", binary()}].

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
