-module(log_tracer).

-include_lib("emqx_resource/include/emqx_resource_behaviour.hrl").

%% callbacks of behaviour emqx_resource
-export([ on_start/2
        , on_stop/2
        , on_query/4
        , on_health_check/2
        , on_api_reply_format/1
        , on_config_merge/3
        ]).

%% callbacks for emqx_resource config schema
-export([schema/0]).

schema() ->
    log_tracer_schema:schema().

on_start(InstId, Config) ->
    io:format("== the demo log tracer ~p started.~nconfig: ~p~n", [InstId, Config]),
    {ok, #{logger_handler_id => abc, health_checked => 0}}.

on_stop(InstId, State) ->
    io:format("== the demo log tracer ~p stopped.~nstate: ~p~n", [InstId, State]),
    ok.

on_query(InstId, Request, AfterQuery, State) ->
    io:format("== the demo log tracer ~p received request: ~p~nstate: ~p~n",
        [InstId, Request, State]),
    emqx_resource:query_success(AfterQuery),
    "this is a demo log messages...".

on_health_check(InstId, State = #{health_checked := Checked}) ->
    NState = State#{health_checked => Checked + 1},
    io:format("== the demo log tracer ~p is working well~nstate: ~p~n", [InstId, NState]),
    {ok, NState}.

on_api_reply_format(#{id := Id, status := Status, state := #{health_checked := NChecked}}) ->
    #{id => Id, status => Status, checked_count => NChecked}.

on_config_merge(OldConfig, NewConfig, _Params) ->
    maps:merge(OldConfig, NewConfig).
