%%
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
%%

-module(emqttd_inflight_SUITE).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("eunit/include/eunit.hrl").

%% CT
-compile(export_all).

all() -> [t_contain, t_lookup, t_insert, t_update, t_delete, t_window,
          t_is_full, t_is_empty].

t_contain(_) ->
    Inflight = emqttd_inflight:new(0),
    ?assertNot(Inflight:contain(k)),
    Inflight1 = Inflight:insert(k, v),
    ?assert(Inflight1:contain(k)).

t_lookup(_) ->
    Inflight = (emqttd_inflight:new(0)):insert(k, v),
    ?assertEqual(v, Inflight:lookup(k)).

t_insert(_) ->
    Inflight = ((emqttd_inflight:new(0)):insert(k1, v1)):insert(k2, v2),
    ?assertEqual(v2, Inflight:lookup(k2)).

t_update(_) ->
    Inflight = ((emqttd_inflight:new(0)):insert(k, v1)):update(k, v2),
    ?assertEqual(v2, Inflight:lookup(k)).

t_delete(_) ->
    Inflight = ((emqttd_inflight:new(0)):insert(k, v1)):delete(k),
    ?assert(Inflight:is_empty()).

t_window(_) ->
    ?assertEqual([], (emqttd_inflight:new(10)):window()),
    Inflight = ((emqttd_inflight:new(0)):insert(1, 1)):insert(2, 2),
    ?assertEqual([1, 2], Inflight:window()).

t_is_full(_) ->
    Inflight = ((emqttd_inflight:new(1)):insert(k, v1)),
    ?assert(Inflight:is_full()).

t_is_empty(_) ->
    Inflight = ((emqttd_inflight:new(1)):insert(k, v1)),
    ?assertNot(Inflight:is_empty()).

