%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_limiter_schema_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

to_rate(Str) ->
    emqx_limiter_schema:to_rate(Str).

to_burst(Str) ->
    emqx_limiter_schema:to_burst(Str).

parse(Str, Type) ->
    typerefl:from_string(Type, Str).

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

t_rate_to_str_no_unit_prop(_Config) ->
    ?assert(
        proper:quickcheck(prop_rate_to_str_no_unit(), [{numtests, 1000}, {to_file, user}])
    ),
    ok.

t_rate_to_str_bytes_prop(_Config) ->
    ?assert(
        proper:quickcheck(prop_rate_to_str_bytes(), [{numtests, 1000}, {to_file, user}])
    ),
    ok.

prop_rate_to_str_no_unit() ->
    TimeUnits = [<<"d">>, <<"h">>, <<"m">>, <<"s">>, <<"ms">>],
    ?FORALL(
        {Capacity, RawDuration},
        {pos_integer(), emqx_proper_types:raw_duration(TimeUnits)},
        begin
            Raw0 = <<(integer_to_binary(Capacity))/binary, "/", RawDuration/binary>>,
            {ok, Parsed0} = parse(Raw0, emqx_limiter_schema:rate_type()),
            Pretty = emqx_limiter_schema:rate_to_str(Parsed0, no_unit),
            {ok, Parsed1} = parse(Pretty, emqx_limiter_schema:rate_type()),
            Parsed0 =:= Parsed1
        end
    ).

prop_rate_to_str_bytes() ->
    TimeUnits = [<<"d">>, <<"h">>, <<"m">>, <<"s">>, <<"ms">>],
    ?FORALL(
        {RawCapacity, RawDuration},
        {emqx_proper_types:raw_byte_size(), emqx_proper_types:raw_duration(TimeUnits)},
        begin
            Raw0 = <<RawCapacity/binary, "/", RawDuration/binary>>,
            {ok, Parsed0} = parse(Raw0, emqx_limiter_schema:rate_type()),
            Pretty = emqx_limiter_schema:rate_to_str(Parsed0, bytes),
            {ok, Parsed1} = parse(Pretty, emqx_limiter_schema:rate_type()),
            Parsed0 =:= Parsed1
        end
    ).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_rate(_) ->
    %% infinity
    ?assertEqual({ok, infinity}, to_rate(" infinity ")),

    %% xMB
    ?assertEqual({ok, {100, 1000}}, to_rate("100")),
    ?assertEqual({ok, {100, 1000}}, to_rate("  100   ")),
    ?assertEqual({ok, {100 * 1024 * 1024, 1000}}, to_rate("100MB")),

    %% xMB/s
    ?assertEqual({ok, {100, 1000}}, to_rate("100/s")),
    ?assertEqual({ok, {100 * 1024 * 1024, 1000}}, to_rate("100MB/s")),

    %% xMB/ys
    ?assertEqual({ok, {100, 10000}}, to_rate("100/10s")),
    ?assertEqual({ok, {100 * 1024 * 1024, 10000}}, to_rate("100mB/10s")),

    ?assertMatch({error, _}, to_rate("infini")),
    ?assertMatch({error, _}, to_rate("0")),
    ?assertMatch({error, _}, to_rate("MB")),
    ?assertMatch({error, _}, to_rate("10s")),
    ?assertMatch({error, _}, to_rate("100MB/")),
    ?assertMatch({error, _}, to_rate("100MB/xx")),
    ?assertMatch({error, _}, to_rate("100MB/1")),
    ?assertMatch({error, _}, to_rate("100/10x")),

    ok.

t_burst(_) ->
    %% Zero is valid for burst
    ?assertMatch({ok, {0, 1000}}, to_burst("0")),

    %% xMB
    ?assertEqual({ok, {100, 1000}}, to_burst("100")),
    ?assertEqual({ok, {100, 1000}}, to_burst("  100   ")),
    ?assertEqual({ok, {100 * 1024 * 1024, 1000}}, to_burst("100MB")),

    %% xMB/s
    ?assertEqual({ok, {100, 1000}}, to_burst("100/s")),
    ?assertEqual({ok, {100 * 1024 * 1024, 1000}}, to_burst("100MB/s")),

    %% xMB/ys
    ?assertEqual({ok, {100, 10000}}, to_burst("100/10s")),
    ?assertEqual({ok, {100 * 1024 * 1024, 10000}}, to_burst("100mB/10s")),

    %% burst cannot be infinity
    ?assertMatch({error, _}, to_burst("infinity")),
    ?assertMatch({error, _}, to_burst("infini")),
    ?assertMatch({error, _}, to_burst("MB")),
    ?assertMatch({error, _}, to_burst("10s")),
    ?assertMatch({error, _}, to_burst("100MB/")),
    ?assertMatch({error, _}, to_burst("100MB/xx")),
    ?assertMatch({error, _}, to_burst("100MB/1")),
    ?assertMatch({error, _}, to_burst("100/10x")),

    ok.
