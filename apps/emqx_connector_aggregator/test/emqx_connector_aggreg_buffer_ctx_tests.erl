%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggreg_buffer_ctx_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_connector_aggregator/include/emqx_connector_aggregator.hrl").

%% 2025-04-22T07:52:03Z — day-of-year 112.
-define(TS, 1745308323).

local_year_bin() ->
    {{Y, _, _}, {_, _, _}} =
        calendar:universal_time_to_local_time(
            calendar:system_time_to_universal_time(?TS, second)
        ),
    list_to_binary(io_lib:format("~4..0B", [Y])).

local_hour_bin() ->
    {{_, _, _}, {H, _, _}} =
        calendar:universal_time_to_local_time(
            calendar:system_time_to_universal_time(?TS, second)
        ),
    list_to_binary(io_lib:format("~2..0B", [H])).

format_timestamp_legacy_test_() ->
    [
        ?_assertEqual(
            "2025-04-22T07:52:03Z",
            emqx_connector_aggreg_buffer_ctx:format_timestamp(?TS, <<"rfc3339utc">>)
        ),
        ?_assertEqual(
            ?TS,
            emqx_connector_aggreg_buffer_ctx:format_timestamp(?TS, <<"unix">>)
        )
    ].

format_timestamp_parts_test_() ->
    [
        ?_assertEqual(
            "2025",
            emqx_connector_aggreg_buffer_ctx:format_timestamp(?TS, <<"YYYY">>)
        ),
        ?_assertEqual(
            "04",
            emqx_connector_aggreg_buffer_ctx:format_timestamp(?TS, <<"MM">>)
        ),
        ?_assertEqual(
            "22",
            emqx_connector_aggreg_buffer_ctx:format_timestamp(?TS, <<"DD">>)
        ),
        ?_assertEqual(
            "07",
            emqx_connector_aggreg_buffer_ctx:format_timestamp(?TS, <<"hh">>)
        ),
        ?_assertEqual(
            "52",
            emqx_connector_aggreg_buffer_ctx:format_timestamp(?TS, <<"mm">>)
        ),
        ?_assertEqual(
            "03",
            emqx_connector_aggreg_buffer_ctx:format_timestamp(?TS, <<"ss">>)
        ),
        ?_assertEqual(
            "112",
            emqx_connector_aggreg_buffer_ctx:format_timestamp(?TS, <<"DOY">>)
        )
    ].

is_valid_datetime_format_test_() ->
    [
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"rfc3339">>)),
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"rfc3339utc">>)),
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"unix">>)),
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"YYYY">>)),
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"MM">>)),
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"DD">>)),
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"hh">>)),
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"mm">>)),
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"ss">>)),
        ?_assert(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"DOY">>)),
        ?_assertNot(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"bogus">>)),
        ?_assertNot(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"YYY">>)),
        ?_assertNot(emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format(<<"">>))
    ].

is_valid_datetime_format_zoned_test_() ->
    Fun = fun emqx_connector_aggreg_buffer_ctx:is_valid_datetime_format/1,
    [
        ?_assert(Fun(<<"utc.YYYY">>)),
        ?_assert(Fun(<<"utc.MM">>)),
        ?_assert(Fun(<<"utc.DOY">>)),
        ?_assert(Fun(<<"local.YYYY">>)),
        ?_assert(Fun(<<"local.hh">>)),
        ?_assertNot(Fun(<<"utc.bogus">>)),
        ?_assertNot(Fun(<<"local.">>)),
        ?_assertNot(Fun(<<"utc.">>)),
        ?_assertNot(Fun(<<"berlin.YYYY">>)),
        %% Zone suffix is only for part tokens, not for full-timestamp formats.
        ?_assertNot(Fun(<<"utc.rfc3339utc">>)),
        ?_assertNot(Fun(<<"local.unix">>))
    ].

lookup_zoned_test_() ->
    Buf = #buffer{since = ?TS, until = ?TS, seq = 0},
    Lookup = fun(Acc) -> emqx_connector_aggreg_buffer_ctx:lookup(Acc, Buf) end,
    [
        ?_assertEqual({ok, "2025"}, Lookup([<<"datetime">>, <<"utc">>, <<"YYYY">>])),
        ?_assertEqual({ok, "04"}, Lookup([<<"datetime">>, <<"utc">>, <<"MM">>])),
        ?_assertEqual({ok, "07"}, Lookup([<<"datetime">>, <<"utc">>, <<"hh">>])),
        ?_assertEqual(
            {ok, binary_to_list(local_year_bin())},
            Lookup([<<"datetime">>, <<"local">>, <<"YYYY">>])
        ),
        ?_assertEqual(
            {ok, binary_to_list(local_hour_bin())},
            Lookup([<<"datetime">>, <<"local">>, <<"hh">>])
        ),
        ?_assertEqual({ok, "2025"}, Lookup([<<"datetime_until">>, <<"utc">>, <<"YYYY">>])),
        ?_assertEqual(
            {ok, binary_to_list(local_year_bin())},
            Lookup([<<"datetime_until">>, <<"local">>, <<"YYYY">>])
        ),
        ?_assertEqual({error, undefined}, Lookup([<<"datetime">>, <<"berlin">>, <<"YYYY">>])),
        ?_assertEqual({error, undefined}, Lookup([<<"datetime">>, <<"utc">>, <<"bogus">>]))
    ].
