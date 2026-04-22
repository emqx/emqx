%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggreg_buffer_ctx_tests).

-include_lib("eunit/include/eunit.hrl").

%% 2025-04-22T07:52:03Z — day-of-year 112.
-define(TS, 1745308323).

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
