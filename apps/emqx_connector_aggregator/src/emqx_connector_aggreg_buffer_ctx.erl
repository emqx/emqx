%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggreg_buffer_ctx).

-behaviour(emqx_template).

-include("emqx_connector_aggregator.hrl").

%% `emqx_template' API
-export([lookup/2]).

%% API
-export([sequence/1, datetime/2, format_timestamp/2, is_valid_datetime_format/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

sequence(#buffer{seq = Seq}) ->
    Seq.

datetime(#buffer{since = Since}, Format) ->
    format_timestamp(Since, Format).

%%------------------------------------------------------------------------------
%% `emqx_template' API
%%------------------------------------------------------------------------------

-spec lookup(emqx_template:accessor(), buffer()) ->
    {ok, integer() | string()} | {error, undefined}.
lookup([<<"datetime">>, Format], #buffer{since = Since}) ->
    {ok, format_timestamp(Since, Format)};
lookup([<<"datetime_until">>, Format], #buffer{until = Until}) ->
    {ok, format_timestamp(Until, Format)};
lookup([<<"sequence">>], #buffer{seq = Seq}) ->
    {ok, Seq};
lookup(_Binding, _Context) ->
    {error, undefined}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

format_timestamp(Timestamp, <<"rfc3339utc">>) ->
    calendar:system_time_to_rfc3339(Timestamp, [{unit, second}, {offset, "Z"}]);
format_timestamp(Timestamp, <<"rfc3339">>) ->
    calendar:system_time_to_rfc3339(Timestamp, [{unit, second}]);
format_timestamp(Timestamp, <<"unix">>) ->
    Timestamp;
format_timestamp(Timestamp, Token) when is_binary(Token) ->
    %% Individual date/time parts are rendered against UTC so that
    %% object names stay stable across cluster nodes in different
    %% timezones (Hive-style partitioning).
    {{Y, Mo, D}, {H, Mi, S}} = calendar:system_time_to_universal_time(Timestamp, second),
    case Token of
        <<"YYYY">> ->
            pad4(Y);
        <<"MM">> ->
            pad2(Mo);
        <<"DD">> ->
            pad2(D);
        <<"hh">> ->
            pad2(H);
        <<"mm">> ->
            pad2(Mi);
        <<"ss">> ->
            pad2(S);
        <<"DOY">> ->
            pad3(
                calendar:date_to_gregorian_days(Y, Mo, D) - calendar:date_to_gregorian_days(Y, 1, 1) +
                    1
            )
    end.

-spec is_valid_datetime_format(binary()) -> boolean().
is_valid_datetime_format(<<"rfc3339">>) -> true;
is_valid_datetime_format(<<"rfc3339utc">>) -> true;
is_valid_datetime_format(<<"unix">>) -> true;
is_valid_datetime_format(<<"YYYY">>) -> true;
is_valid_datetime_format(<<"MM">>) -> true;
is_valid_datetime_format(<<"DD">>) -> true;
is_valid_datetime_format(<<"hh">>) -> true;
is_valid_datetime_format(<<"mm">>) -> true;
is_valid_datetime_format(<<"ss">>) -> true;
is_valid_datetime_format(<<"DOY">>) -> true;
is_valid_datetime_format(_) -> false.

pad2(N) -> lists:flatten(io_lib:format("~2..0B", [N])).
pad3(N) -> lists:flatten(io_lib:format("~3..0B", [N])).
pad4(N) -> lists:flatten(io_lib:format("~4..0B", [N])).
