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
lookup([<<"datetime">>, Zone, Token], #buffer{since = Since}) ->
    lookup_part(Since, Zone, Token);
lookup([<<"datetime_until">>, Format], #buffer{until = Until}) ->
    {ok, format_timestamp(Until, Format)};
lookup([<<"datetime_until">>, Zone, Token], #buffer{until = Until}) ->
    lookup_part(Until, Zone, Token);
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
    %% Without a zone suffix, date/time parts default to UTC.
    format_part(Timestamp, utc, Token).

-spec is_valid_datetime_format(binary()) -> boolean().
is_valid_datetime_format(<<"rfc3339">>) -> true;
is_valid_datetime_format(<<"rfc3339utc">>) -> true;
is_valid_datetime_format(<<"unix">>) -> true;
is_valid_datetime_format(<<"utc.", Token/binary>>) -> is_valid_token(Token);
is_valid_datetime_format(<<"local.", Token/binary>>) -> is_valid_token(Token);
is_valid_datetime_format(Bin) -> is_valid_token(Bin).

is_valid_token(<<"YYYY">>) -> true;
is_valid_token(<<"MM">>) -> true;
is_valid_token(<<"DD">>) -> true;
is_valid_token(<<"hh">>) -> true;
is_valid_token(<<"mm">>) -> true;
is_valid_token(<<"ss">>) -> true;
is_valid_token(<<"DOY">>) -> true;
is_valid_token(_) -> false.

lookup_part(Timestamp, Zone, Token) ->
    case zone_atom(Zone) of
        {ok, ZoneAtom} ->
            case is_valid_token(Token) of
                true -> {ok, format_part(Timestamp, ZoneAtom, Token)};
                false -> {error, undefined}
            end;
        error ->
            {error, undefined}
    end.

zone_atom(<<"utc">>) -> {ok, utc};
zone_atom(<<"local">>) -> {ok, local};
zone_atom(_) -> error.

format_part(Timestamp, utc, Token) ->
    render_token(calendar:system_time_to_universal_time(Timestamp, second), Token);
format_part(Timestamp, local, Token) ->
    UT = calendar:system_time_to_universal_time(Timestamp, second),
    render_token(calendar:universal_time_to_local_time(UT), Token).

render_token({{Y, _, _}, {_, _, _}}, <<"YYYY">>) ->
    pad4(Y);
render_token({{_, Mo, _}, {_, _, _}}, <<"MM">>) ->
    pad2(Mo);
render_token({{_, _, D}, {_, _, _}}, <<"DD">>) ->
    pad2(D);
render_token({{_, _, _}, {H, _, _}}, <<"hh">>) ->
    pad2(H);
render_token({{_, _, _}, {_, Mi, _}}, <<"mm">>) ->
    pad2(Mi);
render_token({{_, _, _}, {_, _, S}}, <<"ss">>) ->
    pad2(S);
render_token({{Y, Mo, D}, {_, _, _}}, <<"DOY">>) ->
    pad3(
        calendar:date_to_gregorian_days(Y, Mo, D) - calendar:date_to_gregorian_days(Y, 1, 1) + 1
    ).

pad2(N) -> lists:flatten(io_lib:format("~2..0B", [N])).
pad3(N) -> lists:flatten(io_lib:format("~3..0B", [N])).
pad4(N) -> lists:flatten(io_lib:format("~4..0B", [N])).
