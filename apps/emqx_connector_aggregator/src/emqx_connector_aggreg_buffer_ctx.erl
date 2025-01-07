%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_aggreg_buffer_ctx).

-behaviour(emqx_template).

-include("emqx_connector_aggregator.hrl").

%% `emqx_template' API
-export([lookup/2]).

%% API
-export([sequence/1, datetime/2]).

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
    Timestamp.
