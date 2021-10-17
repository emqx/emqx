%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_schema).

-include_lib("typerefl/include/types.hrl").

-export([ roots/0, fields/1, to_rate/1
        , to_bucket_rate/1, minimum_period/0]).

-define(KILOBYTE, 1024).

-type limiter_type() :: bytes_in
                      | message_in
                      | connection
                      | message_routing.

-type bucket_name() :: atom().
-type zone_name() :: atom().
-type rate() :: infinity | float().
-type bucket_rate() :: list(infinity | number()).

-typerefl_from_string({rate/0, ?MODULE, to_rate}).
-typerefl_from_string({bucket_rate/0, ?MODULE, to_bucket_rate}).

-reflect_type([ rate/0
              , bucket_rate/0
              ]).

-export_type([limiter_type/0, bucket_name/0, zone_name/0]).

-import(emqx_schema, [sc/2, map/2]).

roots() -> [emqx_limiter].

fields(emqx_limiter) ->
    [ {bytes_in, sc(ref(limiter), #{})}
    , {message_in, sc(ref(limiter), #{})}
    , {connection, sc(ref(limiter), #{})}
    , {message_routing, sc(ref(limiter), #{})}
    ];

fields(limiter) ->
    [ {global, sc(rate(), #{})}
    , {zone, sc(map("zone name", rate()), #{})}
    , {bucket, sc(map("bucket id", ref(bucket)),
                  #{desc => "Token Buckets"})}
    ];

fields(bucket) ->
    [ {zone, sc(atom(), #{desc => "the zone which the bucket in"})}
    , {aggregated, sc(bucket_rate(), #{})}
    , {per_client, sc(bucket_rate(), #{})}
    ].

%% minimum period is 100ms
minimum_period() ->
    100.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
ref(Field) -> hoconsc:ref(?MODULE, Field).

to_rate(Str) ->
    Tokens = [string:trim(T) || T <- string:tokens(Str, "/")],
    case Tokens of
        ["infinity"] ->
            {ok, infinity};
        [Quota, Interval] ->
            {ok, Val} = to_quota(Quota),
            case emqx_schema:to_duration_ms(Interval) of
                {ok, Ms} when Ms > 0 ->
                    {ok, Val * minimum_period() / Ms};
                _ ->
                    {error, Str}
            end;
        _ ->
            {error, Str}
    end.

to_bucket_rate(Str) ->
    Tokens = [string:trim(T) || T <- string:tokens(Str, "/,")],
    case Tokens of
        [Rate, Capa] ->
            {ok, infinity} = to_quota(Rate),
            {ok, CapaVal} = to_quota(Capa),
            if CapaVal =/= infinity ->
                    {ok, [infinity, CapaVal]};
               true ->
                    {error, Str}
            end;
        [Quota, Interval, Capacity] ->
            {ok, Val} = to_quota(Quota),
            case emqx_schema:to_duration_ms(Interval) of
                {ok, Ms} when Ms > 0 ->
                    {ok, CapaVal} = to_quota(Capacity),
                    {ok, [Val * minimum_period() / Ms, CapaVal]};
                _ ->
                    {error, Str}
            end;
        _ ->
            {error, Str}
    end.


to_quota(Str) ->
    {ok, MP} = re:compile("^\s*(?:(?:([1-9][0-9]*)([a-zA-z]*))|infinity)\s*$"),
    Result = re:run(Str, MP, [{capture, all_but_first, list}]),
    case Result of
        {match, [Quota, Unit]} ->
            Val = erlang:list_to_integer(Quota),
            Unit2 = string:to_lower(Unit),
            {ok, apply_unit(Unit2, Val)};
        {match, [Quota]} ->
            {ok, erlang:list_to_integer(Quota)};
        {match, []} ->
            {ok, infinity};
        _ ->
            {error, Str}
    end.

apply_unit("", Val) -> Val;
apply_unit("kb", Val) -> Val * ?KILOBYTE;
apply_unit("mb", Val) -> Val * ?KILOBYTE * ?KILOBYTE;
apply_unit("gb", Val) -> Val * ?KILOBYTE * ?KILOBYTE * ?KILOBYTE;
apply_unit(Unit, _) -> throw("invalid unit:" ++ Unit).
