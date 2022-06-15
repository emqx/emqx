%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("hocon/include/hoconsc.hrl").

-export([
    roots/0,
    fields/1,
    to_rate/1,
    to_capacity/1,
    default_period/0,
    to_burst_rate/1,
    to_initial/1,
    namespace/0,
    get_bucket_cfg_path/2,
    desc/1,
    types/0,
    infinity_value/0
]).

-define(KILOBYTE, 1024).

-type limiter_type() ::
    bytes_in
    | message_in
    | connection
    | message_routing
    | batch.

-type bucket_name() :: atom().
-type rate() :: infinity | float().
-type burst_rate() :: 0 | float().
%% the capacity of the token bucket
-type capacity() :: non_neg_integer().
%% initial capacity of the token bucket
-type initial() :: non_neg_integer().
-type bucket_path() :: list(atom()).

%% the processing strategy after the failure of the token request

%% Forced to pass
-type failure_strategy() ::
    force
    %% discard the current request
    | drop
    %% throw an exception
    | throw.

-typerefl_from_string({rate/0, ?MODULE, to_rate}).
-typerefl_from_string({burst_rate/0, ?MODULE, to_burst_rate}).
-typerefl_from_string({capacity/0, ?MODULE, to_capacity}).
-typerefl_from_string({initial/0, ?MODULE, to_initial}).

-reflect_type([
    rate/0,
    burst_rate/0,
    capacity/0,
    initial/0,
    failure_strategy/0,
    bucket_name/0
]).

-export_type([limiter_type/0, bucket_path/0]).

-define(UNIT_TIME_IN_MS, 1000).

namespace() -> limiter.

roots() -> [limiter].

fields(limiter) ->
    [
        {Type,
            ?HOCON(?R_REF(limiter_opts), #{
                desc => ?DESC(Type),
                default => make_limiter_default(Type)
            })}
     || Type <- types()
    ];
fields(limiter_opts) ->
    [
        {rate, ?HOCON(rate(), #{desc => ?DESC(rate), default => "infinity"})},
        {burst,
            ?HOCON(burst_rate(), #{
                desc => ?DESC(burst),
                default => 0
            })},
        {bucket,
            ?HOCON(
                ?MAP("bucket_name", ?R_REF(bucket_opts)),
                #{
                    desc => ?DESC(bucket_cfg),
                    default => #{<<"default">> => #{}},
                    example => #{
                        <<"mybucket-name">> => #{
                            <<"rate">> => <<"infinity">>,
                            <<"capcity">> => <<"infinity">>,
                            <<"initial">> => <<"100">>,
                            <<"per_client">> => #{<<"rate">> => <<"infinity">>}
                        }
                    }
                }
            )}
    ];
fields(bucket_opts) ->
    [
        {rate, ?HOCON(rate(), #{desc => ?DESC(rate), default => "infinity"})},
        {capacity, ?HOCON(capacity(), #{desc => ?DESC(capacity), default => "infinity"})},
        {initial, ?HOCON(initial(), #{default => "0", desc => ?DESC(initial)})},
        {per_client,
            ?HOCON(
                ?R_REF(client_bucket),
                #{
                    default => #{},
                    desc => ?DESC(per_client)
                }
            )}
    ];
fields(client_bucket) ->
    [
        {rate, ?HOCON(rate(), #{default => "infinity", desc => ?DESC(rate)})},
        {initial, ?HOCON(initial(), #{default => "0", desc => ?DESC(initial)})},
        %% low_watermark add for emqx_channel and emqx_session
        %% both modules consume first and then check
        %% so we need to use this value to prevent excessive consumption
        %% (e.g, consumption from an empty bucket)
        {low_watermark,
            ?HOCON(
                initial(),
                #{
                    desc => ?DESC(low_watermark),
                    default => "0"
                }
            )},
        {capacity,
            ?HOCON(capacity(), #{
                desc => ?DESC(client_bucket_capacity),
                default => "infinity"
            })},
        {divisible,
            ?HOCON(
                boolean(),
                #{
                    desc => ?DESC(divisible),
                    default => false
                }
            )},
        {max_retry_time,
            ?HOCON(
                emqx_schema:duration(),
                #{
                    desc => ?DESC(max_retry_time),
                    default => "10s"
                }
            )},
        {failure_strategy,
            ?HOCON(
                failure_strategy(),
                #{
                    desc => ?DESC(failure_strategy),
                    default => force
                }
            )}
    ].

desc(limiter) ->
    "Settings for the rate limiter.";
desc(limiter_opts) ->
    "Settings for the limiter.";
desc(bucket_opts) ->
    "Settings for the bucket.";
desc(client_bucket) ->
    "Settings for the client bucket.";
desc(_) ->
    undefined.

%% default period is 100ms
default_period() ->
    100.

to_rate(Str) ->
    to_rate(Str, true, false).

-spec get_bucket_cfg_path(limiter_type(), bucket_name()) -> bucket_path().
get_bucket_cfg_path(Type, BucketName) ->
    [limiter, Type, bucket, BucketName].

types() ->
    [bytes_in, message_in, connection, message_routing, batch].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% `infinity` to `infinity_value` rules:
%% 1. all infinity capacity will change to infinity_value
%% 2. if the rate of global and bucket  both are `infinity`,
%%    use `infinity_value` as bucket rate. see `emqx_limiter_server:get_counter_rate/2`
infinity_value() ->
    %% 1 TB
    1099511627776.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

to_burst_rate(Str) ->
    to_rate(Str, false, true).

%% rate can be: 10 10MB 10MB/s 10MB/2s infinity
%% e.g. the bytes_in regex tree is:
%%
%%        __ infinity
%%        |                 - xMB
%%  rate -|                 |
%%        __ ?Size(/?Time) -|            - xMB/s
%%                          |            |
%%                          - xMB/?Time -|
%%                                       - xMB/ys
to_rate(Str, CanInfinity, CanZero) ->
    Regex = "^\s*(?:([0-9]+[a-zA-Z]*)(?:/([0-9]*)([m s h d M S H D]{1,2}))?\s*$)|infinity\s*$",
    {ok, MP} = re:compile(Regex),
    case re:run(Str, MP, [{capture, all_but_first, list}]) of
        {match, []} when CanInfinity ->
            {ok, infinity};
        %% if time unit is 1s, it can be omitted
        {match, [QuotaStr]} ->
            Fun = fun(Quota) ->
                {ok, Quota * default_period() / ?UNIT_TIME_IN_MS}
            end,
            to_capacity(QuotaStr, Str, CanZero, Fun);
        {match, [QuotaStr, TimeVal, TimeUnit]} ->
            Interval =
                case TimeVal of
                    %% for xM/s
                    [] -> "1" ++ TimeUnit;
                    %% for xM/ys
                    _ -> TimeVal ++ TimeUnit
                end,
            Fun = fun(Quota) ->
                try
                    case emqx_schema:to_duration_ms(Interval) of
                        {ok, Ms} when Ms > 0 ->
                            {ok, Quota * default_period() / Ms};
                        {ok, 0} when CanZero ->
                            {ok, 0};
                        _ ->
                            {error, Str}
                    end
                catch
                    _:_ ->
                        {error, Str}
                end
            end,
            to_capacity(QuotaStr, Str, CanZero, Fun);
        _ ->
            {error, Str}
    end.

to_capacity(QuotaStr, Str, CanZero, Fun) ->
    case to_capacity(QuotaStr) of
        {ok, Val} -> check_capacity(Str, Val, CanZero, Fun);
        {error, _Error} -> {error, Str}
    end.

check_capacity(_Str, 0, true, Cont) ->
    %% must check the interval part or maybe will get incorrect config, e.g. "0/0sHello"
    Cont(0);
check_capacity(Str, 0, false, _Cont) ->
    {error, Str};
check_capacity(_Str, Quota, _CanZero, Cont) ->
    Cont(Quota).

to_capacity(Str) ->
    Regex = "^\s*(?:([0-9]+)([a-zA-Z]*))|infinity\s*$",
    to_quota(Str, Regex).

to_initial(Str) ->
    Regex = "^\s*([0-9]+)([a-zA-Z]*)\s*$",
    to_quota(Str, Regex).

to_quota(Str, Regex) ->
    {ok, MP} = re:compile(Regex),
    try
        Result = re:run(Str, MP, [{capture, all_but_first, list}]),
        case Result of
            {match, [Quota, Unit]} ->
                Val = erlang:list_to_integer(Quota),
                Unit2 = string:to_lower(Unit),
                {ok, apply_unit(Unit2, Val)};
            {match, [Quota, ""]} ->
                {ok, erlang:list_to_integer(Quota)};
            {match, ""} ->
                {ok, infinity_value()};
            _ ->
                {error, Str}
        end
    catch
        _:Error ->
            {error, Error}
    end.

apply_unit("", Val) -> Val;
apply_unit("kb", Val) -> Val * ?KILOBYTE;
apply_unit("mb", Val) -> Val * ?KILOBYTE * ?KILOBYTE;
apply_unit("gb", Val) -> Val * ?KILOBYTE * ?KILOBYTE * ?KILOBYTE;
apply_unit(Unit, _) -> throw("invalid unit:" ++ Unit).

make_limiter_default(connection) ->
    #{
        <<"rate">> => <<"1000/s">>,
        <<"bucket">> => #{
            <<"default">> =>
                #{
                    <<"rate">> => <<"1000/s">>,
                    <<"capacity">> => 1000
                }
        }
    };
make_limiter_default(_) ->
    #{}.
