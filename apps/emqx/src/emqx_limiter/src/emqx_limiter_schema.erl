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

-export([
    roots/0,
    fields/1,
    to_rate/1,
    to_capacity/1,
    minimum_period/0,
    to_burst_rate/1,
    to_initial/1,
    namespace/0,
    get_bucket_cfg_path/2,
    desc/1
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
-type capacity() :: infinity | number().
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

-import(emqx_schema, [sc/2, map/2]).
-define(UNIT_TIME_IN_MS, 1000).

namespace() -> limiter.

roots() -> [limiter].

fields(limiter) ->
    [
        {bytes_in,
            sc(
                ref(limiter_opts),
                #{
                    description =>
                        <<
                            "The bytes_in limiter.<br>"
                            "It is used to limit the inbound bytes rate for this EMQX node."
                            "If the this limiter limit is reached,"
                            "the restricted client will be slow down even be hung for a while."
                        >>
                }
            )},
        {message_in,
            sc(
                ref(limiter_opts),
                #{
                    description =>
                        <<
                            "The message_in limiter.<br>"
                            "This is used to limit the inbound message numbers for this EMQX node"
                            "If the this limiter limit is reached,"
                            "the restricted client will be slow down even be hung for a while."
                        >>
                }
            )},
        {connection,
            sc(
                ref(limiter_opts),
                #{
                    description =>
                        <<
                            "The connection limiter.<br>"
                            "This is used to limit the connection rate for this EMQX node"
                            "If the this limiter limit is reached,"
                            "New connections will be refused"
                        >>
                }
            )},
        {message_routing,
            sc(
                ref(limiter_opts),
                #{
                    description =>
                        <<
                            "The message_routing limiter.<br>"
                            "This is used to limite the deliver rate for this EMQX node"
                            "If the this limiter limit is reached,"
                            "New publish will be refused"
                        >>
                }
            )},
        {batch,
            sc(
                ref(limiter_opts),
                #{
                    description => <<
                        "The batch limiter.<br>"
                        "This is used for EMQX internal batch operation"
                        "e.g. limite the retainer's deliver rate"
                    >>
                }
            )}
    ];
fields(limiter_opts) ->
    [
        {rate, sc(rate(), #{default => "infinity", desc => "The rate"})},
        {burst,
            sc(
                burst_rate(),
                #{
                    default => "0/0s",
                    desc =>
                        "The burst, This value is based on rate.<br/>\n"
                        " This value + rate = the maximum limit that can be achieved when limiter burst."
                }
            )},
        {bucket, sc(map("bucket name", ref(bucket_opts)), #{desc => "Buckets config"})}
    ];
fields(bucket_opts) ->
    [
        {rate, sc(rate(), #{desc => "Rate for this bucket."})},
        {capacity, sc(capacity(), #{desc => "The maximum number of tokens for this bucket."})},
        {initial,
            sc(initial(), #{
                default => "0",
                desc => "The initial number of tokens for this bucket."
            })},
        {per_client,
            sc(
                ref(client_bucket),
                #{
                    default => #{},
                    desc =>
                        "The rate limit for each user of the bucket,"
                        " this field is not required"
                }
            )}
    ];
fields(client_bucket) ->
    [
        {rate, sc(rate(), #{default => "infinity", desc => "Rate for this bucket."})},
        {initial,
            sc(initial(), #{default => "0", desc => "The initial number of tokens for this bucket."})},
        %% low_water_mark add for emqx_channel and emqx_session
        %% both modules consume first and then check
        %% so we need to use this value to prevent excessive consumption
        %% (e.g, consumption from an empty bucket)
        {low_water_mark,
            sc(
                initial(),
                #{
                    desc =>
                        "If the remaining tokens are lower than this value,\n"
                        "the check/consume will succeed, but it will be forced to wait for a short period of time.",
                    default => "0"
                }
            )},
        {capacity,
            sc(capacity(), #{
                desc => "The capacity of the token bucket.",
                default => "infinity"
            })},
        {divisible,
            sc(
                boolean(),
                #{
                    desc => "Is it possible to split the number of requested tokens?",
                    default => false
                }
            )},
        {max_retry_time,
            sc(
                emqx_schema:duration(),
                #{
                    desc => "The maximum retry time when acquire failed.",
                    default => "10s"
                }
            )},
        {failure_strategy,
            sc(
                failure_strategy(),
                #{
                    desc => "The strategy when all the retries failed.",
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

%% minimum period is 100ms
minimum_period() ->
    100.

to_rate(Str) ->
    to_rate(Str, true, false).

-spec get_bucket_cfg_path(limiter_type(), bucket_name()) -> bucket_path().
get_bucket_cfg_path(Type, BucketName) ->
    [limiter, Type, bucket, BucketName].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
ref(Field) -> hoconsc:ref(?MODULE, Field).

to_burst_rate(Str) ->
    to_rate(Str, false, true).

to_rate(Str, CanInfinity, CanZero) ->
    Tokens = [string:trim(T) || T <- string:tokens(Str, "/")],
    case Tokens of
        ["infinity"] when CanInfinity ->
            {ok, infinity};
        %% if time unit is 1s, it can be omitted
        [QuotaStr] ->
            {ok, Val} = to_capacity(QuotaStr),
            check_capacity(
                Str,
                Val,
                CanZero,
                fun(Quota) ->
                    {ok, Quota * minimum_period() / ?UNIT_TIME_IN_MS}
                end
            );
        [QuotaStr, Interval] ->
            {ok, Val} = to_capacity(QuotaStr),
            check_capacity(
                Str,
                Val,
                CanZero,
                fun(Quota) ->
                    case emqx_schema:to_duration_ms(Interval) of
                        {ok, Ms} when Ms > 0 ->
                            {ok, Quota * minimum_period() / Ms};
                        _ ->
                            {error, Str}
                    end
                end
            );
        _ ->
            {error, Str}
    end.

check_capacity(_Str, 0, true, _Cont) ->
    {ok, 0};
check_capacity(Str, 0, false, _Cont) ->
    {error, Str};
check_capacity(_Str, Quota, _CanZero, Cont) ->
    Cont(Quota).

to_capacity(Str) ->
    Regex = "^\s*(?:([0-9]+)([a-zA-z]*))|infinity\s*$",
    to_quota(Str, Regex).

to_initial(Str) ->
    Regex = "^\s*([0-9]+)([a-zA-z]*)\s*$",
    to_quota(Str, Regex).

to_quota(Str, Regex) ->
    {ok, MP} = re:compile(Regex),
    Result = re:run(Str, MP, [{capture, all_but_first, list}]),
    case Result of
        {match, [Quota, Unit]} ->
            Val = erlang:list_to_integer(Quota),
            Unit2 = string:to_lower(Unit),
            {ok, apply_unit(Unit2, Val)};
        {match, [Quota, ""]} ->
            {ok, erlang:list_to_integer(Quota)};
        {match, ""} ->
            {ok, infinity};
        _ ->
            {error, Str}
    end.

apply_unit("", Val) -> Val;
apply_unit("kb", Val) -> Val * ?KILOBYTE;
apply_unit("mb", Val) -> Val * ?KILOBYTE * ?KILOBYTE;
apply_unit("gb", Val) -> Val * ?KILOBYTE * ?KILOBYTE * ?KILOBYTE;
apply_unit(Unit, _) -> throw("invalid unit:" ++ Unit).
