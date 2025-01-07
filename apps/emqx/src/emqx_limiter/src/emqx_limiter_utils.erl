%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_limiter_utils).

-export([
    calc_capacity/1,
    extract_with_type/2,
    get_listener_opts/1,
    get_node_opts/1,
    convert_node_opts/1,
    default_client_config/0,
    default_bucket_config/0
]).

-import(emqx_limiter_schema, [default_period/0, short_paths/0]).

%%--------------------------------------------------------------------
%% Configuration-related runtime code
%%--------------------------------------------------------------------

calc_capacity(#{rate := infinity}) ->
    infinity;
calc_capacity(#{rate := Rate, burst := Burst}) ->
    erlang:floor(1000 * Rate / default_period()) + Burst.

%% @doc extract data of a type from the nested config
extract_with_type(_Type, undefined) ->
    undefined;
extract_with_type(Type, #{client := ClientCfg} = BucketCfg) ->
    BucketVal = maps:find(Type, BucketCfg),
    ClientVal = maps:find(Type, ClientCfg),
    merge_client_bucket(Type, ClientVal, BucketVal);
extract_with_type(Type, BucketCfg) ->
    BucketVal = maps:find(Type, BucketCfg),
    merge_client_bucket(Type, undefined, BucketVal).

%% @doc get the limiter configuration from the listener setting
%% and compatible with the old version limiter schema
get_listener_opts(Conf) ->
    Limiter = maps:get(limiter, Conf, undefined),
    ShortPaths = maps:with(short_paths(), Conf),
    get_listener_opts(Limiter, ShortPaths).

get_listener_opts(Limiter, ShortPaths) when map_size(ShortPaths) =:= 0 ->
    Limiter;
get_listener_opts(undefined, ShortPaths) ->
    convert_listener_short_paths(ShortPaths);
get_listener_opts(Limiter, ShortPaths) ->
    Shorts = convert_listener_short_paths(ShortPaths),
    emqx_utils_maps:deep_merge(Limiter, Shorts).

convert_listener_short_paths(ShortPaths) ->
    DefBucket = default_bucket_config(),
    DefClient = default_client_config(),
    Fun = fun(Name, Rate, Acc) ->
        Type = short_path_name_to_type(Name),
        case Name of
            max_conn_rate ->
                Acc#{Type => DefBucket#{rate => Rate}};
            _ ->
                Client = maps:get(client, Acc, #{}),
                Acc#{client => Client#{Type => DefClient#{rate => Rate}}}
        end
    end,
    maps:fold(Fun, #{}, ShortPaths).

%% @doc get the node-level limiter configuration and compatible with the old version limiter schema
get_node_opts(Type) ->
    Opts = emqx:get_config([limiter, Type], default_bucket_config()),
    case type_to_short_path_name(Type) of
        undefined ->
            Opts;
        Name ->
            case emqx:get_config([limiter, Name], undefined) of
                undefined ->
                    Opts;
                Rate ->
                    Opts#{rate := Rate}
            end
    end.

convert_node_opts(Conf) ->
    DefBucket = default_bucket_config(),
    ShorPaths = short_paths(),
    Fun = fun
        %% The `client` in the node options was deprecated
        (client, _Value, Acc) ->
            Acc;
        (Name, Value, Acc) ->
            case lists:member(Name, ShorPaths) of
                true ->
                    Type = short_path_name_to_type(Name),
                    Acc#{Type => DefBucket#{rate => Value}};
                _ ->
                    Acc#{Name => Value}
            end
    end,
    maps:fold(Fun, #{}, Conf).

merge_client_bucket(Type, {ok, ClientVal}, {ok, BucketVal}) ->
    #{Type => BucketVal, client => #{Type => ClientVal}};
merge_client_bucket(Type, {ok, ClientVal}, _) ->
    #{client => #{Type => ClientVal}};
merge_client_bucket(Type, _, {ok, BucketVal}) ->
    #{Type => BucketVal};
merge_client_bucket(_, _, _) ->
    undefined.

short_path_name_to_type(max_conn_rate) ->
    connection;
short_path_name_to_type(messages_rate) ->
    messages;
short_path_name_to_type(bytes_rate) ->
    bytes.

type_to_short_path_name(connection) ->
    max_conn_rate;
type_to_short_path_name(messages) ->
    messages_rate;
type_to_short_path_name(bytes) ->
    bytes_rate;
type_to_short_path_name(_) ->
    undefined.

%% Since the client configuration can be absent and be a undefined value,
%% but we must need some basic settings to control the behaviour of the limiter,
%% so here add this helper function to generate a default setting.
%% This is a temporary workaround until we found a better way to simplify.
default_client_config() ->
    #{
        rate => infinity,
        initial => 0,
        low_watermark => 0,
        burst => 0,
        divisible => true,
        max_retry_time => timer:hours(1),
        failure_strategy => force
    }.

default_bucket_config() ->
    #{
        rate => infinity,
        burst => 0,
        initial => 0
    }.
