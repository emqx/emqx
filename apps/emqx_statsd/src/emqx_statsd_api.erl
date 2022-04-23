%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_statsd_api).

-behaviour(minirest_api).

-include("emqx_statsd.hrl").

-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/2]).

-export([statsd/2]).

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-define(API_TAG_STATSD, [<<"statsd">>]).
-define(SCHEMA_MODULE, emqx_statsd_schema).

-define(INTERNAL_ERROR, 'INTERNAL_ERROR').

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    ["/statsd"].

schema("/statsd") ->
    #{
        'operationId' => statsd,
        get =>
            #{
                description => <<"Get statsd config">>,
                tags => ?API_TAG_STATSD,
                responses =>
                    #{200 => statsd_config_schema()}
            },
        put =>
            #{
                description => <<"Set statsd config">>,
                tags => ?API_TAG_STATSD,
                'requestBody' => statsd_config_schema(),
                responses =>
                    #{200 => statsd_config_schema()}
            }
    }.

%%--------------------------------------------------------------------
%% Helper funcs
%%--------------------------------------------------------------------

statsd_config_schema() ->
    emqx_dashboard_swagger:schema_with_example(
        ref(?SCHEMA_MODULE, "statsd"),
        statsd_example()
    ).

statsd_example() ->
    #{
        enable => true,
        flush_time_interval => "32s",
        sample_time_interval => "32s",
        server => "127.0.0.1:8125"
    }.

statsd(get, _Params) ->
    {200, emqx:get_raw_config([<<"statsd">>], #{})};
statsd(put, #{body := Body}) ->
    case emqx_statsd:update(Body) of
        {ok, NewConfig} ->
            {200, NewConfig};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Update config failed ~p", [Reason])),
            {500, ?INTERNAL_ERROR, Message}
    end.
