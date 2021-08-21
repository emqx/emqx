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

-module(emqx_statsd_api).

-behaviour(minirest_api).

-include("emqx_statsd.hrl").

-import(emqx_mgmt_util, [response_schema/2, request_body_schema/1]).

-export([api_spec/0]).

-export([ statsd/2
        ]).

api_spec() ->
    {statsd_api(), schemas()}.

schemas() ->
    [#{statsd => emqx_mgmt_api_configs:gen_schema(emqx:get_raw_config([statsd]))}].

statsd_api() ->
    Metadata = #{
        get => #{
            description => <<"Get statsd info">>,
            responses => #{
                <<"200">> => response_schema(<<>>, statsd)
            }
        },
        put => #{
            description => <<"Update Statsd">>,
            'requestBody' => request_body_schema(statsd),
            responses => #{
                <<"200">> =>
                    response_schema(<<>>, statsd),
                <<"400">> =>
                    response_schema(<<"Bad Request">>, #{
                        type => object,
                        properties => #{
                            message => #{type => string},
                            code => #{type => string}
                        }
                    })
            }
        }
    },
    [{"/statsd", Metadata, statsd}].

statsd(get, _Request) ->
    {200, emqx:get_raw_config([<<"statsd">>], #{})};

statsd(put, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    {ok, Config} = emqx:update_config([statsd], Params),
    case maps:get(<<"enable">>, Params) of
        true ->
            _ = emqx_statsd_sup:stop_child(?APP),
            emqx_statsd_sup:start_child(?APP, maps:get(config, Config));
        false ->
            _ = emqx_statsd_sup:stop_child(?APP),
            ok
    end,
    {200, emqx:get_raw_config([<<"statsd">>], #{})}.
