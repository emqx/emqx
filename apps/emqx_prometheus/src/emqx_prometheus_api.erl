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

-module(emqx_prometheus_api).

-behaviour(minirest_api).

-include("emqx_prometheus.hrl").

-import(emqx_mgmt_util, [ response_schema/2
                        , request_body_schema/1
                        ]).

-export([api_spec/0]).

-export([ prometheus/2
        % , stats/2
        ]).

api_spec() ->
    {[prometheus_api()], schemas()}.

schemas() ->
    [#{prometheus => emqx_mgmt_api_configs:gen_schema(emqx:get_raw_config([prometheus]))}].

prometheus_api() ->
    Metadata = #{
        get => #{
            description => <<"Get Prometheus info">>,
            responses => #{
                <<"200">> => response_schema(<<>>, prometheus)
            }
        },
        put => #{
            description => <<"Update Prometheus">>,
            'requestBody' => request_body_schema(prometheus),
            responses => #{
                <<"200">> =>response_schema(<<>>, prometheus),
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
    {"/prometheus", Metadata, prometheus}.

% prometheus_data_api() ->
%     Metadata = #{
%         get => #{
%             description => <<"Get Prometheus Data">>,
%             parameters => [#{
%                 name => format_type,
%                 in => path,
%                 schema => #{type => string}
%             }],
%             responses => #{
%                 <<"200">> =>
%                     response_schema(<<"Update Prometheus successfully">>),
%                 <<"400">> =>
%                     response_schema(<<"Bad Request">>, #{
%                         type => object,
%                         properties => #{
%                             message => #{type => string},
%                             code => #{type => string}
%                         }
%                     })
%             }
%         }
%     },
%     {"/prometheus/stats", Metadata, stats}.

prometheus(get, _Request) ->
    {200, emqx:get_raw_config([<<"prometheus">>], #{})};

prometheus(put, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    {ok, Config} = emqx:update_config([prometheus], Params),
    case maps:get(<<"enable">>, Params) of
        true ->
            _ = emqx_prometheus_sup:stop_child(?APP),
            emqx_prometheus_sup:start_child(?APP, maps:get(config, Config));
        false ->
            _ = emqx_prometheus_sup:stop_child(?APP),
            ok
        end,
    {200, emqx:get_raw_config([<<"prometheus">>], #{})}.

% stats(_Bindings, Params) ->
%     Type = proplists:get_value(<<"format_type">>, Params, <<"json">>),
%     Data = emqx_prometheus:collect(Type),
%     case Type of
%         <<"json">> ->
%             {ok, Data};
%         <<"prometheus">> ->
%             {ok, #{<<"content-type">> => <<"text/plain">>}, Data}
%     end.
