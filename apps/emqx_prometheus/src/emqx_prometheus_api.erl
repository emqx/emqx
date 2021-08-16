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

-import(emqx_mgmt_util, [ response_schema/1
                        , response_schema/2
                        , request_body_schema/1
                        ]).

-export([api_spec/0]).

-export([ prometheus/2
        % , stats/2
        ]).

api_spec() ->
    {[prometheus_api()], schemas()}.

schemas() ->
    [#{prometheus => #{
        type => object,
        properties => #{
            push_gateway_server => #{
                type => string,
                description => <<"prometheus PushGateway Server">>,
                example => get_raw(<<"push_gateway_server">>, <<"http://127.0.0.1:9091">>)},
            interval => #{
                type => string,
                description => <<"Interval">>,
                example => get_raw(<<"interval">>, <<"15s">>)},
            enable => #{
                type => boolean,
                description => <<"Prometheus status">>,
                example => get_raw(<<"enable">>, false)}
        }
    }}].

prometheus_api() ->
    Metadata = #{
        get => #{
            description => <<"Get Prometheus info">>,
            responses => #{
                <<"200">> => response_schema(prometheus)
            }
        },
        put => #{
            description => <<"Update Prometheus">>,
            'requestBody' => request_body_schema(prometheus),
            responses => #{
                <<"200">> =>
                    response_schema(<<"Update Prometheus successfully">>),
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
    Response = emqx_config:get_raw([<<"prometheus">>], #{}),
    {200, Response};

prometheus(put, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    Enable = maps:get(<<"enable">>, Params),
    {ok, _, _} = emqx_config:update([prometheus], Params),
    enable_prometheus(Enable).

% stats(_Bindings, Params) ->
%     Type = proplists:get_value(<<"format_type">>, Params, <<"json">>),
%     Data = emqx_prometheus:collect(Type),
%     case Type of
%         <<"json">> ->
%             {ok, Data};
%         <<"prometheus">> ->
%             {ok, #{<<"content-type">> => <<"text/plain">>}, Data}
%     end.

enable_prometheus(true) ->
    ok = emqx_prometheus_sup:stop_child(?APP),
    emqx_prometheus_sup:start_child(?APP, emqx_config:get([prometheus], #{})),
    {200};
enable_prometheus(false) ->
    _ = emqx_prometheus_sup:stop_child(?APP),
    {200}.

get_raw(Key, Def) ->
    emqx_config:get_raw([<<"prometheus">>] ++ [Key], Def).
