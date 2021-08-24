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

-import(emqx_mgmt_util, [ schema/1
                        , bad_request/0]).

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
            responses => #{<<"200">> => schema(prometheus)}
        },
        put => #{
            description => <<"Update Prometheus">>,
            'requestBody' => schema(prometheus),
            responses => #{
                <<"200">> => schema(prometheus),
                <<"400">> => bad_request()
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

prometheus(get, _Params) ->
    {200, emqx:get_raw_config([<<"prometheus">>], #{})};

prometheus(put, #{body := Body}) ->
    {ok, Config} = emqx:update_config([prometheus], Body),
    case maps:get(<<"enable">>, Body) of
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
