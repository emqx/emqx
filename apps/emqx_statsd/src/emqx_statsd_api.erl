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

-import(emqx_mgmt_util, [ response_schema/1
                        , response_schema/2
                        , request_body_schema/1
                        ]).

-export([api_spec/0]).

-export([ statsd/2
        ]).

api_spec() ->
    {statsd_api(), schemas()}.

schemas() ->
    [#{statsd => #{
        type => object,
        properties => #{
            server => #{
                type => string,
                description => <<"Statsd Server">>,
                example => get_raw(<<"server">>, <<"127.0.0.1:8125">>)},
            enable => #{
                type => boolean,
                description => <<"Statsd status">>,
                example => get_raw(<<"enable">>, false)},
            sample_time_interval => #{
                type => string,
                description => <<"Sample Time Interval">>,
                example => get_raw(<<"sample_time_interval">>, <<"10s">>)},
            flush_time_interval => #{
                type => string,
                description => <<"Flush Time Interval">>,
                example => get_raw(<<"flush_time_interval">>, <<"10s">>)}
        }
    }}].

statsd_api() ->
    Metadata = #{
        get => #{
            description => <<"Get statsd info">>,
            responses => #{
                <<"200">> => response_schema(<<"statsd">>)
            }
        },
        put => #{
            description => <<"Update Statsd">>,
            'requestBody' => request_body_schema(<<"statsd">>),
            responses => #{
                <<"200">> =>
                    response_schema(<<"Update Statsd successfully">>),
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
    Response = emqx_config:get_raw([<<"statsd">>], #{}),
    {200, Response};

statsd(put, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    Enable = maps:get(<<"enable">>, Params),
    {ok, _} = emqx:update_config([statsd], Params),
    enable_statsd(Enable).

enable_statsd(true) ->
    ok = emqx_statsd_sup:stop_child(?APP),
    emqx_statsd_sup:start_child(?APP, emqx_config:get([statsd], #{})),
    {200};
enable_statsd(false) ->
    _ = emqx_statsd_sup:stop_child(?APP),
    {200}.

get_raw(Key, Def) ->
    emqx_config:get_raw([<<"statsd">>]++ [Key], Def).
