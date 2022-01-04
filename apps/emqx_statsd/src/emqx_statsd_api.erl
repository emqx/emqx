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

-import(emqx_mgmt_util, [ schema/1
                        , bad_request/0]).

-export([api_spec/0]).

-export([ statsd/2
        ]).

api_spec() ->
    {statsd_api(), []}.

conf_schema() ->
    emqx_mgmt_api_configs:gen_schema(emqx:get_raw_config([statsd])).

statsd_api() ->
    Metadata = #{
        get => #{
            description => <<"Get statsd info">>,
            responses => #{<<"200">> => schema(conf_schema())}
        },
        put => #{
            description => <<"Update Statsd">>,
            'requestBody' => schema(conf_schema()),
            responses => #{
                <<"200">> => schema(conf_schema()),
                <<"400">> => bad_request()
            }
        }
    },
    [{"/statsd", Metadata, statsd}].

statsd(get, _Params) ->
    {200, emqx:get_raw_config([<<"statsd">>], #{})};

statsd(put, #{body := Body}) ->
    case emqx:update_config([statsd],
                            Body,
                            #{rawconf_with_defaults => true, override_to => cluster}) of
        {ok, #{raw_config := NewConfig, config := Config}} ->
            ok = emqx_statsd_sup:ensure_child_stopped(?APP),
            case maps:get(<<"enable">>, Body) of
                true ->
                    ok = emqx_statsd_sup:ensure_child_started(?APP, maps:get(config, Config));
                false ->
                    ok
            end,
            {200, NewConfig};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("Update config failed ~p", [Reason])),
            {500, 'INTERNAL_ERROR', Message}
    end.
