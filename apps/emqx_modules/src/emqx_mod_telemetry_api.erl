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

-module(emqx_mod_telemetry_api).

-behavior(minirest_api).

-import(emqx_mgmt_util, [ response_schema/1
                        , response_schema/2
                        , request_body_schema/1
                        ]).

% -export([cli/1]).

-export([ status/2
        , data/2
        ]).

-export([enable_telemetry/2]).

-export([api_spec/0]).

api_spec() ->
    {[status_api(), data_api()], schemas()}.

schemas() ->
    [#{broker_info => #{
        type => object,
        properties => #{
            emqx_version => #{
                type => string,
                description => <<"EMQ X Version">>},
            license => #{
                type => object,
                properties => #{
                    edition => #{type => string}
                },
                description => <<"EMQ X License">>},
            os_name => #{
                type => string,
                description => <<"OS Name">>},
            os_version => #{
                type => string,
                description => <<"OS Version">>},
            otp_version => #{
                type => string,
                description => <<"Erlang/OTP Version">>},
            up_time => #{
                type => integer,
                description => <<"EMQ X Runtime">>},
            uuid => #{
                type => string,
                description => <<"EMQ X UUID">>},
            nodes_uuid => #{
                type => array,
                items => #{type => string},
                description => <<"EMQ X Cluster Nodes UUID">>},
            active_plugins => #{
                type => array,
                items => #{type => string},
                description => <<"EMQ X Active Plugins">>},
            active_modules => #{
                type => array,
                items => #{type => string},
                description => <<"EMQ X Active Modules">>},
            num_clients => #{
                type => integer,
                description => <<"EMQ X Current Connections">>},
            messages_received => #{
                type => integer,
                description => <<"EMQ X Current Received Message">>},
            messages_sent => #{
                type => integer,
                description => <<"EMQ X Current Sent Message">>}
        }
    }}].

status_api() ->
    Metadata = #{
        get => #{
            description => "Get telemetry status",
            responses => #{
                <<"200">> => response_schema(<<"Bad Request">>,
                    #{
                        type => object,
                        properties => #{enable => #{type => boolean}}
                    }
                )
            }
        },
        put => #{
            description => "Enable or disbale telemetry",
            'requestBody' => request_body_schema(#{
                type => object,
                properties => #{
                    enable => #{
                        type => boolean
                    }
                }
            }),
            responses => #{
                <<"200">> =>
                    response_schema(<<"Enable or disbale telemetry successfully">>),
                <<"400">> =>
                    response_schema(<<"Bad Request">>,
                        #{
                            type => object,
                            properties => #{
                                message => #{type => string},
                                code => #{type => string}
                            }
                        }
                    )
            }
        }
    },
    {"/telemetry/status", Metadata, status}.

data_api() ->
    Metadata = #{
        get => #{
            responses => #{
                <<"200">> => response_schema(<<"Get telemetry data">>, <<"broker_info">>)
            }
        }
    },
    {"/telemetry/data", Metadata, data}.

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------
status(get, _Request) ->
    {200, get_telemetry_status()};

status(put, Request) ->
    {ok, Body, _} = cowboy_req:read_body(Request),
    Params = emqx_json:decode(Body, [return_maps]),
    Enable = maps:get(<<"enable">>, Params),
    case Enable =:= emqx_mod_telemetry:get_status() of
        true ->
            Reason = case Enable of
                true -> <<"Telemetry status is already enabled">>;
                false -> <<"Telemetry status is already disable">>
            end,
            {400, #{code => "BAD_REQUEST", message => Reason}};
        false ->
            enable_telemetry(Enable),
             {200}
    end.

data(get, _Request) ->
    {200, emqx_json:encode(get_telemetry_data())}.
%%--------------------------------------------------------------------
%% CLI
%%--------------------------------------------------------------------
% cli(["enable", Enable0]) ->
%     Enable = list_to_atom(Enable0),
%     case Enable =:= emqx_mod_telemetry:is_enabled() of
%         true ->
%             case Enable of
%                 true -> emqx_ctl:print("Telemetry status is already enabled~n");
%                 false -> emqx_ctl:print("Telemetry status is already disable~n")
%             end;
%         false ->
%             enable_telemetry(Enable),
%             case Enable of
%                 true -> emqx_ctl:print("Enable telemetry successfully~n");
%                 false -> emqx_ctl:print("Disable telemetry successfully~n")
%             end
%     end;

% cli(["get", "status"]) ->
%     case get_telemetry_status() of
%         [{enabled, true}] ->
%             emqx_ctl:print("Telemetry is enabled~n");
%         [{enabled, false}] ->
%             emqx_ctl:print("Telemetry is disabled~n")
%     end;

% cli(["get", "data"]) ->
%     TelemetryData = get_telemetry_data(),
%     case emqx_json:safe_encode(TelemetryData, [pretty]) of
%         {ok, Bin} ->
%             emqx_ctl:print("~s~n", [Bin]);
%         {error, _Reason} ->
%             emqx_ctl:print("Failed to get telemetry data")
%     end;

% cli(_) ->
%     emqx_ctl:usage([{"telemetry enable",   "Enable telemetry"},
%                     {"telemetry disable",  "Disable telemetry"},
%                     {"telemetry get data", "Get reported telemetry data"}]).

%%--------------------------------------------------------------------
%% internal function
%%--------------------------------------------------------------------
enable_telemetry(Enable) ->
    lists:foreach(fun(Node) ->
        enable_telemetry(Node, Enable)
    end, ekka_mnesia:running_nodes()).

enable_telemetry(Node, Enable) when Node =:= node() ->
    case Enable of
        true ->
            emqx_mod_telemetry:load(#{});
        false ->
            emqx_mod_telemetry:unload(#{})
    end;
enable_telemetry(Node, Enable) ->
    rpc_call(Node, ?MODULE, enable_telemetry, [Node, Enable]).

get_telemetry_status() ->
    #{enabled => emqx_mod_telemetry:get_status()}.

get_telemetry_data() ->
    {ok, TelemetryData} = emqx_mod_telemetry:get_telemetry(),
    TelemetryData.

rpc_call(Node, Module, Fun, Args) ->
    case rpc:call(Node, Module, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Result -> Result
    end.
