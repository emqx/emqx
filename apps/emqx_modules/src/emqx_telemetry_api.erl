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

-module(emqx_telemetry_api).

-behaviour(minirest_api).

-import(emqx_mgmt_util, [ schema/1
                        , object_schema/1
                        , object_schema/2
                        , properties/1
                        , bad_request/0
                        ]).

% -export([cli/1]).

-export([ status/2
        , data/2
        ]).

-export([enable_telemetry/2]).

-export([api_spec/0]).

api_spec() ->
    {[status_api(), data_api()], []}.

properties() ->
    properties([
        {emqx_version, string, <<"EMQ X Version">>},
        {license, object, [{edition, string, <<"EMQ X License">>}]},
        {os_name, string, <<"OS Name">>},
        {os_version, string, <<"OS Version">>},
        {otp_version, string, <<"Erlang/OTP Version">>},
        {up_time, string, <<"EMQ X Runtime">>},
        {uuid, string, <<"EMQ X UUID">>},
        {nodes_uuid, string, <<"EMQ X Cluster Nodes UUID">>},
        {active_plugins, {array, string}, <<"EMQ X Active Plugins">>},
        {active_modules, {array, string}, <<"EMQ X Active Modules">>},
        {num_clients, integer, <<"EMQ X Current Connections">>},
        {messages_received, integer, <<"EMQ X Current Received Message">>},
        {messages_sent, integer, <<"EMQ X Current Sent Message">>}
    ]).

status_api() ->
    Props = properties([{enable, boolean}]),
    Metadata = #{
        get => #{
            description => "Get telemetry status",
            responses => #{<<"200">> => object_schema(Props)}
        },
        put => #{
            description => "Enable or disable telemetry",
            'requestBody' => object_schema(Props),
            responses => #{
                <<"200">> =>
                    object_schema(properties([{enable, boolean, <<"">>}]),
                        <<"Enable or disable telemetry successfully">>),
                <<"400">> => bad_request()
            }
        }
    },
    {"/telemetry/status", Metadata, status}.

data_api() ->
    Metadata = #{
        get => #{
            responses => #{
                <<"200">> => object_schema(properties(), <<"Get telemetry data">>)}}},
    {"/telemetry/data", Metadata, data}.

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------
status(get, _Params) ->
    {200, get_telemetry_status()};

status(put, #{body := Body}) ->
    Enable = maps:get(<<"enable">>, Body),
    case Enable =:= emqx_telemetry:get_status() of
        true ->
            Reason = case Enable of
                true -> <<"Telemetry status is already enabled">>;
                false -> <<"Telemetry status is already disable">>
            end,
            {400, #{code => 'BAD_REQUEST', message => Reason}};
        false ->
            enable_telemetry(Enable),
            {200, #{<<"enable">> => emqx_telemetry:get_status()}}
    end.

data(get, _Request) ->
    {200, emqx_json:encode(get_telemetry_data())}.
%%--------------------------------------------------------------------
%% CLI
%%--------------------------------------------------------------------
% cli(["enable", Enable0]) ->
%     Enable = list_to_atom(Enable0),
%     case Enable =:= emqx_telemetry:is_enabled() of
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
%             emqx_ctl:print("~ts~n", [Bin]);
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
    end, mria_mnesia:running_nodes()).

enable_telemetry(Node, Enable) when Node =:= node() ->
    case Enable of
        true ->
            emqx_telemetry:enable();
        false ->
            emqx_telemetry:disable()
    end;
enable_telemetry(Node, Enable) ->
    rpc_call(Node, ?MODULE, enable_telemetry, [Node, Enable]).

get_telemetry_status() ->
    #{enabled => emqx_telemetry:get_status()}.

get_telemetry_data() ->
    {ok, TelemetryData} = emqx_telemetry:get_telemetry(),
    TelemetryData.

rpc_call(Node, Module, Fun, Args) ->
    case rpc:call(Node, Module, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Result -> Result
    end.
