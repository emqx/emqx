%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-rest_api(#{name   => enable_telemetry,
            method => 'PUT',
            path   => "/telemetry/status",
            func   => enable,
            descr  => "Enable or disbale telemetry"}).

-rest_api(#{name   => get_telemetry_status,
            method => 'GET',
            path   => "/telemetry/status",
            func   => get_status,
            descr  => "Get telemetry status"}).

-rest_api(#{name   => get_telemetry_data,
            method => 'GET',
            path   => "/telemetry/data",
            func   => get_data,
            descr  => "Get reported telemetry data"}).

-export([ cli/1
        , enable/2
        , get_status/2
        , get_data/2
        , enable_telemetry/1
        , disable_telemetry/1
        , get_telemetry_status/0
        , get_telemetry_data/0
        ]).

-import(minirest, [return/1]).

%%--------------------------------------------------------------------
%% CLI
%%--------------------------------------------------------------------

cli(["enable"]) ->
    emqx_mgmt:enable_telemetry(),
    emqx_ctl:print("Enable telemetry successfully~n");

cli(["disable"]) ->
    emqx_mgmt:disable_telemetry(),
    emqx_ctl:print("Disable telemetry successfully~n");

cli(["get", "status"]) ->
    case emqx_mgmt:get_telemetry_status() of
        [{enabled, true}] ->
            emqx_ctl:print("Telemetry is enabled~n");
        [{enabled, false}] ->
            emqx_ctl:print("Telemetry is disabled~n")
    end;

cli(["get", "data"]) ->
    {ok, TelemetryData} = emqx_mgmt:get_telemetry_data(),
    case emqx_json:safe_encode(TelemetryData, [pretty]) of
        {ok, Bin} ->
            emqx_ctl:print("~s~n", [Bin]);
        {error, _Reason} ->
            emqx_ctl:print("Failed to get telemetry data")
    end;

cli(_) ->
    emqx_ctl:usage([{"telemetry enable",   "Enable telemetry"},
                    {"telemetry disable",  "Disable telemetry"},
                    {"telemetry get data", "Get reported telemetry data"}]).

%%--------------------------------------------------------------------
%% HTTP API
%%--------------------------------------------------------------------

enable(_Bindings, Params) ->
    case proplists:get_value(<<"enabled">>, Params) of
        true ->
            enable_telemetry(),
            return(ok);
        false ->
            disable_telemetry(),
            return(ok);
        undefined ->
            return({error, missing_required_params})
    end.

get_status(_Bindings, _Params) ->
    return(get_telemetry_status()).

get_data(_Bindings, _Params) ->
    return(get_telemetry_data()).

enable_telemetry() ->
    lists:foreach(fun enable_telemetry/1, ekka_mnesia:running_nodes()).

enable_telemetry(Node) when Node =:= node() ->
    emqx_telemetry:enable();
enable_telemetry(Node) ->
    rpc_call(Node, ?MODULE, enable_telemetry, [Node]).

disable_telemetry() ->
    lists:foreach(fun disable_telemetry/1, ekka_mnesia:running_nodes()).

disable_telemetry(Node) when Node =:= node() ->
    emqx_telemetry:disable();
disable_telemetry(Node) ->
    rpc_call(Node, ?MODULE, disable_telemetry, [Node]).

get_telemetry_status() ->
    {ok, [{enabled, emqx_telemetry:is_enabled()}]}.

get_telemetry_data() ->
    emqx_telemetry:get_telemetry().

rpc_call(Node, Module, Fun, Args) ->
    case rpc:call(Node, Module, Fun, Args) of
        {badrpc, Reason} -> {error, Reason};
        Result -> Result
    end.
