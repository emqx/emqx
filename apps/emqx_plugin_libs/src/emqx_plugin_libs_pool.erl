%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugin_libs_pool).

-export([ start_pool/3
        , stop_pool/1
        , pool_name/1
        , health_check/3
        ]).

pool_name(ID) when is_binary(ID) ->
    list_to_atom(binary_to_list(ID)).

start_pool(Name, Mod, Options) ->
    case ecpool:start_sup_pool(Name, Mod, Options) of
        {ok, _} -> logger:log(info, "Initiated ~0p Successfully", [Name]);
        {error, {already_started, _Pid}} ->
            stop_pool(Name),
            start_pool(Name, Mod, Options);
        {error, Reason} ->
            logger:log(error, "Initiate ~0p failed ~0p", [Name, Reason]),
            error({start_pool_failed, Name})
    end.

stop_pool(Name) ->
    case ecpool:stop_sup_pool(Name) of
        ok -> logger:log(info, "Destroyed ~0p Successfully", [Name]);
        {error, Reason} ->
            logger:log(error, "Destroy ~0p failed, ~0p", [Name, Reason]),
            error({stop_pool_failed, Name})
    end.

health_check(PoolName, CheckFunc, State) when is_function(CheckFunc) ->
    Status = [begin
        case ecpool_worker:client(Worker) of
            {ok, Conn} -> CheckFunc(Conn);
            _ -> false
        end
    end || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    case length(Status) > 0 andalso lists:all(fun(St) -> St =:= true end, Status) of
        true -> {ok, State};
        false -> {error, test_query_failed}
    end.
