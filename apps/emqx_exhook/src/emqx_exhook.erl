%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook).

-include("emqx_exhook.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    cast/2,
    call_fold/3
]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

%%--------------------------------------------------------------------
%% Dispatch APIs
%%--------------------------------------------------------------------

-spec cast(atom(), map()) -> ok.
cast(Hookpoint, Req) ->
    cast(Hookpoint, Req, emqx_exhook_mgr:running()).

cast(_, _, []) ->
    ok;
cast(Hookpoint, Req, [ServerName | More]) ->
    %% XXX: Need a real asynchronous running
    _ = emqx_exhook_server:call(
        Hookpoint,
        Req,
        emqx_exhook_mgr:server(ServerName)
    ),
    cast(Hookpoint, Req, More).

-spec call_fold(atom(), term(), function()) ->
    {ok, term()}
    | {stop, term()}.
call_fold(Hookpoint, Req, AccFun) ->
    case emqx_exhook_mgr:running() of
        [] ->
            {stop, deny_action_result(Hookpoint, Req)};
        ServerNames ->
            call_fold(Hookpoint, Req, AccFun, ServerNames)
    end.

call_fold(_, Req, _, []) ->
    {ok, Req};
call_fold(Hookpoint, Req, AccFun, [ServerName | More]) ->
    Server = emqx_exhook_mgr:server(ServerName),
    case emqx_exhook_server:call(Hookpoint, Req, Server) of
        {ok, Resp} ->
            case AccFun(Req, Resp) of
                {stop, NReq} ->
                    {stop, NReq};
                {ok, NReq} ->
                    call_fold(Hookpoint, NReq, AccFun, More);
                _ ->
                    call_fold(Hookpoint, Req, AccFun, More)
            end;
        _ ->
            case emqx_exhook_server:failed_action(Server) of
                ignore ->
                    call_fold(Hookpoint, Req, AccFun, More);
                deny ->
                    {stop, deny_action_result(Hookpoint, Req)}
            end
    end.

%% XXX: Hard-coded the deny response
deny_action_result('client.authenticate', _) ->
    #{result => false};
deny_action_result('client.authorize', _) ->
    #{result => false};
deny_action_result('message.publish', Msg) ->
    %% TODO: Not support to deny a message
    %% maybe we can put the 'allow_publish' into message header
    Msg.

%%--------------------------------------------------------------------
%% APIs for `emqx_telemetry'
%%--------------------------------------------------------------------

-spec get_basic_usage_info() ->
    #{
        num_servers => non_neg_integer(),
        servers =>
            [
                #{
                    driver => Driver,
                    hooks => [emqx_exhook_server:hookpoint()]
                }
            ]
    }
when
    Driver :: grpc.
get_basic_usage_info() ->
    try
        Servers = emqx_exhook_mgr:running(),
        NumServers = length(Servers),
        ServerInfo =
            lists:map(
                fun(ServerName) ->
                    Hooks = emqx_exhook_mgr:hooks(ServerName),
                    HookNames = lists:map(fun(#{name := Name}) -> Name end, Hooks),
                    #{
                        hooks => HookNames,
                        %% currently, only grpc driver exists.
                        driver => grpc
                    }
                end,
                Servers
            ),
        #{
            num_servers => NumServers,
            servers => ServerInfo
        }
    catch
        _:_ ->
            #{
                num_servers => 0,
                servers => []
            }
    end.
