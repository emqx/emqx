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

-module(emqx_exhook).

-include("emqx_exhook.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[ExHook]").

-export([ enable/1
        , disable/1
        , list/0
        ]).

-export([ cast/2
        , call_fold/3
        ]).

%%--------------------------------------------------------------------
%% Mgmt APIs
%%--------------------------------------------------------------------

-spec enable(atom()|string()) -> ok | {error, term()}.
enable(Name) ->
    with_mngr(fun(Pid) -> emqx_exhook_mngr:enable(Pid, Name) end).

-spec disable(atom()|string()) -> ok | {error, term()}.
disable(Name) ->
    with_mngr(fun(Pid) -> emqx_exhook_mngr:disable(Pid, Name) end).

-spec list() -> [atom() | string()].
list() ->
    with_mngr(fun(Pid) -> emqx_exhook_mngr:list(Pid) end).

with_mngr(Fun) ->
    case lists:keyfind(emqx_exhook_mngr, 1,
                       supervisor:which_children(emqx_exhook_sup)) of
        {_, Pid, _, _} ->
            Fun(Pid);
        _ ->
            {error, no_manager_svr}
    end.

%%--------------------------------------------------------------------
%% Dispatch APIs
%%--------------------------------------------------------------------

-spec cast(atom(), map()) -> ok.
cast(Hookpoint, Req) ->
    cast(Hookpoint, Req, emqx_exhook_mngr:running()).

cast(_, _, []) ->
    ok;
cast(Hookpoint, Req, [ServerName|More]) ->
    %% XXX: Need a real asynchronous running
    _ = emqx_exhook_server:call(Hookpoint, Req,
                                emqx_exhook_mngr:server(ServerName)),
    cast(Hookpoint, Req, More).

-spec call_fold(atom(), term(), function())
  -> {ok, term()}
   | {stop, term()}.
call_fold(Hookpoint, Req, AccFun) ->
    FailedAction = emqx_exhook_mngr:get_request_failed_action(),
    ServerNames = emqx_exhook_mngr:running(),
    case ServerNames == [] andalso FailedAction == deny of
        true ->
            {stop, deny_action_result(Hookpoint, Req)};
        _ ->
            call_fold(Hookpoint, Req, FailedAction, AccFun, ServerNames)
    end.

call_fold(_, Req, _, _, []) ->
    {ok, Req};
call_fold(Hookpoint, Req, FailedAction, AccFun, [ServerName|More]) ->
    Server = emqx_exhook_mngr:server(ServerName),
    case emqx_exhook_server:call(Hookpoint, Req, Server) of
        {ok, Resp} ->
            case AccFun(Req, Resp) of
                {stop, NReq} ->
                    {stop, NReq};
                {ok, NReq} ->
                    call_fold(Hookpoint, NReq, FailedAction, AccFun, More);
                _ ->
                    call_fold(Hookpoint, Req, FailedAction, AccFun, More)
            end;
        _ ->
            case FailedAction of
                deny ->
                    {stop, deny_action_result(Hookpoint, Req)};
                _ ->
                    call_fold(Hookpoint, Req, FailedAction, AccFun, More)
            end
    end.

%% XXX: Hard-coded the deny response
deny_action_result('client.authenticate', _) ->
    #{result => false};
deny_action_result('client.check_acl', _) ->
    #{result => false};
deny_action_result('message.publish', Msg) ->
    %% TODO: Not support to deny a message
    %% maybe we can put the 'allow_publish' into message header
    Msg.
