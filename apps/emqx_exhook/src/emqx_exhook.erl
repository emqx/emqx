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
cast(Hookpoint, Req, [ServiceName|More]) ->
    %% XXX: Need a real asynchronous running
    _ = emqx_exhook_server:call(Hookpoint, Req,
                                emqx_exhook_mngr:server(ServiceName)),
    cast(Hookpoint, Req, More).

-spec call_fold(atom(), term(), function())
  -> {ok, term()}
   | {stop, term()}.
call_fold(Hookpoint, Req, AccFun) ->
    call_fold(Hookpoint, Req, AccFun, emqx_exhook_mngr:running()).

call_fold(_, Req, _, []) ->
    {ok, Req};
call_fold(Hookpoint, Req, AccFun, [ServiceName|More]) ->
    case emqx_exhook_server:call(Hookpoint, Req,
                                 emqx_exhook_mngr:server(ServiceName)) of
        {ok, Resp} ->
            case AccFun(Req, Resp) of
                {stop, NReq} -> {stop, NReq};
                {ok, NReq} -> call_fold(Hookpoint, NReq, AccFun, More);
                _ -> call_fold(Hookpoint, Req, AccFun, More)
            end;
        _ ->
            call_fold(Hookpoint, Req, AccFun, More)
    end.
