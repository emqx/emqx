%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Mgmt APIs
-export([ enable/2
        , disable/1
        , disable_all/0
        , list/0
        ]).

-export([ cast/2
        , call_fold/3
        ]).

%%--------------------------------------------------------------------
%% Mgmt APIs
%%--------------------------------------------------------------------

%% XXX: Only return the running servers
-spec list() -> [emqx_exhook_server:server()].
list() ->
    [server(Name) || Name <- running()].

-spec enable(atom()|string(), list()) -> ok | {error, term()}.
enable(Name, Opts) ->
    case lists:member(Name, running()) of
        true ->
            {error, already_started};
        _ ->
            case emqx_exhook_server:load(Name, Opts) of
                {ok, ServiceState} ->
                    save(Name, ServiceState);
                {error, Reason} ->
                    ?LOG(error, "Load server ~p failed: ~p", [Name, Reason]),
                    {error, Reason}
            end
    end.

-spec disable(atom()|string()) -> ok | {error, term()}.
disable(Name) ->
    case server(Name) of
        undefined -> {error, not_running};
        Service ->
            ok = emqx_exhook_server:unload(Service),
            unsave(Name)
    end.

-spec disable_all() -> ok.
disable_all() ->
    lists:foreach(fun disable/1, running()).

%%----------------------------------------------------------
%% Dispatch APIs
%%----------------------------------------------------------

-spec cast(atom(), map()) -> ok.
cast(Hookpoint, Req) ->
    cast(Hookpoint, Req, running()).

cast(_, _, []) ->
    ok;
cast(Hookpoint, Req, [ServiceName|More]) ->
    %% XXX: Need a real asynchronous running
    _ = emqx_exhook_server:call(Hookpoint, Req, server(ServiceName)),
    cast(Hookpoint, Req, More).

-spec call_fold(atom(), term(), function())
  -> {ok, term()}
   | {stop, term()}.
call_fold(Hookpoint, Req, AccFun) ->
    call_fold(Hookpoint, Req, AccFun, running()).

call_fold(_, Req, _, []) ->
    {ok, Req};
call_fold(Hookpoint, Req, AccFun, [ServiceName|More]) ->
    case emqx_exhook_server:call(Hookpoint, Req, server(ServiceName)) of
        {ok, Resp} ->
            case AccFun(Req, Resp) of
                {stop, NReq} -> {stop, NReq};
                {ok, NReq} -> call_fold(Hookpoint, NReq, AccFun, More);
                _ -> call_fold(Hookpoint, Req, AccFun, More)
            end;
        _ ->
            call_fold(Hookpoint, Req, AccFun, More)
    end.

%%----------------------------------------------------------
%% Storage

-compile({inline, [save/2]}).
save(Name, ServiceState) ->
    Saved = persistent_term:get(?APP, []),
    persistent_term:put(?APP, lists:reverse([Name | Saved])),
    persistent_term:put({?APP, Name}, ServiceState).

-compile({inline, [unsave/1]}).
unsave(Name) ->
    case persistent_term:get(?APP, []) of
        [] ->
            persistent_term:erase(?APP);
        Saved ->
            persistent_term:put(?APP, lists:delete(Name, Saved))
    end,
    persistent_term:erase({?APP, Name}),
    ok.

-compile({inline, [running/0]}).
running() ->
    persistent_term:get(?APP, []).

-compile({inline, [server/1]}).
server(Name) ->
    case catch persistent_term:get({?APP, Name}) of
        {'EXIT', {badarg,_}} -> undefined;
        Service -> Service
    end.
