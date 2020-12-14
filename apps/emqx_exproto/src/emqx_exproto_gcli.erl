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

%% the gRPC client worker for ConnectionHandler service
-module(emqx_exproto_gcli).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-logger_header("[ExProto gClient]").

%% APIs
-export([async_call/3]).

-export([start_link/2]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(CONN_ADAPTER_MOD, emqx_exproto_v_1_connection_handler_client).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link(Pool, Id) ->
    gen_server:start_link({local, emqx_misc:proc_name(?MODULE, Id)},
                          ?MODULE, [Pool, Id], []).

async_call(FunName, Req = #{conn := Conn}, Options) ->
    cast(pick(Conn), {rpc, FunName, Req, Options, self()}).

%%--------------------------------------------------------------------
%% cast, pick
%%--------------------------------------------------------------------

-compile({inline, [cast/2, pick/1]}).

cast(Deliver, Msg) ->
    gen_server:cast(Deliver, Msg).

pick(Conn) ->
    gproc_pool:pick_worker(exproto_gcli_pool, Conn).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({rpc, Fun, Req, Options, From}, State) ->
    try
        case apply(?CONN_ADAPTER_MOD, Fun, [Req, Options]) of
            {ok, Resp, _Metadata} ->
                ?LOG(debug, "~p got {ok, ~0p, ~0p}", [Fun, Resp, _Metadata]),
                reply(From, Fun, {ok, Resp});
            {error, {Code, Msg}, _Metadata} ->
                ?LOG(error, "CALL ~0p:~0p(~0p, ~0p) response errcode: ~0p, errmsg: ~0p",
                        [?CONN_ADAPTER_MOD, Fun, Req, Options, Code, Msg]),
                reply(From, Fun, {error, {Code, Msg}});
            {error, Reason} ->
                ?LOG(error, "CALL ~0p:~0p(~0p, ~0p) error: ~0p",
                        [?CONN_ADAPTER_MOD, Fun, Req, Options, Reason]),
                reply(From, Fun, {error, Reason})
        end
    catch _ : Rsn : Stk ->
        ?LOG(error, "CALL ~0p:~0p(~0p, ~0p) throw an exception: ~0p, stacktrace: ~0p",
             [?CONN_ADAPTER_MOD, Fun, Req, Options, Rsn, Stk]),
        reply(From, Fun, {error, Rsn})
    end,
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

reply(Pid, Fun, Result) ->
    Pid ! {hreply, Fun, Result},
    ok.
