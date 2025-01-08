%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_gc).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/2]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export_type([opts/0, limit/0]).

-type limit() :: all | non_neg_integer().
-type opts() :: #{
    deadline := emqx_retainer:deadline(),
    limit := limit()
}.

-callback clear_expired(_BackendState, emqx_retainer:deadline(), limit()) ->
    {_Complete :: boolean(), _NCleared :: non_neg_integer()}.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec start_link(emqx_retainer:context(), opts()) -> {ok, pid()} | ignore.
start_link(Context, Opts) ->
    case is_responsible() of
        true ->
            gen_server:start_link(?MODULE, {Context, Opts}, []);
        false ->
            ignore
    end.

is_responsible() ->
    Nodes = lists:sort(mria_membership:running_core_nodelist()),
    Nodes =/= [] andalso hd(Nodes) == node().

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init({Context, Opts}) ->
    ok = gen_server:cast(self(), clear_expired),
    {ok, {Context, Opts}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(clear_expired, State = {Context, Opts}) ->
    Result = {Complete, NCleared} = clear_expired(Context, Opts),
    ?tp(debug, emqx_retainer_cleared_expired, #{
        complete => Complete,
        n_cleared => NCleared
    }),
    {stop, {shutdown, Result}, State};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

clear_expired(Context, Opts) ->
    Mod = emqx_retainer:backend_module(Context),
    BackendState = emqx_retainer:backend_state(Context),
    Deadline = maps:get(deadline, Opts),
    Limit = maps:get(limit, Opts),
    Mod:clear_expired(BackendState, Deadline, Limit).
