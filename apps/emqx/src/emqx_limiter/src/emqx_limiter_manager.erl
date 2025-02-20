%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_manager).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%% API
-export([
    start_link/0,
    find_bucket/2,
    insert_bucket/3,
    delete_bucket/2,
    post_config_update/5
]).

-export([
    find_root/1,
    insert_root/2,
    delete_root/1
]).

-export([
    start_server/1,
    start_server/2,
    restart_server/1,
    stop_server/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-type limiter_id() :: emqx_limiter_schema:limiter_id().
-type limiter_type() :: emqx_limiter_schema:limiter_type().
-type uid() :: {limiter_id(), limiter_type()}.

%% counter record in ets table
-record(bucket, {
    uid :: uid(),
    bucket :: bucket_ref()
}).

-type bucket_ref() :: emqx_limiter_bucket_ref:bucket_ref().

-define(UID(Id, Type), {Id, Type}).
-define(TAB, emqx_limiter_counters).
-define(ROOT_ID, root).

%%--------------------------------------------------------------------
%%  API
%%--------------------------------------------------------------------
-spec start_server(limiter_type()) -> _.
start_server(Type) ->
    emqx_limiter_server_sup:start(Type).

-spec start_server(limiter_type(), hocon:config()) -> _.
start_server(Type, Cfg) ->
    emqx_limiter_server_sup:start(Type, Cfg).

-spec restart_server(limiter_type()) -> _.
restart_server(Type) ->
    emqx_limiter_server:restart(Type).

-spec stop_server(limiter_type()) -> _.
stop_server(Type) ->
    emqx_limiter_server_sup:stop(Type).

-spec find_bucket(limiter_id(), limiter_type()) ->
    {ok, bucket_ref()} | undefined.
find_bucket(Id, Type) ->
    case ets:lookup(?TAB, ?UID(Id, Type)) of
        [#bucket{bucket = Bucket}] ->
            {ok, Bucket};
        _ ->
            undefined
    end.

-spec insert_bucket(
    limiter_id(),
    limiter_type(),
    bucket_ref()
) -> boolean().
insert_bucket(Id, Type, Bucket) ->
    ets:insert(
        ?TAB,
        #bucket{uid = ?UID(Id, Type), bucket = Bucket}
    ).

-spec delete_bucket(limiter_id(), limiter_type()) -> true.
delete_bucket(Id, Type) ->
    ets:delete(?TAB, ?UID(Id, Type)).

-spec find_root(limiter_type()) ->
    {ok, bucket_ref()} | undefined.
find_root(Type) ->
    find_bucket(?ROOT_ID, Type).

-spec insert_root(
    limiter_type(),
    bucket_ref()
) -> boolean().
insert_root(Type, Bucket) ->
    insert_bucket(?ROOT_ID, Type, Bucket).

-spec delete_root(limiter_type()) -> true.
delete_root(Type) ->
    delete_bucket(?ROOT_ID, Type).

post_config_update([limiter], _Config, NewConf, _OldConf, _AppEnvs) ->
    Conf = emqx_limiter_utils:convert_node_opts(NewConf),
    _ = [on_post_config_update(Type, Cfg) || {Type, Cfg} <- maps:to_list(Conf)],
    ok.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()}
    | {error, Error :: {already_started, pid()}}
    | {error, Error :: term()}
    | ignore.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%%  gen_server callbacks
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, State :: term()}
    | {ok, State :: term(), Timeout :: timeout()}
    | {ok, State :: term(), hibernate}
    | {stop, Reason :: term()}
    | ignore.
init([]) ->
    ok = emqx_config_handler:add_handler([limiter], ?MODULE),
    _ = ets:new(?TAB, [
        set,
        public,
        named_table,
        {keypos, #bucket.uid},
        {write_concurrency, true},
        {read_concurrency, true},
        {heir, erlang:whereis(emqx_limiter_sup), none}
    ]),
    {ok, #{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
    {reply, Reply :: term(), NewState :: term()}
    | {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()}
    | {reply, Reply :: term(), NewState :: term(), hibernate}
    | {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: term(), Reply :: term(), NewState :: term()}
    | {stop, Reason :: term(), NewState :: term()}.
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignore, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: term(), NewState :: term()}.
handle_cast(Req, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
    {noreply, NewState :: term()}
    | {noreply, NewState :: term(), Timeout :: timeout()}
    | {noreply, NewState :: term(), hibernate}
    | {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(
    Reason :: normal | shutdown | {shutdown, term()} | term(),
    State :: term()
) -> any().
terminate(_Reason, _State) ->
    emqx_config_handler:remove_handler([limiter]),
    ok.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
on_post_config_update(Type, Config) ->
    case emqx_limiter_server:whereis(Type) of
        undefined ->
            start_server(Type, Config);
        _ ->
            emqx_limiter_server:update_config(Type, Config)
    end.
