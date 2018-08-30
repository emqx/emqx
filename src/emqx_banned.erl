%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_banned).

-behaviour(gen_server).

-include("emqx.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% API
-export([start_link/0]).
-export([check/1]).
-export([add/1, del/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).

-record(state, {expiry_timer}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, ordered_set},
                {disc_copies, [node()]},
                {record_name, banned},
                {attributes, record_info(fields, banned)}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start the banned server
-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(check(emqx_types:credentials()) -> boolean()).
check(#{client_id := ClientId, username := Username, peername := {IPAddr, _}}) ->
    ets:member(?TAB, {client_id, ClientId})
        orelse ets:member(?TAB, {username, Username})
            orelse ets:member(?TAB, {ipaddr, IPAddr}).

add(Record) when is_record(Record, banned) ->
    mnesia:dirty_write(?TAB, Record).

del(Key) ->
    mnesia:dirty_delete(?TAB, Key).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    emqx_time:seed(),
    {ok, ensure_expiry_timer(#state{})}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[BANNED] Unexpected request: ~p", [Req]),
    {reply, ignore, State}.

handle_cast(Msg, State) ->
    emqx_logger:error("[BANNED] Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, Ref, expire}, State = #state{expiry_timer = Ref}) ->
    mnesia:async_dirty(fun expire_banned_items/1, [erlang:timestamp()]),
    {noreply, ensure_expiry_timer(State), hibernate};

handle_info(Info, State) ->
    emqx_logger:error("[BANNED] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{expiry_timer = Timer}) ->
    emqx_misc:cancel_timer(Timer).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_expiry_timer(State) ->
    Interval = emqx_config:get_env(banned_expiry_interval, timer:minutes(5)),
    State#state{expiry_timer = emqx_misc:start_timer(
                                 Interval + rand:uniform(Interval), expire)}.

expire_banned_items(Now) ->
    expire_banned_item(mnesia:first(?TAB), Now).

expire_banned_item('$end_of_table', _Now) ->
    ok;
expire_banned_item(Key, Now) ->
    case mnesia:read(?TAB, Key) of
        [#banned{until = undefined}] -> ok;
        [B = #banned{until = Until}] when Until < Now ->
           mnesia:delete_object(?TAB, B, sticky_write);
        [_] -> ok;
        [] -> ok
    end,
    expire_banned_item(mnesia:next(?TAB, Key), Now).

