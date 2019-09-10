%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_banned).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Banned]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([start_link/0]).

-export([ check/1
        , add/1
        , delete/1
        , info/1
        ]).

-export([is_enabled/1]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(BANNED_TAB, ?MODULE).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?BANNED_TAB, [
                {type, set},
                {disc_copies, [node()]},
                {record_name, banned},
                {attributes, record_info(fields, banned)},
                {storage_properties, [{ets, [{read_concurrency, true}]}]}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?BANNED_TAB).

%% @doc Start the banned server.
-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(check(emqx_types:client()) -> boolean()).
check(#{zone      := Zone,
        client_id := ClientId,
        username  := Username,
        peername  := {IPAddr, _}
       }) ->
    is_enabled(Zone) andalso
        ets:member(?BANNED_TAB, {client_id, ClientId})
            orelse ets:member(?BANNED_TAB, {username, Username})
                orelse ets:member(?BANNED_TAB, {ipaddr, IPAddr}).

is_enabled(Zone) ->
    emqx_zone:get_env(Zone, enable_ban, false).

-spec(add(emqx_types:banned()) -> ok).
add(Banned) when is_record(Banned, banned) ->
    mnesia:dirty_write(?BANNED_TAB, Banned).

-spec(delete({client_id, emqx_types:client_id()} |
             {username, emqx_types:username()} |
             {peername, emqx_types:peername()}) -> ok).
delete(Key) ->
    mnesia:dirty_delete(?BANNED_TAB, Key).

info(InfoKey) ->
    mnesia:table_info(?BANNED_TAB, InfoKey).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, ensure_expiry_timer(#{expiry_timer => undefined})}.

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, TRef, expire}, State = #{expiry_timer := TRef}) ->
    mnesia:async_dirty(fun expire_banned_items/1,
                       [erlang:system_time(second)]),
    {noreply, ensure_expiry_timer(State), hibernate};

handle_info(Info, State) ->
    ?LOG(error, "unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{expiry_timer := TRef}) ->
    emqx_misc:cancel_timer(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-ifdef(TEST).
ensure_expiry_timer(State) ->
    State#{expiry_timer := emqx_misc:start_timer(timer:seconds(1), expire)}.
-else.
ensure_expiry_timer(State) ->
    State#{expiry_timer := emqx_misc:start_timer(timer:minutes(1), expire)}.
-endif.

expire_banned_items(Now) ->
    mnesia:foldl(
      fun(B = #banned{until = Until}, _Acc) when Until < Now ->
              mnesia:delete_object(?BANNED_TAB, B, sticky_write);
         (_, _Acc) -> ok
      end, ok, ?BANNED_TAB).

