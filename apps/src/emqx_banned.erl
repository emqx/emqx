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

-export([start_link/0, stop/0]).

-export([ check/1
        , create/1
        , delete/1
        , info/1
        ]).

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

%% for tests
-spec(stop() -> ok).
stop() -> gen_server:stop(?MODULE).

-spec(check(emqx_types:clientinfo()) -> boolean()).
check(ClientInfo) ->
    do_check({clientid, maps:get(clientid, ClientInfo, undefined)})
        orelse do_check({username, maps:get(username, ClientInfo, undefined)})
            orelse do_check({peerhost, maps:get(peerhost, ClientInfo, undefined)}).

do_check({_, undefined}) ->
    false;
do_check(Who) when is_tuple(Who) ->
    case mnesia:dirty_read(?BANNED_TAB, Who) of
        [] -> false;
        [#banned{until = Until}] ->
            Until > erlang:system_time(second)
    end.

-spec(create(emqx_types:banned()) -> ok).
create(#{who    := Who,
         by     := By,
         reason := Reason,
         at     := At,
         until  := Until}) ->
    mnesia:dirty_write(?BANNED_TAB, #banned{who = Who,
                                            by = By,
                                            reason = Reason,
                                            at = At,
                                            until = Until});
create(Banned) when is_record(Banned, banned) ->
    mnesia:dirty_write(?BANNED_TAB, Banned).

-spec(delete({clientid, emqx_types:clientid()}
           | {username, emqx_types:username()}
           | {peerhost, emqx_types:peerhost()}) -> ok).
delete(Who) ->
    mnesia:dirty_delete(?BANNED_TAB, Who).

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
    mnesia:async_dirty(fun expire_banned_items/1, [erlang:system_time(second)]),
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
    State#{expiry_timer := emqx_misc:start_timer(10, expire)}.
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

