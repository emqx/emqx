%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_delayed_publish).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

%% Hook callbacks
-export([ load/0
        , on_message_publish/1
        , unload/0
        ]).

-export([start_link/0]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(delayed_message,
        { key
        , msg
        }).

-define(TAB, ?MODULE).
-define(SERVER, ?MODULE).
-define(MAX_INTERVAL, 4294967).

%%-type(state() :: #{timer => reference(), publish_at => integer()}).

%%------------------------------------------------------------------------------
%% Plugin callbacks
%%------------------------------------------------------------------------------

-spec(load() -> ok).
load() ->
    emqx:hook('message.publish', {?MODULE, on_message_publish, []}).

on_message_publish(Msg = #message{id = Id, topic = <<"$delayed/", Topic/binary>>, timestamp = Ts}) ->
    [Delay, Topic1] = binary:split(Topic, <<"/">>),
    PubAt = case binary_to_integer(Delay) of
                Interval when Interval < ?MAX_INTERVAL ->
                    Interval + emqx_time:now_secs(Ts);
                Timestamp ->
                    %% Check malicious timestamp?
                    case (Timestamp - emqx_time:now_secs(Ts)) > ?MAX_INTERVAL of
                        true  -> error(invalid_delayed_timestamp);
                        false -> Timestamp
                    end
            end,
    PubMsg = Msg#message{topic = Topic1},
    Headers = PubMsg#message.headers,
    ok = store(#delayed_message{key = {PubAt, delayed_mid(Id)}, msg = PubMsg}),
    {stop, PubMsg#message{headers = Headers#{allow_publish => false}}};

on_message_publish(Msg) ->
    {ok, Msg}.

delayed_mid(undefined) ->
    emqx_guid:gen();
delayed_mid(MsgId) -> MsgId.

-spec(unload() -> ok).
unload() ->
    emqx:unhook('message.publish', {?MODULE, on_message_publish, []}).

%%------------------------------------------------------------------------------
%% Start delayed publish server
%%------------------------------------------------------------------------------

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(store(#delayed_message{}) -> ok).
store(DelayedMsg) ->
    gen_server:call(?SERVER, {store, DelayedMsg}, infinity).

%%------------------------------------------------------------------------------
%% gen_server callback
%%------------------------------------------------------------------------------

init([]) ->
    ok = ekka_mnesia:create_table(?TAB, [
                {type, set},
                {disc_copies, [node()]},
                {local_content, true},
                {record_name, delayed_message},
                {attributes, record_info(fields, delayed_message)}]),
    ok = mnesia:wait_for_tables([?TAB], 3000),
    {ok, ensure_publish_timer(#{timer => undefined, publish_at => 0})}.

handle_call({store, DelayedMsg = #delayed_message{key = Key}}, _From, State) ->
    ok = mnesia:dirty_write(?TAB, DelayedMsg),
    {reply, ok, ensure_publish_timer(Key, State)};

handle_call(Req, _From, State) ->
    ?LOG(error, "[Delayed] Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "[Delayed] Unexpected cast: ~p", [Msg]),
    {noreply, State}.

%% Do Publish...
handle_info({timeout, TRef, do_publish}, State = #{timer := TRef}) ->
    DeletedKeys = do_publish(mnesia:dirty_first(?TAB), os:system_time(seconds)),
    lists:foreach(fun(Key) -> mnesia:dirty_delete(?TAB, Key) end, DeletedKeys),
    {noreply, ensure_publish_timer(State#{timer := undefined, publish_at := 0})};

handle_info(Info, State) ->
    ?LOG(error, "[Delayed] Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{timer := TRef}) ->
    emqx_misc:cancel_timer(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% Ensure publish timer
ensure_publish_timer(State) ->
    ensure_publish_timer(mnesia:dirty_first(?TAB), State).

ensure_publish_timer('$end_of_table', State) ->
    State#{timer := undefined, publish_at := 0};
ensure_publish_timer({Ts, _Id}, State = #{timer := undefined}) ->
    ensure_publish_timer(Ts, os:system_time(seconds), State);
ensure_publish_timer({Ts, _Id}, State = #{timer := TRef, publish_at := PubAt})
    when Ts < PubAt ->
    ok = emqx_misc:cancel_timer(TRef),
    ensure_publish_timer(Ts, os:system_time(seconds), State);
ensure_publish_timer(_Key, State) ->
    State.

ensure_publish_timer(Ts, Now, State) ->
    Interval = max(1, Ts - Now),
    TRef = emqx_misc:start_timer(timer:seconds(Interval), do_publish),
    State#{timer := TRef, publish_at := Now + Interval}.

do_publish(Key, Now) ->
    do_publish(Key, Now, []).

%% Do publish
do_publish('$end_of_table', _Now, Acc) ->
    Acc;
do_publish({Ts, _Id}, Now, Acc) when Ts > Now ->
    Acc;
do_publish(Key = {Ts, _Id}, Now, Acc) when Ts =< Now ->
    case mnesia:dirty_read(?TAB, Key) of
        [] -> ok;
        [#delayed_message{msg = Msg}] ->
            emqx_pool:async_submit(fun emqx_broker:publish/1, [Msg])
    end,
    do_publish(mnesia:dirty_next(?TAB, Key), Now, [Key|Acc]).
