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

-module(emqx_retainer_mnesia).

-behaviour(emqx_retainer).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").

-logger_header("[Retainer]").

-export([delete_message/2
        , store_retained/2
        , read_message/2
        , match_messages/3
        , clear_expired/1
        , clean/1]).

-export([create_resource/1]).

-define(DEF_MAX_RETAINED_MESSAGES, 0).

-rlog_shard({?RETAINER_SHARD, ?TAB}).

-record(retained, {topic, msg, expiry_time}).

-type batch_read_result() ::
        {ok, list(emqx:message()), cursor()}.

%%--------------------------------------------------------------------
%% emqx_retainer_storage callbacks
%%--------------------------------------------------------------------
create_resource(#{storage_type := StorageType}) ->
    Copies = case StorageType of
                 ram       -> ram_copies;
                 disc      -> disc_copies;
                 disc_only -> disc_only_copies
             end,
    StoreProps = [{ets, [compressed,
                         {read_concurrency, true},
                         {write_concurrency, true}]},
                  {dets, [{auto_save, 1000}]}],
    ok = ekka_mnesia:create_table(?TAB, [
                {type, set},
                {Copies, [node()]},
                {record_name, retained},
                {attributes, record_info(fields, retained)},
                {storage_properties, StoreProps}]),
    ok = ekka_mnesia:copy_table(?TAB, Copies),
    ok = ekka_rlog:wait_for_shards([?RETAINER_SHARD], infinity),
    case mnesia:table_info(?TAB, storage_type) of
        Copies -> ok;
        _Other ->
            {atomic, ok} = mnesia:change_table_copy_type(?TAB, node(), Copies),
            ok
    end.

store_retained(_, Msg =#message{topic = Topic}) ->
    ExpiryTime = emqx_retainer:get_expiry_time(Msg),
    case is_table_full() of
        false ->
            ok = emqx_metrics:inc('messages.retained'),
            ekka_mnesia:dirty_write(?TAB,
                                    #retained{topic = topic2tokens(Topic),
                                              msg = Msg,
                                              expiry_time = ExpiryTime});
        _ ->
            Tokens = topic2tokens(Topic),
            Fun = fun() ->
                          case mnesia:read(?TAB, Tokens) of
                              [_] ->
                                  mnesia:write(?TAB,
                                               #retained{topic = Tokens,
                                                         msg = Msg,
                                                         expiry_time = ExpiryTime},
                                               write);
                              [] ->
                                  ?LOG(error,
                                       "Cannot retain message(topic=~s) for table is full!",
                                       [Topic]),
                                  ok
                          end
            end,
            {atomic, ok} = ekka_mnesia:transaction(?RETAINER_SHARD, Fun),
            ok
    end.

clear_expired(_) ->
    NowMs = erlang:system_time(millisecond),
    MsHd = #retained{topic = '$1', msg = '_', expiry_time = '$3'},
    Ms = [{MsHd, [{'=/=', '$3', 0}, {'<', '$3', NowMs}], ['$1']}],
    Fun = fun() ->
                  Keys = mnesia:select(?TAB, Ms, write),
                  lists:foreach(fun(Key) -> mnesia:delete({?TAB, Key}) end, Keys)
          end,
    {atomic, _} = ekka_mnesia:transaction(?RETAINER_SHARD, Fun),
    ok.

delete_message(_, Topic) ->
    case emqx_topic:wildcard(Topic) of
        true -> match_delete_messages(Topic);
        false ->
            Tokens = topic2tokens(Topic),
            Fun = fun() ->
                       mnesia:delete({?TAB, Tokens})
                  end,
            case ekka_mnesia:transaction(?RETAINER_SHARD, Fun) of
                {atomic, Result} ->
                    Result;
                ok ->
                    ok
                end
    end,
    ok.

read_message(_, Topic) ->
    {ok, read_messages(Topic)}.

match_messages(_, Topic, Cursor) ->
    MaxReadNum = emqx_config:get([?APP, flow_control, max_read_number]),
    case Cursor of
        undefined ->
            case MaxReadNum of
                0 ->
                    {ok, sort_retained(match_messages(Topic)), undefined};
                _ ->
                    start_batch_read(Topic, MaxReadNum)
            end;
        _ ->
            batch_read_messages(Cursor, MaxReadNum)
    end.

clean(_) ->
    ekka_mnesia:clear_table(?TAB),
    ok.
%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
sort_retained([]) -> [];
sort_retained([Msg]) -> [Msg];
sort_retained(Msgs)  ->
    lists:sort(fun(#message{timestamp = Ts1}, #message{timestamp = Ts2}) ->
                       Ts1 =< Ts2 end,
               Msgs).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------
topic2tokens(Topic) ->
    emqx_topic:words(Topic).

-spec start_batch_read(topic(), pos_integer()) -> batch_read_result().
start_batch_read(Topic, MaxReadNum) ->
    Ms = make_match_spec(Topic),
    TabQH = ets:table(?TAB, [{traverse, {select, Ms}}]),
    QH = qlc:q([E || E <- TabQH]),
    Cursor = qlc:cursor(QH),
    batch_read_messages(Cursor, MaxReadNum).

-spec batch_read_messages(emqx_retainer_storage:cursor(), pos_integer()) -> batch_read_result().
batch_read_messages(Cursor, MaxReadNum) ->
    Answers = qlc:next_answers(Cursor, MaxReadNum),
    Orders = sort_retained(Answers),
    case erlang:length(Orders) < MaxReadNum of
        true ->
            qlc:delete_cursor(Cursor),
            {ok, Orders, undefined};
        _ ->
            {ok, Orders, Cursor}
    end.

-spec(read_messages(emqx_types:topic())
      -> [emqx_types:message()]).
read_messages(Topic) ->
    Tokens = topic2tokens(Topic),
    case mnesia:dirty_read(?TAB, Tokens) of
        [] -> [];
        [#retained{msg = Msg, expiry_time = Et}] ->
            case Et =:= 0 orelse Et >= erlang:system_time(millisecond) of
                true -> [Msg];
                false -> []
            end
    end.

-spec(match_messages(emqx_types:topic())
      -> [emqx_types:message()]).
match_messages(Filter) ->
    Ms = make_match_spec(Filter),
    mnesia:dirty_select(?TAB, Ms).

-spec(match_delete_messages(emqx_types:topic()) -> ok).
match_delete_messages(Filter) ->
    Cond = condition(emqx_topic:words(Filter)),
    MsHd = #retained{topic = Cond, msg = '_', expiry_time = '_'},
    Ms = [{MsHd, [], ['$_']}],
    Rs = mnesia:dirty_select(?TAB, Ms),
    lists:foreach(fun(R) -> ekka_mnesia:dirty_delete_object(?TAB, R) end, Rs).

%% @private
condition(Ws) ->
    Ws1 = [case W =:= '+' of true -> '_'; _ -> W end || W <- Ws],
    case lists:last(Ws1) =:= '#' of
        false -> Ws1;
        _ -> (Ws1 -- ['#']) ++ '_'
    end.

-spec make_match_spec(topic()) -> ets:match_spec().
make_match_spec(Filter) ->
    NowMs = erlang:system_time(millisecond),
    Cond = condition(emqx_topic:words(Filter)),
    MsHd = #retained{topic = Cond, msg = '$2', expiry_time = '$3'},
    [{MsHd, [{'=:=', '$3', 0}], ['$2']},
     {MsHd, [{'>', '$3', NowMs}], ['$2']}].

-spec is_table_full() -> boolean().
is_table_full() ->
    [#{config := Cfg} | _] = emqx_config:get([?APP, connector]),
    Limit = maps:get(max_retained_messages,
                     Cfg,
                     ?DEF_MAX_RETAINED_MESSAGES),
    Limit > 0 andalso (table_size() >= Limit).

-spec table_size() -> non_neg_integer().
table_size() ->
    mnesia:table_info(?TAB, size).
