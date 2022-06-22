%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_exclusive).

-behaviour(emqx_gen_mod).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[exclusive]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% emqx_gen_mod callbacks
-export([
    load/1,
    unload/1,
    description/0
]).

-export([
    exclusive_subscribe/3,
    exclusive_unsubscribe/3
]).

-record(exclusive_subscription, {
    topic :: emqx_types:topic(),
    clientid :: emqx_types:clientid()
}).

-define(TAB, emqx_exclusive_subscription).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    StoreProps = [
        {ets, [
            {read_concurrency, true},
            {write_concurrency, true}
        ]}
    ],
    ok = ekka_mnesia:create_table(?TAB, [
        {type, set},
        {ram_copies, [node()]},
        {record_name, exclusive_subscription},
        {attributes, record_info(fields, exclusive_subscription)},
        {storage_properties, StoreProps}
    ]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TAB, ram_copies).

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

-spec load(list()) -> ok.
load(_Env) ->
    emqx_hooks:put('client.subscribe', {?MODULE, exclusive_subscribe, []}),
    emqx_hooks:put('client.unsubscribe', {?MODULE, exclusive_unsubscribe, []}).

-spec unload(list()) -> ok.
unload(_Env) ->
    emqx_hooks:del('client.subscribe', {?MODULE, exclusive_subscribe}),
    emqx_hooks:del('client.unsubscribe', {?MODULE, exclusive_unsubscribe}).

description() ->
    "EMQ X Exclusive Subscription Module".
%%--------------------------------------------------------------------
%% Hooks
%%--------------------------------------------------------------------
exclusive_subscribe(ClientInfo, _Prop, TopicFilters) ->
    {ok, check_is_enabled(ClientInfo, TopicFilters, fun on_subscribe/2)}.

exclusive_unsubscribe(ClientInfo, _Prop, TopicFilters) ->
    check_is_enabled(ClientInfo, TopicFilters, fun on_unsubscribe/2).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
check_is_enabled(#{zone := Zone} = ClientInfo, TopicFilters, Cont) ->
    case emqx_zone:get_env(Zone, exclusive_subscription) of
        false ->
            TopicFilters;
        _ ->
            case lists:any(fun is_exclusive_subscribe/1, TopicFilters) of
                false ->
                    TopicFilters;
                _ ->
                    Cont(ClientInfo, TopicFilters)
            end
    end.

on_subscribe(#{clientid := ClientId}, TopicFilters) ->
    Fun = fun() ->
        try_subscribe(ClientId, TopicFilters)
    end,
    case mnesia:transaction(Fun) of
        {atomic, Res} ->
            Res;
        {aborted, Reason} ->
            ?LOG(warning, "Cannot check subscribe ~p due to ~p.", [TopicFilters, Reason]),
            lists:map(
                fun({Filter, SubOpts} = TopicFilter) ->
                    case is_exclusive_subscribe(Filter) of
                        false ->
                            TopicFilter;
                        _ ->
                            {Filter, SubOpts#{is_exclusive => true}}
                    end
                end,
                TopicFilters
            )
    end.

try_subscribe(ClientId, TopicFilters) ->
    try_subscribe(TopicFilters, ClientId, []).

try_subscribe([{<<"$exclusive/", _/binary>> = Topic, SubOpts} = TopicFilters | T], ClientId, Acc) ->
    try_subscribe(
        T,
        ClientId,
        case mnesia:wread({?TAB, Topic}) of
            [] ->
                mnesia:write(
                    ?TAB,
                    #exclusive_subscription{
                        clientid = ClientId,
                        topic = Topic
                    },
                    write
                ),
                [TopicFilters | Acc];
            [_] ->
                [{Topic, SubOpts#{is_exclusive => true}} | Acc]
        end
    );
try_subscribe([H | T], ClientId, Acc) ->
    try_subscribe(T, ClientId, [H | Acc]);
try_subscribe([], _ClientId, Acc) ->
    lists:reverse(Acc).

on_unsubscribe(#{clientid := ClientId}, TopicFilters) ->
    _ = mnesia:transaction(fun() -> try_unsubscribe(TopicFilters, ClientId) end).

try_unsubscribe([{<<"$exclusive/", _/binary>> = Topic, _} | T], ClientId) ->
    case mnesia:wread({?TAB, Topic}) of
        [#exclusive_subscription{clientid = ClientId}] ->
            mnesia:delete({?TAB, Topic});
        _ ->
            ok
    end,
    try_unsubscribe(T, ClientId);
try_unsubscribe([H | T], ClientId) ->
    try_unsubscribe(T, ClientId);
try_unsubscribe([], _) ->
    ok.

is_exclusive_subscribe({<<"$exclusive/", Rest/binary>>, _SubOpt}) when Rest =/= <<>> ->
    true;
is_exclusive_subscribe(<<"$exclusive/", Rest/binary>>) when Rest =/= <<>> ->
    true;
is_exclusive_subscribe(_) ->
    false.
