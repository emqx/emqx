%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exclusive_subscription).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[exclusive]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% For upgrade
-export([on_add_module/0, on_delete_module/0]).

-export([
    check_subscribe/2,
    unsubscribe/2
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
    case ekka_mnesia:copy_table(?TAB, ram_copies) of
        ok ->
            ok;
        {no_exists, _} ->
            mnesia(boot)
    end.

%%--------------------------------------------------------------------
%% Upgrade
%%--------------------------------------------------------------------

on_add_module() ->
    mnesia(boot).

on_delete_module() ->
    mnesia:clear_table(?TAB).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
-spec check_subscribe(emqx_types:clientinfo(), emqx_types:topic()) ->
    allow | deny.
check_subscribe(#{clientid := ClientId}, Topic) ->
    Fun = fun() ->
        try_subscribe(ClientId, Topic)
    end,
    case mnesia:transaction(Fun) of
        {atomic, Res} ->
            Res;
        {aborted, Reason} ->
            ?LOG(warning, "Cannot check subscribe ~p due to ~p.", [Topic, Reason]),
            deny
    end.

unsubscribe(Topic, #{is_exclusive := true}) ->
    _ = mnesia:transaction(fun() -> mnesia:delete({?TAB, Topic}) end),
    ok;
unsubscribe(_Topic, _SubOpts) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
try_subscribe(ClientId, Topic) ->
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
            allow;
        [_] ->
            deny
    end.
