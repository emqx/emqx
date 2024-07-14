%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc A microscopic event pub/sub module.
-module(emqx_ds_upubsub).

%% API:
-export([init/1, destroy/1, pub/3, sub/3, unsub/1, unsub_all/1]).

-include("emqx_ds_upubsub.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type dispatch_key() :: term().

-type name() :: term().

-define(pterm(NAME), {?MODULE, NAME}).

%%================================================================================
%% API functions
%%================================================================================

-spec init(name()) -> ok.
init(Name) ->
    _ = destroy(Name),
    Tid = ets:new(ds_upubsub_table, [
        bag, public, {read_concurrency, false}, {write_concurrency, true}
    ]),
    persistent_term:put(?pterm(Name), Tid).

-spec destroy(name()) -> ok.
destroy(Name) ->
    case persistent_term:get(?pterm(Name), undefined) of
        undefined ->
            ok;
        Tid ->
            ets:delete(Tid),
            ok
    end.

-spec pub(name(), dispatch_key(), term()) -> ok.
pub(Name, DispatchKey, Msg) ->
    Tab = persistent_term:get(?pterm(Name)),
    lists:foreach(
        fun(Alias) ->
            Alias ! #upubsub{ref = Alias, val = Msg}
        end,
        ets:lookup_element(Tab, DispatchKey, 2, [])
    ).

-spec sub(name(), dispatch_key(), #{oneshot => boolean()}) -> reference().
sub(Name, DispatchKey, Opts) ->
    Tab = persistent_term:get(?pterm(Name)),
    Alias =
        case Opts of
            #{oneshot := true} -> alias([reply]);
            _ -> alias([explicit_unalias])
        end,
    %% Subscriptions are kept in the process dictionary to enable
    %% semi-automatic cleanup:
    put({?MODULE, Alias}, {Tab, DispatchKey}),
    ets:insert(Tab, {DispatchKey, Alias}).

-spec unsub(reference()) -> boolean().
unsub(Ref) ->
    case erase({?MODULE, Ref}) of
        {Tab, DispatchKey} ->
            ets:delete_object(Tab, {DispatchKey, Ref}),
            unalias(Ref),
            flush(Ref),
            true;
        _ ->
            false
    end.

%% @doc `exit' option means the process is about to exit, so this
%% function shouldn't bother doing costly cleanups (like flushing the
%% received messages), and just remove global subscription.
-spec unsub_all(exit) -> ok.
unsub_all(exit) ->
    lists:foreach(
        fun
            ({{?MODULE, Ref}, {Tab, DispatchKey}}) ->
                ets:delete_object(Tab, {DispatchKey, Ref});
            (_) ->
                ok
        end,
        get()
    ).

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

flush(Ref) ->
    receive
        #upubsub{ref = Ref} -> flush(Ref)
    after 0 ->
        ok
    end.
