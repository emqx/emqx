%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_listeners_conf).

-behaviour(emqx_config_backup).

-export([
    action/4,
    create/3,
    ensure_remove/2,
    get_raw/2,
    update/3
]).

%% Data backup
-export([
    import_config/1
]).

-include_lib("emqx/include/logger.hrl").

-define(CONF_ROOT_KEY, listeners).
-define(path(_Type_, _Name_), [?CONF_ROOT_KEY, _Type_, _Name_]).
-define(OPTS, #{rawconf_with_defaults => true, override_to => cluster}).
-define(IMPORT_OPTS, #{override_to => cluster}).

action(Type, Name, Action, Conf) ->
    wrap(emqx_conf:update(?path(Type, Name), {action, Action, Conf}, ?OPTS)).

create(Type, Name, Conf) ->
    wrap(emqx_conf:update(?path(Type, Name), {create, Conf}, ?OPTS)).

ensure_remove(Type, Name) ->
    wrap(emqx_conf:tombstone(?path(Type, Name), ?OPTS)).

get_raw(Type, Name) -> emqx_conf:get_raw(?path(Type, Name), undefined).

update(Type, Name, Conf) ->
    wrap(emqx_conf:update(?path(Type, Name), {update, Conf}, ?OPTS)).

wrap({error, {post_config_update, emqx_listeners, Reason}}) -> {error, Reason};
wrap({error, {pre_config_update, emqx_listeners, Reason}}) -> {error, Reason};
wrap({error, Reason}) -> {error, Reason};
wrap(Ok) -> Ok.

%%------------------------------------------------------------------------------
%% Data backup
%%------------------------------------------------------------------------------

import_config(RawConf) ->
    NewConf = maps:get(<<"listeners">>, RawConf, #{}),
    OldConf = emqx:get_raw_config([?CONF_ROOT_KEY], #{}),
    MergedConf = merge_confs(OldConf, NewConf),
    case emqx_conf:update([?CONF_ROOT_KEY], MergedConf, ?IMPORT_OPTS) of
        {ok, #{raw_config := NewRawConf}} ->
            {ok, #{root_key => ?CONF_ROOT_KEY, changed => changed_paths(OldConf, NewRawConf)}};
        Error ->
            {error, #{root_key => ?CONF_ROOT_KEY, reason => Error}}
    end.

merge_confs(OldConf, NewConf) ->
    AllTypes = maps:keys(maps:merge(OldConf, NewConf)),
    lists:foldr(
        fun(Type, Acc) ->
            NewListeners = maps:get(Type, NewConf, #{}),
            OldListeners = maps:get(Type, OldConf, #{}),
            Acc#{Type => maps:merge(OldListeners, NewListeners)}
        end,
        #{},
        AllTypes
    ).

changed_paths(OldRawConf, NewRawConf) ->
    maps:fold(
        fun(Type, Listeners, ChangedAcc) ->
            OldListeners = maps:get(Type, OldRawConf, #{}),
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(Listeners, OldListeners)),
            [?path(Type, K) || K <- maps:keys(Changed)] ++ ChangedAcc
        end,
        [],
        NewRawConf
    ).
