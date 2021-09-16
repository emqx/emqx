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

-module(emqx_authz_mnesia).

-include("emqx_authz.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

%% AuthZ Callbacks
-export([ mnesia/1
        , authorize/4
        , description/0
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    ok = ekka_mnesia:create_table(?ACL_TABLE, [
            {type, ordered_set},
            {rlog_shard, ?ACL_SHARDED},
            {disc_copies, [node()]},
            {attributes, record_info(fields, ?ACL_TABLE)},
            {storage_properties, [{ets, [{read_concurrency, true}]}]}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?ACL_TABLE, disc_copies).

description() ->
    "AuthZ with Mnesia".

authorize(#{username := _Username,
            clientid := _Clientid
           } = _Client, _PubSub, _Topic, #{type := mnesia}) ->
    ok.
