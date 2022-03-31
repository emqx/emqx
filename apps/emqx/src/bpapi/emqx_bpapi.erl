%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bpapi).

%% API:
-export([
    start/0,
    announce/1,
    supported_version/1, supported_version/2,
    versions_file/1
]).

-export_type([api/0, api_version/0, var_name/0, call/0, rpc/0, bpapi_meta/0]).

-include("emqx.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-type api() :: atom().
-type api_version() :: non_neg_integer().
-type var_name() :: atom().
-type call() :: {module(), atom(), [var_name()]}.
-type rpc() :: {_From :: call(), _To :: call()}.

-type bpapi_meta() ::
    #{
        api := api(),
        version := api_version(),
        calls := [rpc()],
        casts := [rpc()]
    }.

-include("emqx_bpapi.hrl").

-callback introduced_in() -> string().

-callback deprecated_since() -> string().

-callback bpapi_meta() -> bpapi_meta().

-optional_callbacks([deprecated_since/0]).

-spec start() -> ok.
start() ->
    ok = mria:create_table(?TAB, [
        {type, set},
        {storage, ram_copies},
        {attributes, record_info(fields, ?TAB)},
        {rlog_shard, ?COMMON_SHARD}
    ]),
    ok = mria:wait_for_tables([?TAB]),
    announce(emqx).

%% @doc Get maximum version of the backplane API supported by the node
-spec supported_version(node(), api()) -> api_version().
supported_version(Node, API) ->
    ets:lookup_element(?TAB, {Node, API}, #?TAB.version).

%% @doc Get maximum version of the backplane API supported by the
%% entire cluster
-spec supported_version(api()) -> api_version().
supported_version(API) ->
    ets:lookup_element(?TAB, {?multicall, API}, #?TAB.version).

-spec announce(atom()) -> ok.
announce(App) ->
    {ok, Data} = file:consult(?MODULE:versions_file(App)),
    {atomic, ok} = mria:transaction(?COMMON_SHARD, fun announce_fun/1, [Data]),
    ok.

-spec versions_file(atom()) -> file:filename_all().
versions_file(App) ->
    filename:join(code:priv_dir(App), "bpapi.versions").

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-spec announce_fun([{api(), api_version()}]) -> ok.
announce_fun(Data) ->
    %% Delete old records, if present:
    MS = ets:fun2ms(fun(#?TAB{key = {node(), API}}) ->
        {node(), API}
    end),
    OldKeys = mnesia:select(?TAB, MS, write),
    _ = [
        mnesia:delete({?TAB, Key})
     || Key <- OldKeys
    ],
    %% Insert new records:
    _ = [
        mnesia:write(#?TAB{key = {node(), API}, version = Version})
     || {API, Version} <- Data
    ],
    %% Update maximum supported version:
    [update_minimum(API) || {API, _} <- Data],
    ok.

-spec update_minimum(api()) -> ok.
update_minimum(API) ->
    MS = ets:fun2ms(fun(
        #?TAB{
            key = {N, A},
            version = Value
        }
    ) when
        N =/= ?multicall,
        A =:= API
    ->
        Value
    end),
    MinVersion = lists:min(mnesia:select(?TAB, MS)),
    mnesia:write(#?TAB{key = {?multicall, API}, version = MinVersion}).
