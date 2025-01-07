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

%% @doc This module is used to cache the description of the configuration items.
-module(emqx_dashboard_desc_cache).

-export([init/0]).

%% internal exports
-export([load_desc/2, lookup/4, lookup/5]).

-include_lib("emqx/include/logger.hrl").

%% @doc Global ETS table to cache the description of the configuration items.
%% The table is owned by the emqx_dashboard_sup the root supervisor of emqx_dashboard.
%% The cache is initialized with the default language (English) and
%% all the desc.<lang>.hocon files in the app's priv directory
init() ->
    ok = ensure_app_loaded(emqx_dashboard),
    PrivDir = code:priv_dir(emqx_dashboard),
    Files0 = filelib:wildcard("desc.*.hocon", PrivDir),
    Files = lists:map(fun(F) -> filename:join([PrivDir, F]) end, Files0),
    ok = emqx_utils_ets:new(?MODULE, [public, ordered_set, {read_concurrency, true}]),
    ok = lists:foreach(fun(F) -> load_desc(?MODULE, F) end, Files).

%% @doc Load the description of the configuration items from the file.
%% Load is incremental, so it can be called multiple times.
%% NOTE: no garbage collection is done, because stale entries are harmless.
load_desc(EtsTab, File) ->
    ?SLOG(info, #{msg => "loading_desc", file => File}),
    {ok, Descs} = hocon:load(File),
    ["desc", Lang, "hocon"] = string:tokens(filename:basename(File), "."),
    Insert = fun(Namespace, Id, Tag, Text) ->
        Key = {bin(Lang), bin(Namespace), bin(Id), bin(Tag)},
        true = ets:insert(EtsTab, {Key, bin(Text)}),
        ok
    end,
    walk_ns(Insert, maps:to_list(Descs)).

%% @doc Lookup the description of the configuration item from the global cache.
lookup(Lang, Namespace, Id, Tag) ->
    lookup(?MODULE, Lang, Namespace, Id, Tag).

%% @doc Lookup the description of the configuration item from the given cache.
lookup(EtsTab, Lang0, Namespace, Id, Tag) ->
    Lang = bin(Lang0),
    try ets:lookup(EtsTab, {Lang, bin(Namespace), bin(Id), bin(Tag)}) of
        [{_, Desc}] ->
            Desc;
        [] when Lang =/= <<"en">> ->
            %% fallback to English
            lookup(EtsTab, <<"en">>, Namespace, Id, Tag);
        _ ->
            %% undefined but not <<>>
            undefined
    catch
        error:badarg ->
            %% schema is not initialized
            %% most likely in test cases
            undefined
    end.

%% The desc files are of names like:
%%   desc.en.hocon or desc.zh.hocon
%% And with content like:
%%   namespace.id.desc = "description"
%%   namespace.id.label = "label"
walk_ns(_Insert, []) ->
    ok;
walk_ns(Insert, [{Namespace, Ids} | Rest]) ->
    walk_id(Insert, Namespace, maps:to_list(Ids)),
    walk_ns(Insert, Rest).

walk_id(_Insert, _Namespace, []) ->
    ok;
walk_id(Insert, Namespace, [{Id, Tags} | Rest]) ->
    walk_tag(Insert, Namespace, Id, maps:to_list(Tags)),
    walk_id(Insert, Namespace, Rest).

walk_tag(_Insert, _Namespace, _Id, []) ->
    ok;
walk_tag(Insert, Namespace, Id, [{Tag, Text} | Rest]) ->
    ok = Insert(Namespace, Id, Tag, Text),
    walk_tag(Insert, Namespace, Id, Rest).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> list_to_binary(L).

ensure_app_loaded(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, _}} -> ok
    end.
