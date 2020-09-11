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

-module(emqx_tables).

-export([ new/1
        , new/2
        ]).

-export([ lookup_value/2
        , lookup_value/3
        ]).

-export([delete/1]).

%% Create an ets table.
-spec(new(atom()) -> ok).
new(Tab) ->
    new(Tab, []).

%% Create a named_table ets.
-spec(new(atom(), list()) -> ok).
new(Tab, Opts) ->
    case ets:info(Tab, name) of
        undefined ->
            _ = ets:new(Tab, lists:usort([named_table | Opts])),
            ok;
        Tab -> ok
    end.

%% KV lookup
-spec(lookup_value(ets:tab(), term()) -> any()).
lookup_value(Tab, Key) ->
    lookup_value(Tab, Key, undefined).

-spec(lookup_value(ets:tab(), term(), any()) -> any()).
lookup_value(Tab, Key, Def) ->
    try ets:lookup_element(Tab, Key, 2)
    catch
        error:badarg -> Def
    end.

%% Delete the ets table.
-spec(delete(ets:tab()) -> ok).
delete(Tab) ->
    case ets:info(Tab, name) of
        undefined -> ok;
        Tab ->
            ets:delete(Tab),
            ok
    end.

