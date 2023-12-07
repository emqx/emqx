%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_sequence).

-include("emqx_persistent_session_ds.hrl").

-export([next/1]).

-type name() :: term().

%% Get the next sequence number for a given name.
%% Should be called inside a transaction.
-spec next(name()) -> integer().
next(Name) ->
    case mnesia:read(?SESSION_SEQUENCE_TAB, Name, write) of
        [#ds_sequence{next = Next}] ->
            write(Name, Next + 1),
            Next;
        [] ->
            write(Name, 1),
            0
    end.

write(Name, Next) ->
    mnesia:write(?SESSION_SEQUENCE_TAB, #ds_sequence{name = Name, next = Next}, write).
