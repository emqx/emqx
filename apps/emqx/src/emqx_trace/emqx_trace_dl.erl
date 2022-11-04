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

%% Data layer for emqx_trace
-module(emqx_trace_dl).

%% API:
-export([
    update/2,
    insert_new_trace/1,
    delete/1,
    get_trace_filename/1,
    delete_finished/1,
    get_enabled_trace/0
]).

-include("emqx_trace.hrl").

%%================================================================================
%% API functions
%%================================================================================

%% Introduced in 5.0
-spec update(Name :: binary(), Enable :: boolean()) ->
    ok.
update(Name, Enable) ->
    case mnesia:read(?TRACE, Name) of
        [] ->
            mnesia:abort(not_found);
        [#?TRACE{enable = Enable}] ->
            ok;
        [Rec] ->
            case emqx_trace:now_second() >= Rec#?TRACE.end_at of
                false -> mnesia:write(?TRACE, Rec#?TRACE{enable = Enable}, write);
                true -> mnesia:abort(finished)
            end
    end.

%% Introduced in 5.0
insert_new_trace(Trace) ->
    case mnesia:read(?TRACE, Trace#?TRACE.name) of
        [] ->
            #?TRACE{start_at = StartAt, type = Type, filter = Filter} = Trace,
            Match = #?TRACE{_ = '_', start_at = StartAt, type = Type, filter = Filter},
            case mnesia:match_object(?TRACE, Match, read) of
                [] ->
                    ok = mnesia:write(?TRACE, Trace, write),
                    {ok, Trace};
                [#?TRACE{name = Name}] ->
                    mnesia:abort({duplicate_condition, Name})
            end;
        [#?TRACE{name = Name}] ->
            mnesia:abort({already_existed, Name})
    end.

%% Introduced in 5.0
-spec delete(Name :: binary()) -> ok.
delete(Name) ->
    case mnesia:read(?TRACE, Name) of
        [_] -> mnesia:delete(?TRACE, Name, write);
        [] -> mnesia:abort(not_found)
    end.

%% Introduced in 5.0
-spec get_trace_filename(Name :: binary()) -> {ok, string()}.
get_trace_filename(Name) ->
    case mnesia:read(?TRACE, Name, read) of
        [] -> mnesia:abort(not_found);
        [#?TRACE{start_at = Start}] -> {ok, emqx_trace:filename(Name, Start)}
    end.

%% Introduced in 5.0
delete_finished(Traces) ->
    lists:map(
        fun(#?TRACE{name = Name}) ->
            case mnesia:read(?TRACE, Name, write) of
                [] -> ok;
                [Trace] -> mnesia:write(?TRACE, Trace#?TRACE{enable = false}, write)
            end
        end,
        Traces
    ).

%% Introduced in 5.0
get_enabled_trace() ->
    mnesia:match_object(?TRACE, #?TRACE{enable = true, _ = '_'}, read).

%%================================================================================
%% Internal functions
%%================================================================================
