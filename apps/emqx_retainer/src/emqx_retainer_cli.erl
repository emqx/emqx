%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_retainer_cli).

-include_lib("emqx/include/logger.hrl").

-export([load/0, retainer/1, unload/0]).

-define(PRINT_MSG(Msg), io:format(Msg)).

-define(PRINT(Format, Args), io:format(Format, Args)).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

load() ->
    ok = emqx_ctl:register_command(retainer, {?MODULE, retainer}, []).

retainer(["info"]) ->
    if_enabled(fun() ->
        count()
    end);
retainer(["topics"]) ->
    if_enabled(fun() ->
        topic(1, 1000)
    end);
retainer(["topics", Start, Len]) ->
    if_enabled(fun() ->
        topic(list_to_integer(Start), list_to_integer(Len))
    end);
retainer(["clean", Topic]) ->
    if_enabled(fun() ->
        emqx_retainer:delete(list_to_binary(Topic))
    end);
retainer(["clean"]) ->
    if_enabled(fun() ->
        emqx_retainer:clean()
    end);
retainer(["reindex", "status"]) ->
    if_mnesia_enabled(fun() ->
        case emqx_retainer_mnesia:reindex_status() of
            true ->
                ?PRINT_MSG("Reindexing is in progress~n");
            false ->
                ?PRINT_MSG("Reindexing is not running~n")
        end
    end);
retainer(["reindex", "start"]) ->
    if_mnesia_enabled(fun() ->
        retainer(["reindex", "start", "false"])
    end);
retainer(["reindex", "start", ForceParam]) ->
    if_mnesia_enabled(fun() ->
        case mria_rlog:role() of
            core ->
                Force =
                    case ForceParam of
                        "true" -> true;
                        _ -> false
                    end,
                do_reindex(Force);
            replicant ->
                ?PRINT_MSG("Can't run reindex on a replicant node")
        end
    end);
retainer(_) ->
    emqx_ctl:usage(
        [
            {"retainer info", "Show the count of retained messages"},
            {"retainer topics", "Same as retainer topic 1 1000"},
            {"retainer topics <Start> <Limit>",
                "Show topics of retained messages by the specified range"},
            {"retainer clean", "Clean all retained messages"},
            {"retainer clean <Topic>", "Clean retained messages by the specified topic filter"},
            {"retainer reindex status",
                "Show reindex status.\nOnly available for built-in backend."},
            {"retainer reindex start [force]",
                "Generate new retainer topic indices from config settings.\n"
                "Pass true as <Force> to ignore previously started reindexing.\n"
                "Only available for built-in backend."}
        ]
    ).

unload() ->
    ok = emqx_ctl:unregister_command(retainer).

%%------------------------------------------------------------------------------
%% Private
%%------------------------------------------------------------------------------

do_reindex(Force) ->
    ?PRINT_MSG("Starting reindexing~n"),
    emqx_retainer_mnesia:reindex(
        Force,
        fun(Done) ->
            ?SLOG(
                info,
                #{
                    msg => "retainer_message_record_reindexing_progress",
                    done => Done
                }
            ),
            ?PRINT("Reindexed ~p messages~n", [Done])
        end
    ),
    ?PRINT_MSG("Reindexing finished~n").

count() ->
    ?PRINT("Number of retained messages: ~p~n", [emqx_retainer:retained_count()]).

topic(Start, Len) ->
    count(),
    {ok, _HasNext, Messages} = emqx_retainer:page_read(undefined, Start, Len),
    [?PRINT("~ts~n", [emqx_message:topic(M)]) || M <- Messages],
    ok.

if_enabled(Fun) ->
    case emqx_retainer:enabled() of
        true -> Fun();
        false -> ?PRINT_MSG("Retainer is not enabled~n")
    end.

if_mnesia_backend(Fun) ->
    case emqx_retainer:backend_module() of
        emqx_retainer_mnesia -> Fun();
        _ -> ?PRINT_MSG("Command only applicable for builtin backend~n")
    end.

if_mnesia_enabled(Fun) ->
    if_enabled(fun() ->
        if_mnesia_backend(Fun)
    end).
