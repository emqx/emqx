%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_cli).

%% API
-export([
    load/0,
    unload/0,

    a2a/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_a2a_registry_internal.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(PRINT(Format, Args), io:format(Format, Args)).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

load() ->
    ok = emqx_ctl:register_command(a2a_registry, {?MODULE, a2a}, []),
    ok.

unload() ->
    ok = emqx_ctl:unregister_command(a2a_registry),
    ok.

a2a(["list" | Args]) ->
    if_enabled(fun() -> handle_list(Args) end);
a2a(["get" | Args]) ->
    if_enabled(fun() -> handle_get(Args) end);
a2a(["delete" | Args]) ->
    if_enabled(fun() -> handle_delete(Args) end);
a2a(["register" | Args]) ->
    if_enabled(fun() -> handle_register(Args) end);
a2a(["stats" | Args]) ->
    if_enabled(fun() -> handle_stats(Args) end);
a2a(_) ->
    usage().

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

usage() ->
    emqx_ctl:usage([
        {
            "a2a_registry list \\\n"
            "  [--org-id ORG_ID] \\\n"
            "  [--unit-id UNIT_ID] \\\n"
            "  [--agent-id AGENT_ID] \\\n"
            "  [--status STATUS]",
            "List registered agent cards"
        },
        {"a2a_registry get ORG_ID UNIT_ID AGENT_ID", "Lookup one registered agent card"},
        {"a2a_registry delete ORG_ID UNIT_ID AGENT_ID", "Delete one registered agent card"},
        {"a2a_registry register ORG_ID UNIT_ID AGENT_ID <agent-card.json>",
            "Insert/update agent card"},
        {"a2a_registry stats", "Print agent card registry statistics"}
    ]).

if_enabled(Fn) ->
    case emqx_a2a_registry_config:is_enabled() of
        false ->
            Error = mk_error(<<"A2A Registry is not enabled">>),
            print_json(Error);
        true ->
            Fn()
    end.

handle_list(Args) ->
    case list_args(Args) of
        {ok, Opts} ->
            Cards0 = emqx_a2a_registry:list_cards(Opts),
            Cards1 = filter_by_status(Cards0, Opts),
            Cards = lists:map(fun emqx_a2a_registry_adapter:card_out/1, Cards1),
            print_json(Cards);
        {error, Reason} ->
            Error = mk_error(fmt("Invalid list args: ~ts\n", [Reason])),
            print_json(Error),
            false
    end.

handle_get(Args) ->
    case get_args(Args) of
        {ok, Opts} ->
            Cards0 = emqx_a2a_registry:list_cards(Opts),
            case filter_by_status(Cards0, Opts) of
                [] ->
                    print_json(null);
                [Card0 | _] ->
                    Card = emqx_a2a_registry_adapter:card_out(Card0),
                    print_json(Card)
            end;
        {error, Reason} ->
            Error = mk_error(fmt("Invalid get args: ~ts\n", [Reason])),
            print_json(Error),
            false
    end.

handle_delete(Args) ->
    case delete_args(Args) of
        {ok, Opts} ->
            ok = emqx_a2a_registry:delete_card(Opts),
            ok;
        {error, Reason} ->
            Error = mk_error(fmt("Invalid delete args: ~ts\n", [Reason])),
            print_json(Error),
            false
    end.

handle_register(Args) ->
    case register_args(Args) of
        {ok, Opts0} ->
            #{filepath := Filepath} = Opts0,
            maybe
                {ok, CardBin} ?= read_card_file(Filepath),
                Opts1 = maps:remove(filepath, Opts0),
                Opts = Opts1#{card_bin => CardBin},
                ok ?= emqx_a2a_registry:write_card(Opts)
            else
                {error, Reason0} ->
                    case emqx_a2a_registry_adapter:format_register_error(Reason0) of
                        {ok, Msg} ->
                            Error = mk_error(Msg),
                            print_json(Error),
                            false;
                        error ->
                            Reason = iolist_to_binary(io_lib:format("~0p", [Reason0])),
                            Error = mk_error(fmt("Invalid register args: ~ts\n", [Reason])),
                            print_json(Error),
                            false
                    end
            end;
        {error, Reason} when is_binary(Reason) ->
            Error = mk_error(Reason),
            print_json(Error),
            false;
        {error, Reason0} ->
            Reason = iolist_to_binary(io_lib:format("~0p", [Reason0])),
            Error = mk_error(fmt("Invalid register args: ~ts\n", [Reason])),
            print_json(Error),
            false
    end.

handle_stats(Args) ->
    case stats_args(Args) of
        {ok, #{namespace := Namespace}} ->
            AllCards = emqx_a2a_registry:list_cards(#{namespace => Namespace}),
            NumAllCards = length(AllCards),
            Stats = #{<<"total">> => NumAllCards},
            print_json(Stats);
        {error, Reason} when is_binary(Reason) ->
            Error = mk_error(Reason),
            print_json(Error),
            false
    end.

read_card_file(Filepath) ->
    maybe
        {error, Reason} ?= file:read_file(Filepath),
        {error, emqx_utils:explain_posix(Reason)}
    end.

list_args(Args) ->
    case collect_args(Args, #{}) of
        {ok, Collected} ->
            Opts =
                maps:fold(
                    fun
                        ("--org-id", OrgId, Acc) ->
                            Acc#{org_id => bin(OrgId)};
                        ("--unit-id", UnitId, Acc) ->
                            Acc#{unit_id => bin(UnitId)};
                        ("--agent-id", AgentId, Acc) ->
                            Acc#{agent_id => bin(AgentId)};
                        ("--status", Status, Acc) ->
                            Acc#{status => bin(Status)};
                        ("--namespace", Namespace, Acc) ->
                            Acc#{namespace => bin(Namespace)};
                        (_, _, Acc) ->
                            Acc
                    end,
                    #{namespace => ?global_ns},
                    Collected
                ),
            {ok, Opts};
        {error, _} = Error ->
            Error
    end.

get_args(Args) ->
    case collect_get_args(Args, #{}) of
        {ok, Collected} ->
            Opts =
                maps:fold(
                    fun
                        ("--org-id", OrgId, Acc) ->
                            Acc#{org_id => bin(OrgId)};
                        ("--unit-id", UnitId, Acc) ->
                            Acc#{unit_id => bin(UnitId)};
                        ("--agent-id", AgentId, Acc) ->
                            Acc#{agent_id => bin(AgentId)};
                        ("--namespace", Namespace, Acc) ->
                            Acc#{namespace => bin(Namespace)};
                        (_, _, Acc) ->
                            Acc
                    end,
                    #{namespace => ?global_ns},
                    Collected
                ),
            {ok, Opts};
        {error, _} = Error ->
            Error
    end.

delete_args(Args) ->
    get_args(Args).

register_args(Args) ->
    case collect_register_args(Args, #{}) of
        {ok, Collected} ->
            Opts =
                maps:fold(
                    fun
                        ("--org-id", OrgId, Acc) ->
                            Acc#{org_id => bin(OrgId)};
                        ("--unit-id", UnitId, Acc) ->
                            Acc#{unit_id => bin(UnitId)};
                        ("--agent-id", AgentId, Acc) ->
                            Acc#{agent_id => bin(AgentId)};
                        ("--file", Filepath, Acc) ->
                            Acc#{filepath => bin(Filepath)};
                        ("--namespace", Namespace, Acc) ->
                            Acc#{namespace => bin(Namespace)};
                        (_, _, Acc) ->
                            Acc
                    end,
                    #{namespace => ?global_ns},
                    Collected
                ),
            {ok, Opts};
        {error, _} = Error ->
            Error
    end.

stats_args(Args) ->
    case collect_stats_args(Args, #{}) of
        {ok, Collected} ->
            Opts =
                maps:fold(
                    fun
                        ("--namespace", Namespace, Acc) ->
                            Acc#{namespace => bin(Namespace)};
                        (_, _, Acc) ->
                            Acc
                    end,
                    #{namespace => ?global_ns},
                    Collected
                ),
            {ok, Opts};
        {error, _} = Error ->
            Error
    end.

filter_by_status(Cards, #{status := Status} = _Opts) ->
    lists:filter(
        fun(Card) -> maps:get(<<"status">>, Card, undefined) == Status end,
        Cards
    );
filter_by_status(Cards, _Opts) ->
    Cards.

collect_args([], Map) ->
    {ok, Map};
%% common
collect_args(["--namespace", Namespace | Args], Map) ->
    collect_args(Args, Map#{"--namespace" => Namespace});
%% list
collect_args(["--org-id", OrgId | Args], Map) ->
    case is_valid_segment_id(OrgId) of
        true ->
            collect_args(Args, Map#{"--org-id" => OrgId});
        false ->
            {error, io_lib:format("invalid org id: ~ts", [OrgId])}
    end;
collect_args(["--unit-id", UnitId | Args], Map) ->
    case is_valid_segment_id(UnitId) of
        true ->
            collect_args(Args, Map#{"--unit-id" => UnitId});
        false ->
            {error, io_lib:format("invalid unit id: ~ts", [UnitId])}
    end;
collect_args(["--agent-id", AgentId | Args], Map) ->
    case is_valid_segment_id(AgentId) of
        true ->
            collect_args(Args, Map#{"--agent-id" => AgentId});
        false ->
            {error, io_lib:format("invalid agent id: ~ts", [AgentId])}
    end;
collect_args(["--status", Status0 | Args], Map) ->
    Status = list_to_binary(Status0),
    case lists:member(Status, [?A2A_PROP_ONLINE_VAL, ?A2A_PROP_OFFLINE_VAL]) of
        true ->
            collect_args(Args, Map#{"--status" => Status});
        false ->
            {error, io_lib:format("invalid status: ~ts", [Status])}
    end;
%% register
collect_args(["--file", Filepath | Args], Map) ->
    collect_args(Args, Map#{"--file" => Filepath});
%% fallback
collect_args(Args0, _Map) ->
    Args = format_args(Args0),
    {error, io_lib:format("unknown arguments: ~ts", [Args])}.

collect_get_args(["--namespace", Namespace, OrgId, UnitId, AgentId | Rest], Map) ->
    %% Converting positional arguments to named ones
    collect_args(
        [
            "--namespace",
            Namespace,
            "--org-id",
            OrgId,
            "--unit-id",
            UnitId,
            "--agent-id",
            AgentId
            | Rest
        ],
        Map
    );
collect_get_args([OrgId, UnitId, AgentId | Rest], Map) ->
    %% Converting positional arguments to named ones
    collect_args(["--org-id", OrgId, "--unit-id", UnitId, "--agent-id", AgentId | Rest], Map);
collect_get_args(Args0, _Map) ->
    Args = format_args(Args0),
    {error, io_lib:format("unknown arguments: ~ts", [Args])}.

collect_register_args(["--namespace", Namespace, OrgId, UnitId, AgentId, Filepath | Rest], Map) ->
    %% Converting positional arguments to named ones
    collect_args(
        [
            "--namespace",
            Namespace,
            "--org-id",
            OrgId,
            "--unit-id",
            UnitId,
            "--agent-id",
            AgentId,
            "--file",
            Filepath
            | Rest
        ],
        Map
    );
collect_register_args([OrgId, UnitId, AgentId, Filepath | Rest], Map) ->
    %% Converting positional arguments to named ones
    collect_args(
        [
            "--org-id",
            OrgId,
            "--unit-id",
            UnitId,
            "--agent-id",
            AgentId,
            "--file",
            Filepath
            | Rest
        ],
        Map
    );
collect_register_args(Args0, _Map) ->
    Args = format_args(Args0),
    Msg = fmt("unknown arguments: ~ts", [Args]),
    {error, Msg}.

collect_stats_args([], Map) ->
    {ok, Map};
collect_stats_args(["--namespace", Namespace | Args], Map) ->
    collect_args(Args, Map#{"--namespace" => Namespace});
collect_stats_args(Args0, _Map) ->
    Args = format_args(Args0),
    Msg = fmt("unknown arguments: ~ts", [Args]),
    {error, Msg}.

format_args(Args) ->
    lists:flatten(lists:join(" ", Args)).

is_valid_segment_id(Id0) ->
    Id = list_to_binary(Id0),
    case re:run(Id, ?SEGMENT_ID_RE, [{capture, none}]) of
        match ->
            true;
        nomatch ->
            false
    end.

bin(X) -> emqx_utils_conv:bin(X).
fmt(Fmt, Args) -> unicode:characters_to_binary(lists:flatten(io_lib:format(Fmt, Args))).

mk_error(Msg) ->
    #{<<"message">> => Msg, <<"error">> => true}.

print_json(X) ->
    ?PRINT("~ts\n", [emqx_utils_json:encode(X)]).
