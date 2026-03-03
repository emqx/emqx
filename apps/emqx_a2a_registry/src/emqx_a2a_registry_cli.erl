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

-define(PRINT(Format, Args), io:format(Format, Args)).
-define(PRINT(Format), ?PRINT(Format, [])).

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
        }
    ]).

if_enabled(Fn) ->
    case emqx_a2a_registry_config:is_enabled() of
        false ->
            ?PRINT("A2A Registry is not enabled\n");
        true ->
            Fn()
    end.

handle_list(Args) ->
    case list_args(Args) of
        {ok, Opts} ->
            Cards0 = emqx_a2a_registry:list_cards(Opts),
            Cards = filter_by_status(Cards0, Opts),
            lists:foreach(fun print_card/1, Cards);
        {error, Reason} ->
            ?PRINT("Invalid list args: ~s", [Reason]),
            false
    end.

handle_get(Args) ->
    case get_args(Args) of
        {ok, Opts} ->
            Cards0 = emqx_a2a_registry:list_cards(Opts),
            case filter_by_status(Cards0, Opts) of
                [] ->
                    ?PRINT("Not found\n");
                [Card | _] ->
                    print_card(Card)
            end;
        {error, Reason} ->
            ?PRINT("Invalid get args: ~s", [Reason]),
            false
    end.

handle_delete(Args) ->
    case delete_args(Args) of
        {ok, Opts} ->
            ok = emqx_a2a_registry:delete_card(Opts),
            ok;
        {error, Reason} ->
            ?PRINT("Invalid get args: ~s", [Reason]),
            false
    end.

list_args(Args) ->
    case collect_args(Args, #{}) of
        {ok, Collected} ->
            Opts =
                maps:fold(
                    fun
                        ("--org-id", OrgId, Acc) ->
                            Acc#{org_id => OrgId};
                        ("--unit-id", UnitId, Acc) ->
                            Acc#{unit_id => UnitId};
                        ("--agent-id", AgentId, Acc) ->
                            Acc#{agent_id => AgentId};
                        ("--status", Status, Acc) ->
                            Acc#{status => Status};
                        (_, _, Acc) ->
                            Acc
                    end,
                    #{},
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
                            Acc#{org_id => OrgId};
                        ("--unit-id", UnitId, Acc) ->
                            Acc#{unit_id => UnitId};
                        ("--agent-id", AgentId, Acc) ->
                            Acc#{agent_id => AgentId};
                        (_, _, Acc) ->
                            Acc
                    end,
                    #{},
                    Collected
                ),
            {ok, Opts};
        {error, _} = Error ->
            Error
    end.

delete_args(Args) ->
    get_args(Args).

filter_by_status(Cards, #{status := Status} = _Opts) ->
    lists:filter(
        fun(Card) -> maps:get(<<"status">>, Card, undefined) == Status end,
        Cards
    );
filter_by_status(Cards, _Opts) ->
    Cards.

collect_args([], Map) ->
    {ok, Map};
%% list
collect_args(["--org-id", OrgId | Args], Map) ->
    case is_valid_segment_id(OrgId) of
        true ->
            collect_args(Args, Map#{"--org-id" => OrgId});
        false ->
            {error, io_lib:format("invalid org id: ~s", [OrgId])}
    end;
collect_args(["--unit-id", UnitId | Args], Map) ->
    case is_valid_segment_id(UnitId) of
        true ->
            collect_args(Args, Map#{"--unit-id" => UnitId});
        false ->
            {error, io_lib:format("invalid unit id: ~s", [UnitId])}
    end;
collect_args(["--agent-id", AgentId | Args], Map) ->
    case is_valid_segment_id(AgentId) of
        true ->
            collect_args(Args, Map#{"--agent-id" => AgentId});
        false ->
            {error, io_lib:format("invalid agent id: ~s", [AgentId])}
    end;
collect_args(["--status", Status0 | Args], Map) ->
    Status = list_to_binary(Status0),
    case lists:member(Status, [?A2A_PROP_ONLINE_VAL, ?A2A_PROP_OFFLINE_VAL]) of
        true ->
            collect_args(Args, Map#{"--status" => Status});
        false ->
            {error, io_lib:format("invalid status: ~s", [Status])}
    end;
collect_args(["--migrate-to", MigrateTo | Args], Map) ->
    collect_args(Args, Map#{"--migrate-to" => MigrateTo});
%% fallback
collect_args(Args, _Map) ->
    {error, io_lib:format("unknown arguments: ~p", [Args])}.

collect_get_args([OrgId, UnitId, AgentId | Rest], Map) ->
    %% Converting positional arguments to named ones
    collect_args(["--org-id", OrgId, "--unit-id", UnitId, "--agent-id", AgentId | Rest], Map);
collect_get_args(Args, _Map) ->
    {error, io_lib:format("unknown arguments: ~p", [Args])}.

is_valid_segment_id(Id0) ->
    Id = list_to_binary(Id0),
    case re:run(Id, ?SEGMENT_ID_RE, [{capture, none}]) of
        match ->
            true;
        nomatch ->
            false
    end.

print_card(Card) ->
    #{
        <<"name">> := Name,
        <<"description">> := Description,
        <<"status">> := Status
    } = Card,
    ?PRINT(
        "Name: ~s\n"
        "Description: ~s\n"
        "Status: ~s\n\n",
        [Name, Description, Status]
    ).
