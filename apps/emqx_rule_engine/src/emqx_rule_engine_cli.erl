%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_engine_cli).

%% API:
-export([load/0, unload/0]).

-export([cmd/1]).

-include_lib("emqx/include/emqx_config.hrl").

%%================================================================================
%% Type definitions
%%================================================================================

-define(id, id).
-define(namespace, namespace).
-define(single_arg, single_arg).

%%================================================================================
%% API functions
%%================================================================================

load() ->
    ok = emqx_ctl:register_command(rules, {?MODULE, cmd}, []).

unload() ->
    ok = emqx_ctl:unregister_command(rules).

%%================================================================================
%% Internal exports
%%================================================================================

cmd(["list" | Args]) ->
    case list_args(Args) of
        {ok, #{?namespace := Namespace}} ->
            lists:foreach(
                fun pretty_print_rule_summary/1,
                emqx_rule_engine:get_rules_ordered_by_ts(Namespace)
            );
        {error, Reason} = Error ->
            emqx_ctl:warning("~ts~n", [Reason]),
            show_usage(),
            Error
    end;
cmd(["show" | Args]) ->
    case show_args(Args) of
        {ok, #{?id := Id, ?namespace := Namespace}} ->
            pretty_print_rule(Namespace, Id);
        {error, Reason} = Error ->
            emqx_ctl:warning("~ts~n", [Reason]),
            show_usage(),
            Error
    end;
cmd(_) ->
    show_usage().

%%================================================================================
%% Internal functions
%%================================================================================

show_usage() ->
    NamespaceOptLine = {"", "Set `--namespace <ns>` to narrow the scope to a specific namespace."},
    emqx_ctl:usage(
        [
            {"rules list [--namespace <ns>]", "List rules"},
            NamespaceOptLine,
            {"rules show [--namespace <ns>] <RuleId>", "Show a rule"},
            NamespaceOptLine
        ]
    ).

collect_args(Args) ->
    do_collect_args(Args, #{}).

do_collect_args([], Acc) ->
    {ok, Acc};
do_collect_args([SingleArg], Acc) when SingleArg /= "--namespace" ->
    do_collect_args([], Acc#{?single_arg => bin(SingleArg)});
do_collect_args(["--namespace", Namespace | Rest], Acc) ->
    do_collect_args(Rest, Acc#{"--namespace" => bin(Namespace)});
do_collect_args(Args, _Acc) ->
    {error, lists:flatten(io_lib:format("bad arguments: ~p", [Args]))}.

list_args(Args) ->
    maybe
        {ok, Collected} ?= collect_args(Args),
        Namespace = maps:get("--namespace", Collected, ?global_ns),
        {ok, #{?namespace => Namespace}}
    end.

show_args(Args) ->
    maybe
        {ok, Collected} ?= collect_args(Args),
        #{?single_arg := Id} = Collected,
        Namespace = maps:get("--namespace", Collected, ?global_ns),
        {ok, #{?id => Id, ?namespace => Namespace}}
    end.

pretty_print_rule_summary(#{id := Id, name := Name, enable := Enable, description := Desc}) ->
    emqx_ctl:print("Rule{id=~ts, name=~ts, enabled=~ts, descr=~ts}\n", [
        Id, Name, Enable, Desc
    ]).

%% erlfmt-ignore
pretty_print_rule(Namespace, Id0) ->
    case emqx_rule_engine:get_rule(Namespace, Id0) of
        {ok, #{id := Id, name := Name, description := Descr, enable := Enable,
               sql := SQL, created_at := CreatedAt, updated_at := UpdatedAt,
               actions := Actions}} ->
            emqx_ctl:print(
              "Id:\n  ~ts\n"
              "Name:\n  ~ts\n"
              "Description:\n  ~ts\n"
              "Enabled:\n  ~ts\n"
              "SQL:\n  ~ts\n"
              "Created at:\n  ~ts\n"
              "Updated at:\n  ~ts\n"
              "Actions:\n  ~s\n"
             ,[Id, Name, left_pad(Descr), Enable, left_pad(SQL),
               emqx_utils_calendar:epoch_to_rfc3339(CreatedAt, millisecond),
               emqx_utils_calendar:epoch_to_rfc3339(UpdatedAt, millisecond),
               [left_pad(format_action(A)) || A <- Actions]
              ]
             );
        _ ->
            ok
    end.

%% erlfmt-ignore
format_action(#{mod := Mod, func := Func, args := Args}) ->
    Name = emqx_rule_engine_api:printable_function_name(Mod, Func),
    io_lib:format("- Name:  ~s\n"
                  "  Type:  function\n"
                  "  Args:  ~p\n"
                 ,[Name, maps:without([preprocessed_tmpl], Args)]
                 );
format_action(BridgeChannelId) when is_binary(BridgeChannelId) ->
    io_lib:format("- Name:  ~s\n"
                  "  Type:  data-bridge\n"
                 ,[BridgeChannelId]
                 );
format_action({bridge, ActionType, ActionName, _Id}) ->
    io_lib:format("- Name:         ~p\n"
                  "  Action Type:  ~p\n"
                  "  Type:         data-bridge\n"
                 ,[ActionName, ActionType]
                 );
format_action({bridge_v2, ActionType, ActionName}) ->
    io_lib:format("- Name:         ~p\n"
                  "  Action Type:  ~p\n"
                  "  Type:         data-bridge\n"
                 ,[ActionName, ActionType]
                 ).

left_pad(Str) ->
    re:replace(Str, "\n", "\n  ", [global]).

bin(X) -> emqx_utils_conv:bin(X).
