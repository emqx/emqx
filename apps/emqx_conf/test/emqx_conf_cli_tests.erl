%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_cli_tests).

-include("emqx_conf.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TARGET_NODE, <<"target">>).
-define(OTHER_NODE, <<"other">>).

cluster_sync_status_prunes_raw_only_diffs_test_() ->
    {setup, fun setup_mocks/0, fun teardown_mocks/1, fun(_) ->
        fun() ->
            setup_cluster_sync_status_raw_diff(),
            ?assertEqual(ok, emqx_conf_cli:admins(["status"])),
            Output = ctl_output(),
            ?assertNotEqual(nomatch, binary:match(Output, <<"changed">>)),
            ?assertEqual(nomatch, binary:match(Output, <<"same">>)),
            ?assertEqual(nomatch, binary:match(Output, <<"enable">>))
        end
    end}.

setup_mocks() ->
    Modules = [emqx_bpapi, emqx_cluster_rpc, emqx_conf_proto_v5, emqx_ctl, emqx_mgmt_cli],
    lists:foreach(
        fun(Module) -> ok = meck:new(Module, [passthrough, no_link]) end,
        Modules
    ),
    Modules.

teardown_mocks(Modules) ->
    meck:unload(Modules).

setup_cluster_sync_status_raw_diff() ->
    ok = meck:expect(
        emqx_cluster_rpc,
        status,
        fun() ->
            {atomic, [
                #{node => ?TARGET_NODE, tnx_id => 1},
                #{node => ?OTHER_NODE, tnx_id => 1}
            ]}
        end
    ),
    ok = meck:expect(
        emqx_mgmt_cli,
        cluster_info,
        fun() -> #{stopped_nodes => []} end
    ),
    ok = meck:expect(
        emqx_bpapi,
        nodes_supporting_bpapi_version,
        fun(emqx_conf, 5) -> [?TARGET_NODE, ?OTHER_NODE] end
    ),
    ok = meck:expect(
        emqx_conf_proto_v5,
        get_config,
        fun
            (?TARGET_NODE, ?global_ns, []) -> checked_conf(<<"select new">>);
            (?OTHER_NODE, ?global_ns, []) -> checked_conf(<<"select old">>)
        end
    ),
    ok = meck:expect(
        emqx_conf_proto_v5,
        get_raw_config,
        fun
            (?TARGET_NODE, ?global_ns, []) ->
                invalid_raw_conf;
            (?OTHER_NODE, ?global_ns, []) ->
                invalid_raw_conf;
            (?TARGET_NODE, ?global_ns, [rule_engine]) ->
                raw_conf(#{}, <<"select new">>);
            (?OTHER_NODE, ?global_ns, [rule_engine]) ->
                raw_conf(#{<<"enable">> => true}, <<"select old">>)
        end
    ),
    ok.

checked_conf(Sql) ->
    #{
        rule_engine => #{
            rules => #{
                <<"same">> => #{enable => true},
                <<"changed">> => #{sql => Sql}
            }
        }
    }.

raw_conf(SameRuleRaw, Sql) ->
    #{
        <<"rules">> => #{
            <<"same">> => SameRuleRaw,
            <<"changed">> => #{<<"sql">> => Sql}
        }
    }.

ctl_output() ->
    unicode:characters_to_binary([
        format_ctl_output(Fun, Args)
     || {_Pid, {emqx_ctl, Fun, Args}, _Result} <- meck:history(emqx_ctl),
        Fun =:= print orelse Fun =:= warning
    ]).

format_ctl_output(_Fun, [Format]) ->
    Format;
format_ctl_output(_Fun, [Format, Args]) ->
    io_lib:format(Format, Args);
format_ctl_output(_Fun, Args) ->
    io_lib:format("~p", [Args]).
