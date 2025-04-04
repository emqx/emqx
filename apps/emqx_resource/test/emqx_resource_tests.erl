%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_tests).

-include_lib("eunit/include/eunit.hrl").
-include("emqx_resource.hrl").

is_dry_run_test_() ->
    [
        ?_assert(emqx_resource:is_dry_run(?PROBE_ID_NEW())),
        ?_assertNot(emqx_resource:is_dry_run("foobar")),
        ?_assert(emqx_resource:is_dry_run(bin([?PROBE_ID_NEW(), "_abc"]))),
        ?_assert(
            emqx_resource:is_dry_run(
                bin(["action:typeA:", ?PROBE_ID_NEW(), ":connector:typeB:dryrun"])
            )
        ),
        ?_assertNot(
            emqx_resource:is_dry_run(
                bin(["action:type1:dryrun:connector:typeb:dryrun"])
            )
        )
    ].

bin(X) -> iolist_to_binary(X).
