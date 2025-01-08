%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
