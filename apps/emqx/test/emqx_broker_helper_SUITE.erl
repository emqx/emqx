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

-module(emqx_broker_helper_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    emqx_broker_helper:start_link(),
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

t_lookup_subid(_) ->
    ?assertEqual(undefined, emqx_broker_helper:lookup_subid(self())),
    emqx_broker_helper:register_sub(self(), <<"clientid">>),
    ct:sleep(10),
    ?assertEqual(<<"clientid">>, emqx_broker_helper:lookup_subid(self())).

t_lookup_subpid(_) ->
    ?assertEqual(undefined, emqx_broker_helper:lookup_subpid(<<"clientid">>)),
    emqx_broker_helper:register_sub(self(), <<"clientid">>),
    ct:sleep(10),
    ?assertEqual(self(), emqx_broker_helper:lookup_subpid(<<"clientid">>)).
    
t_register_sub(_) ->
    ok = emqx_broker_helper:register_sub(self(), <<"clientid">>),
    ct:sleep(10),
    ok = emqx_broker_helper:register_sub(self(), <<"clientid">>),
    try emqx_broker_helper:register_sub(self(), <<"clientid2">>) of
        _ -> ct:fail(should_throw_error)
    catch error:Reason ->
        ?assertEqual(Reason, subid_conflict)
    end,
    ?assertEqual(self(), emqx_broker_helper:lookup_subpid(<<"clientid">>)).

t_shard_seq(_) ->
    ?assertEqual([], ets:lookup(emqx_subseq, <<"topic">>)),
    emqx_broker_helper:create_seq(<<"topic">>),
    ?assertEqual([{<<"topic">>, 1}], ets:lookup(emqx_subseq, <<"topic">>)),
    emqx_broker_helper:reclaim_seq(<<"topic">>),
    ?assertEqual([], ets:lookup(emqx_subseq, <<"topic">>)).

t_shards_num(_) ->
    ?assertEqual(emqx_vm:schedulers() * 32, emqx_broker_helper:shards_num()).
    
t_get_sub_shard(_) ->
    ?assertEqual(0, emqx_broker_helper:get_sub_shard(self(), <<"topic">>)).

t_terminate(_) ->
    ?assertEqual(ok, gen_server:stop(emqx_broker_helper)).

t_uncovered_func(_) ->
    gen_server:call(emqx_broker_helper, test),
    gen_server:cast(emqx_broker_helper, test),
    emqx_broker_helper ! test.