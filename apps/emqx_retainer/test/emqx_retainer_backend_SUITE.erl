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

-module(emqx_retainer_backend_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() -> emqx_common_test_helpers:all(?MODULE).

-define(BASE_CONF, #{
    <<"retainer">> =>
        #{
            <<"enable">> => true,
            <<"backend">> =>
                #{
                    <<"type">> => <<"built_in_database">>,
                    <<"storage_type">> => <<"ram">>,
                    <<"max_retained_messages">> => 0
                }
        }
}).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_retainer, #{
                config => ?BASE_CONF,
                before_start => fun() ->
                    ok = emqx_schema_hooks:inject_from_modules([emqx_retainer_dummy])
                end
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_external_backend(_Config) ->
    {ok, _} = emqx_retainer:update_config(#{
        <<"enable">> => true,
        <<"backend">> => #{
            <<"enable">> => false
        },
        <<"external_backends">> =>
            #{
                <<"dummy">> => #{<<"enable">> => true}
            }
    }),
    ?assertMatch(
        ok,
        emqx_retainer:clean()
    ),
    ?assertMatch(
        ok,
        emqx_retainer:delete(<<"topic">>)
    ),
    ?assertMatch(
        {ok, []},
        emqx_retainer:read_message(<<"topic">>)
    ),
    ?assertMatch(
        {ok, false, []},
        emqx_retainer:page_read(<<"topic">>, 0, 10)
    ),
    ?assertEqual(
        0,
        emqx_retainer:retained_count()
    ),
    ?assertEqual(
        emqx_retainer_dummy,
        emqx_retainer:backend_module()
    ),
    ?assertEqual(
        true,
        emqx_retainer:enabled()
    ).
