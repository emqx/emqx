%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_registry_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).
-compile(nowarn_export_all).

all() -> emqx_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

init_per_suite(Cfg) ->
    emqx_ct_helpers:start_apps([emqx_gateway], fun set_special_configs/1),
    Cfg.

end_per_suite(_Cfg) ->
    emqx_ct_helpers:stop_apps([emqx_gateway]),
    ok.

set_special_configs(emqx_gateway) ->
    emqx_config:put(
      [emqx_gateway],
      #{stomp =>
        #{'1' =>
          #{authenticator => allow_anonymous,
            clientinfo_override =>
                #{password => "${Packet.headers.passcode}",
                  username => "${Packet.headers.login}"},
            frame =>
                #{max_body_length => 8192,
                  max_headers => 10,
                  max_headers_length => 1024},
            listener =>
                #{tcp =>
                  #{'1' =>
                    #{acceptors => 16,active_n => 100,backlog => 1024,
                      bind => 61613,high_watermark => 1048576,
                      max_conn_rate => 1000,max_connections => 1024000,
                      send_timeout => 15000,send_timeout_close => true}}}}}}),
    ok;
set_special_configs(_) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_load_unload(_) ->
    OldCnt = length(emqx_gateway_registry:list()),
    RgOpts = [{cbkmod, ?MODULE}],
    GwOpts = [paramsin],
    ok = emqx_gateway_registry:load(test, RgOpts, GwOpts),
    ?assertEqual(OldCnt+1, length(emqx_gateway_registry:list())),

    #{cbkmod := ?MODULE,
      rgopts := RgOpts,
      gwopts := GwOpts,
      state  := #{gwstate := 1}} = emqx_gateway_registry:lookup(test),

    {error, already_existed} = emqx_gateway_registry:load(test, [{cbkmod, ?MODULE}], GwOpts),

    ok = emqx_gateway_registry:unload(test),
    undefined = emqx_gateway_registry:lookup(test),
    OldCnt = length(emqx_gateway_registry:list()),
    ok.

init([paramsin]) ->
    {ok, _GwState = #{gwstate => 1}}.

