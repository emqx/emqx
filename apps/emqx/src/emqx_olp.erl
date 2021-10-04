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
-module(emqx_olp).

-export([ is_overloaded/0
        , backoff/1
        , backoff_gc/1
        , backoff_hibernation/1
        ]).


%% exports for O&M
-export([ status/0
        , on/0
        , off/0
        ]).

-spec is_overloaded() -> boolean().
is_overloaded() ->
  load_ctl:is_overloaded().

-spec backoff(Zone :: atom()) -> ok | timeout.
backoff(Zone) ->
  case emqx_config:get_zone_conf(Zone, [overload_protection, enable], false) of
    true ->
      Delay = emqx_config:get_zone_conf(Zone, [overload_protection, backoff_delay], 1),
      load_ctl:maydelay(Delay);
    false ->
      ok
  end.

-spec backoff_gc(Zone :: atom()) -> ok | timeout.
backoff_gc(Zone) ->
  load_ctl:is_overloaded()
    andalso emqx_config:get_zone_conf(Zone, [overload_protection, enable], false)
    andalso emqx_config:get_zone_conf(Zone, [overload_protection, backoff_gc], false).

-spec backoff_hibernation(Zone :: atom()) -> ok | timeout.
backoff_hibernation(Zone) ->
  load_ctl:is_overloaded()
    andalso emqx_config:get_zone_conf(Zone, [overload_protection, enable], false)
    andalso emqx_config:get_zone_conf(Zone, [overload_protection, backoff_hibernation], false).

-spec status() -> any().
status() ->
  is_overloaded().

-spec off() -> ok | {error, timeout}.
off() ->
  load_ctl:stop_runq_flagman(5000).

-spec on() -> any().
on() ->
 load_ctl:restart_runq_flagman().

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
