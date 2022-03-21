%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("lc/include/lc.hrl").

-export([
    is_overloaded/0,
    backoff/1,
    backoff_gc/1,
    backoff_hibernation/1,
    backoff_new_conn/1
]).

%% exports for O&M
-export([
    status/0,
    enable/0,
    disable/0
]).

-type cfg_key() ::
    backoff_gc
    | backoff_hibernation
    | backoff_new_conn.

-type cnt_name() ::
    'olp.delay.ok'
    | 'olp.delay.timeout'
    | 'olp.hbn'
    | 'olp.gc'
    | 'olp.new_conn'.

-define(overload_protection, overload_protection).

%% @doc Light realtime check if system is overloaded.
-spec is_overloaded() -> boolean().
is_overloaded() ->
    load_ctl:is_overloaded().

%% @doc Backoff with a delay if the system is overloaded, for tasks that could be deferred.
%%      returns `false' if backoff didn't happen, the system is cool.
%%      returns `ok' if backoff is triggered and get unblocked when the system is cool.
%%      returns `timeout' if backoff is triggered but get unblocked due to timeout as configured.
-spec backoff(Zone :: atom()) -> ok | false | timeout.
backoff(Zone) ->
    case emqx_config:get_zone_conf(Zone, [?overload_protection]) of
        #{enable := true, backoff_delay := Delay} ->
            case load_ctl:maydelay(Delay) of
                false ->
                    false;
                ok ->
                    emqx_metrics:inc('olp.delay.ok'),
                    ok;
                timeout ->
                    emqx_metrics:inc('olp.delay.timeout'),
                    timeout
            end;
        _ ->
            ok
    end.

%% @doc If forceful GC should be skipped when the system is overloaded.
-spec backoff_gc(Zone :: atom()) -> boolean().
backoff_gc(Zone) ->
    do_check(Zone, ?FUNCTION_NAME, 'olp.gc').

%% @doc If hibernation should be skipped when the system is overloaded.
-spec backoff_hibernation(Zone :: atom()) -> boolean().
backoff_hibernation(Zone) ->
    do_check(Zone, ?FUNCTION_NAME, 'olp.hbn').

%% @doc Returns {error, overloaded} if new connection should be
%%      closed when system is overloaded.
-spec backoff_new_conn(Zone :: atom()) -> ok | {error, overloaded}.
backoff_new_conn(Zone) ->
    case do_check(Zone, ?FUNCTION_NAME, 'olp.new_conn') of
        true ->
            {error, overloaded};
        false ->
            ok
    end.

-spec status() -> any().
status() ->
    is_overloaded().

%% @doc turn off background runq check.
-spec disable() -> ok | {error, timeout}.
disable() ->
    load_ctl:stop_runq_flagman(5000).

%% @doc turn on background runq check.
-spec enable() -> {ok, pid()} | {error, running | restarting | disabled}.
enable() ->
    case load_ctl:restart_runq_flagman() of
        {error, disabled} ->
            OldCfg = load_ctl:get_config(),
            ok = load_ctl:put_config(OldCfg#{?RUNQ_MON_F0 => true}),
            load_ctl:restart_runq_flagman();
        Other ->
            Other
    end.

%%% Internals
-spec do_check(Zone :: atom(), cfg_key(), cnt_name()) -> boolean().
do_check(Zone, Key, CntName) ->
    case load_ctl:is_overloaded() of
        true ->
            case emqx_config:get_zone_conf(Zone, [?overload_protection]) of
                #{enable := true, Key := true} ->
                    emqx_metrics:inc(CntName),
                    true;
                _ ->
                    false
            end;
        false ->
            false
    end.

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
