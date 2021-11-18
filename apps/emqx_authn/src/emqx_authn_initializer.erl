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

-module(emqx_authn_initializer).

-behaviour(gen_statem).

-define(DEFAULT_INTERVAL, 5).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% APIs
-export([start_link/0,
         start_link/1,
         initialize/0,
         deinitialize/0
        ]).

%% gen_statem callbacks
-export([init/1,
         callback_mode/0,
         handle_event/4,
         code_change/4
        ]).

-type(opts() :: #{name := atom(),
                  interval := non_neg_integer(),
                  init_fun := fun((pid()) -> any())}).

%%--------------------------------------------------------------------
%% External interface
%%--------------------------------------------------------------------

-spec(start_link() -> gen_statem:start_ret()).
start_link() ->
        start_link(
          #{name => ?AUTHN,
            interval => ?DEFAULT_INTERVAL,
            init_fun => fun(_) -> initialize() end}).

-spec(start_link(opts()) -> gen_statem:start_ret()).
start_link(#{} = Opts) ->
        gen_statem:start_link(?MODULE, Opts, []).

-spec(initialize() -> ok).
initialize() ->
    _ = ?AUTHN:register_providers(emqx_authn:providers()),

    lists:foreach(
      fun({ChainName, RawAuthConfigs}) ->
              AuthConfig = emqx_authn:check_configs(RawAuthConfigs),
              ?AUTHN:initialize_authentication(
                 ChainName,
                 AuthConfig)
      end,
      chain_configs()).

-spec(deinitialize() -> ok).
deinitialize() ->
    ok = ?AUTHN:deregister_providers(provider_types()).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

init(#{init_fun := Fun,
       name := Name,
       interval := Interval} = Data)
  when
      is_function(Fun, 1)
      and is_atom(Name)
      and is_integer(Interval) ->
    {ok, waiting_for_process, Data, [{state_timeout, 0, check_process}]}.

handle_event(state_timeout, check_process, waiting_for_process, Data) ->
    handle_check_process(Data);

handle_event(internal, {initialize, Pid}, initializing, Data) ->
    handle_initialize(Pid, Data);

handle_event(info, Msg, idle, Data) ->
    handle_info(Msg, Data);

handle_event(EventType, Event, State, Data) ->
    handle_unknown(EventType, Event, State, Data).

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_check_process(#{name := Name, interval := Interval} = Data) ->
    case whereis(Name) of
        undefined ->
            ?tp(debug, emqx_authn_initializer_no_process, #{name => Name}),
            {keep_state_and_data, [{state_timeout, Interval, check_process}]};
        Pid ->
            ?tp(info, emqx_authn_initializer_process_registered, #{name => Name}),
            {next_state, initializing, Data, [{next_event, internal, {initialize, Pid}}]}
    end.

handle_initialize(Pid, #{init_fun := InitFun, name := Name} = Data) ->
    _ = InitFun(Pid),
    Ref = monitor(process, Pid),
    ?tp(info, emqx_authn_initializer_process_initialized, #{name => Name}),
    {next_state, idle, Data#{ref => Ref}, [{hibernate, true}]}.

handle_info({'DOWN', Ref, process, _Pid, _Info} = Msg, #{ref := Ref, name:= Name} = Data) ->
    NewData = maps:without([ref], Data),
    ?tp(warning, emqx_authn_initializer_process_died, #{name => Name, message => Msg}),
    {next_state, waiting_for_process, NewData, [{state_timeout, 0, check_process}]}.

handle_unknown(EventType, Event, State, Data) ->
    ?SLOG(warning,
          #{msg => "emqx_authn_initializer received unknown event",
            event_type => EventType,
            event => Event,
            state => State,
            data => Data
           }),
    keep_state_and_data.

chain_configs() ->
    [global_chain_config() | listener_chain_configs()].

global_chain_config() ->
    {?GLOBAL, emqx:get_raw_config([<<"authentication">>], [])}.

listener_chain_configs() ->
    lists:map(
     fun({ListenerID, _}) ->
        {ListenerID, emqx:get_raw_config(auth_config_path(ListenerID), [])}
     end,
     emqx_listeners:list()).

auth_config_path(ListenerID) ->
    [<<"listeners">>]
    ++ binary:split(atom_to_binary(ListenerID), <<":">>)
    ++ [<<"authentication">>].

provider_types() ->
    lists:map(fun({Type, _Module}) -> Type end, emqx_authn:providers()).
