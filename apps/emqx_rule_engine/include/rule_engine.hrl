%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(APP, emqx_rule_engine).

-define(KV_TAB, '@rule_engine_db').

-type maybe(T) :: T | undefined.

-type rule_id() :: binary().
-type rule_name() :: binary().

-type mf() :: {Module::atom(), Fun::atom()}.

-type hook() :: atom() | 'any'.

-type topic() :: binary().
-type bridge_channel_id() :: binary().
-type selected_data() :: map().
-type envs() :: map().
-type output_type() :: bridge | builtin | func.
-type output_target() :: bridge_channel_id() | atom() | output_fun().
-type output_fun_args() :: map().
-type output() :: #{
        type := output_type(),
        target := output_target(),
        args => output_fun_args()
}.
-type output_fun() :: fun((selected_data(), envs(), output_fun_args()) -> any()).

-type rule_info() ::
       #{ from := list(topic())
        , outputs := [output()]
        , sql := binary()
        , is_foreach := boolean()
        , fields := list()
        , doeach := term()
        , incase := term()
        , conditions := tuple()
        , enabled := boolean()
        , description => binary()
        }.

-record(rule,
        { id :: rule_id()
        , created_at :: integer() %% epoch in millisecond precision
        , info :: rule_info()
        }).

%% Arithmetic operators
-define(is_arith(Op), (Op =:= '+' orelse
                       Op =:= '-' orelse
                       Op =:= '*' orelse
                       Op =:= '/' orelse
                       Op =:= 'div')).

%% Compare operators
-define(is_comp(Op), (Op =:= '=' orelse
                      Op =:= '=~' orelse
                      Op =:= '>' orelse
                      Op =:= '<' orelse
                      Op =:= '<=' orelse
                      Op =:= '>=' orelse
                      Op =:= '<>' orelse
                      Op =:= '!=')).

%% Logical operators
-define(is_logical(Op), (Op =:= 'and' orelse Op =:= 'or')).

-define(RAISE(_EXP_, _ERROR_),
        ?RAISE(_EXP_, _ = do_nothing, _ERROR_)).

-define(RAISE(_EXP_, _EXP_ON_FAIL_, _ERROR_),
        fun() ->
            try (_EXP_)
            catch _EXCLASS_:_EXCPTION_:_ST_ ->
                _EXP_ON_FAIL_,
                throw(_ERROR_)
            end
        end()).

%% Tables
-define(RULE_TAB, emqx_rule).

-define(RULE_ENGINE_SHARD, emqx_rule_engine_shard).
