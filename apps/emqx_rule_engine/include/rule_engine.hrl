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

-type(maybe(T) :: T | undefined).

-type(rule_id() :: binary()).
-type(rule_name() :: binary()).

-type(descr() :: #{en := binary(), zh => binary()}).

-type(mf() :: {Module::atom(), Fun::atom()}).

-type(hook() :: atom() | 'any').

-type(topic() :: binary()).
-type(bridge_channel_id() :: binary()).

-type(rule_info() ::
       #{ from := list(topic())
        , to := list(bridge_channel_id() | fun())
        , sql := binary()
        , is_foreach := boolean()
        , fields := list()
        , doeach := term()
        , incase := list()
        , conditions := tuple()
        , enabled := boolean()
        , description := binary()
        }).

-define(descr, #{en => <<>>, zh => <<>>}).

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
