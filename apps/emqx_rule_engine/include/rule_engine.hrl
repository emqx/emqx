%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-type mf() :: {Module :: atom(), Fun :: atom()}.

-type hook() :: atom() | 'any'.
-type topic() :: binary().

-type selected_data() :: map().
-type envs() :: map().

-type builtin_action_func() :: republish | console.
-type builtin_action_module() :: emqx_rule_actions.
-type bridge_channel_id() :: binary().
-type action_fun_args() :: map().

-type action() ::
    #{
        mod := builtin_action_module() | module(),
        func := builtin_action_func() | atom(),
        args => action_fun_args()
    }
    | bridge_channel_id().

-type rule() ::
    #{
        id := rule_id(),
        name := binary(),
        sql := binary(),
        actions := [action()],
        enable := boolean(),
        description => binary(),
        %% epoch in millisecond precision
        created_at := integer(),
        %% epoch in millisecond precision
        updated_at := integer(),
        from := list(topic()),
        is_foreach := boolean(),
        fields := list(),
        doeach := term(),
        incase := term(),
        conditions := tuple()
    }.

%% Arithmetic operators
-define(is_arith(Op),
    (Op =:= '+' orelse
        Op =:= '-' orelse
        Op =:= '*' orelse
        Op =:= '/' orelse
        Op =:= 'div' orelse
        Op =:= 'mod')
).

%% Compare operators
-define(is_comp(Op),
    (Op =:= '=' orelse
        Op =:= '=~' orelse
        Op =:= '>' orelse
        Op =:= '<' orelse
        Op =:= '<=' orelse
        Op =:= '>=' orelse
        Op =:= '<>' orelse
        Op =:= '!=')
).

%% Logical operators
-define(is_logical(Op), (Op =:= 'and' orelse Op =:= 'or')).

-define(RAISE(EXP, ERROR),
    ?RAISE(EXP, _ = do_nothing, ERROR)
).

-define(RAISE_BAD_SQL(Detail), throw(Detail)).

-define(RAISE(EXP, EXP_ON_FAIL, ERROR),
    fun() ->
        try
            (EXP)
        catch
            EXCLASS:EXCPTION:ST ->
                EXP_ON_FAIL,
                throw(ERROR)
        end
    end()
).

%% Tables
-define(RULE_TAB, emqx_rule_engine).

%% Allowed sql function provider modules
-define(DEFAULT_SQL_FUNC_PROVIDER, emqx_rule_funcs).
-define(IS_VALID_SQL_FUNC_PROVIDER_MODULE_NAME(Name),
    (case Name of
        <<"emqx_rule_funcs", _/binary>> ->
            true;
        <<"EmqxRuleFuncs", _/binary>> ->
            true;
        _ ->
            false
    end)
).

-define(KEY_PATH, [rule_engine, rules]).
-define(RULE_PATH(RULE), [rule_engine, rules, RULE]).
