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

-include_lib("emqx/include/logger.hrl").

-define(APP, emqx_rule_engine).

-define(KV_TAB, '@rule_engine_db').

-type(maybe(T) :: T | undefined).

-type(rule_id() :: binary()).
-type(rule_name() :: binary()).

-type(resource_id() :: binary()).
-type(action_instance_id() :: binary()).

-type(action_name() :: atom()).
-type(resource_type_name() :: atom()).

-type(category() :: data_persist| data_forward | offline_msgs | debug | other).

-type(descr() :: #{en := binary(), zh => binary()}).

-type(mf() :: {Module::atom(), Fun::atom()}).

-type(hook() :: atom() | 'any').

-type(topic() :: binary()).

-type(resource_status() :: #{ alive := boolean()
                            , atom() => binary() | atom() | list(binary()|atom())
                            }).

-define(descr, #{en => <<>>, zh => <<>>}).

-record(action,
        { name :: action_name()
        , category :: category()
        , for :: hook()
        , app :: atom()
        , types = [] :: list(resource_type_name())
        , module :: module()
        , on_create :: mf()
        , on_destroy :: maybe(mf())
        , hidden = false :: boolean()
        , params_spec :: #{atom() => term()} %% params specs
        , title = ?descr :: descr()
        , description = ?descr :: descr()
        }).

-record(action_instance,
        { id
        , name
        , fallbacks
        , args %% the args got from API for initializing action_instance
        }).
-type(action_instance()
      :: #action_instance{ id :: action_instance_id()
                         , name :: action_name()
                         , fallbacks :: list(#action_instance{})
                         , args :: #{binary() => term()} %% the args got from API for initializing action_instance
                         }).

-record(rule,
        { id
        , for
        , rawsql
        , is_foreach
        , fields
        , doeach
        , incase
        , conditions
        , on_action_failed
        , actions
        , enabled
        , created_at %% epoch in millisecond precision
        , description
        , state = normal
        }).
-type(rule() :: #rule{ id :: rule_id()
                     , for :: list(topic())
                     , rawsql :: binary()
                     , is_foreach :: boolean()
                     , fields :: list()
                     , doeach :: term()
                     , incase :: list()
                     , conditions :: tuple()
                     , on_action_failed :: continue | stop
                     , actions :: list(#action_instance{})
                     , enabled :: boolean()
                     , created_at :: integer() %% epoch in millisecond precision
                     , description :: binary()
                     , state :: normal | atom()
                     }).

-record(resource,
        { id :: resource_id()
        , type :: resource_type_name()
        , config :: #{} %% the configs got from API for initializing resource
        , created_at :: integer() | undefined %% epoch in millisecond precision
        , description :: binary()
        }).

-record(resource_type,
        { name :: resource_type_name()
        , provider :: atom()
        , params_spec :: #{atom() => term()} %% params specs
        , on_create :: mf()
        , on_status :: mf()
        , on_destroy :: mf()
        , title = ?descr :: descr()
        , description = ?descr :: descr()
        }).

-record(rule_hooks,
        { hook :: atom()
        , rule_id :: rule_id()
        }).

-record(resource_params,
        { id :: resource_id()
        , params :: map() %% the params got after initializing the resource
        , status = #{is_alive => false} :: #{is_alive := boolean(), atom() => term()}
        }).

-record(action_instance_params,
        { id :: action_instance_id()
        %% the params got after initializing the action
        , params :: #{}
        %% the Func/Bindings got after initializing the action
        , apply :: fun((Data::map(), Envs::map()) -> any())
                 | #{mod := module(), bindings := #{atom() => term()}}
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

-define(RPC_TIMEOUT, 30000).

-define(CLUSTER_CALL(Func, Args), ?CLUSTER_CALL(Func, Args, ok)).

-define(CLUSTER_CALL(Func, Args, ResParttern),
    fun() -> case rpc:multicall(ekka_mnesia:running_nodes(), ?MODULE, Func, Args, ?RPC_TIMEOUT) of
        {ResL, []} ->
            case lists:filter(fun(ResParttern) -> false; (_) -> true end, ResL) of
                [] -> ResL;
                ErrL ->
                    ?LOG_SENSITIVE(error, "cluster_call error found, ResL: ~p", [ResL]),
                    throw({Func, ErrL})
            end;
        {ResL, BadNodes} ->
            ?LOG_SENSITIVE(error, "cluster_call bad nodes found: ~p, ResL: ~p", [BadNodes, ResL]),
            throw({Func, {failed_on_nodes, BadNodes}})
   end end()).

%% like CLUSTER_CALL/3, but recall the remote node using FallbackFunc if Func is undefined
-define(CLUSTER_CALL(Func, Args, ResParttern, FallbackFunc, FallbackArgs),
    fun() ->
        RNodes = ekka_mnesia:running_nodes(),
        ResL = erpc:multicall(RNodes, ?MODULE, Func, Args, ?RPC_TIMEOUT),
        Res = lists:zip(RNodes, ResL),
        BadRes = lists:filtermap(fun
            ({_Node, {ok, ResParttern}}) ->
                false;
            ({Node, {error, {exception, undef, _}}}) ->
                try erpc:call(Node, ?MODULE, FallbackFunc, FallbackArgs, ?RPC_TIMEOUT) of
                    ResParttern ->
                        false;
                    OtherRes ->
                        {true, #{rpc_type => call, func => FallbackFunc,
                                 result => OtherRes, node => Node}}
                catch
                    Err:Reason ->
                        {true, #{rpc_type => call, func => FallbackFunc,
                                 exception => {Err, Reason}, node => Node}}
                end;
            ({Node, OtherRes}) ->
                {true, #{rpc_type => multicall, func => FallbackFunc,
                         result => OtherRes, node => Node}}
        end, Res),
        case BadRes of
            [] -> Res;
            _ -> throw(BadRes)
        end
    end()).

%% Tables
-define(RULE_TAB, emqx_rule).
-define(ACTION_TAB, emqx_rule_action).
-define(ACTION_INST_PARAMS_TAB, emqx_action_instance_params).
-define(RES_TAB, emqx_resource).
-define(RES_PARAMS_TAB, emqx_resource_params).
-define(RULE_HOOKS, emqx_rule_hooks).
-define(RES_TYPE_TAB, emqx_resource_type).
