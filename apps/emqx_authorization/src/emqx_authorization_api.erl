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

-module(emqx_authorization_api).

-include("emqx_authorization.hrl").

-rest_api(#{name   => lookup_authorization,
            method => 'GET',
            path   => "/authorization",
            func   => lookup_authorization,
            descr  => "Lookup Authorization"
           }).

-rest_api(#{name   => update_authorization,
            method => 'PUT',
            path   => "/authorization",
            func   => update_authorization,
            descr  => "Rewrite authorization list"
           }).

-rest_api(#{name   => append_authorization,
            method => 'POST',
            path   => "/authorization/append",
            func   => append_authorization,
            descr  => "Add a new rule at the end of the authorization list"
           }).

-rest_api(#{name   => push_authorization,
            method => 'POST',
            path   => "/authorization/push",
            func   => push_authorization,
            descr  => "Add a new rule at the start of the authorization list"
           }).

-export([ lookup_authorization/2
        , update_authorization/2
        , append_authorization/2
        , push_authorization/2
        ]).

lookup_authorization(_Bindings, _Params) ->
    minirest:return({ok, emqx_authorization:lookup()}).

update_authorization(_Bindings, Params) ->
    Rules = get_rules(Params),
    minirest:return(emqx_authorization:update(Rules)).

append_authorization(_Bindings, Params) ->
    Rules = get_rules(Params),
    NRules = lists:append(emqx_authorization:lookup(), Rules),
    minirest:return(emqx_authorization:update(NRules)).

push_authorization(_Bindings, Params) ->
    Rules = get_rules(Params),
    NRules = lists:append(Rules, emqx_authorization:lookup()),
    minirest:return(emqx_authorization:update(NRules)).

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

get_rules(Params) ->
    {ok, Conf} = hocon:binary(jsx:encode(Params), #{format => richmap}),
    CheckConf = hocon_schema:check(emqx_authorization_schema, Conf),
    #{<<"rules">> := Rules} = hocon_schema:richmap_to_map(CheckConf),
    Rules.

%%--------------------------------------------------------------------
%% EUnits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.
