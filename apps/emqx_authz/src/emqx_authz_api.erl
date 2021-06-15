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

-module(emqx_authz_api).

-include("emqx_authz.hrl").

-rest_api(#{name   => lookup_authz,
            method => 'GET',
            path   => "/authz",
            func   => lookup_authz,
            descr  => "Lookup Authorization"
           }).

-rest_api(#{name   => update_authz,
            method => 'PUT',
            path   => "/authz",
            func   => update_authz,
            descr  => "Rewrite authz list"
           }).

-rest_api(#{name   => append_authz,
            method => 'POST',
            path   => "/authz/append",
            func   => append_authz,
            descr  => "Add a new rule at the end of the authz list"
           }).

-rest_api(#{name   => push_authz,
            method => 'POST',
            path   => "/authz/push",
            func   => push_authz,
            descr  => "Add a new rule at the start of the authz list"
           }).

-export([ lookup_authz/2
        , update_authz/2
        , append_authz/2
        , push_authz/2
        ]).

lookup_authz(_Bindings, _Params) ->
    minirest:return({ok, emqx_authz:lookup()}).

update_authz(_Bindings, Params) ->
    Rules = get_rules(Params),
    minirest:return(emqx_authz:update(Rules)).

append_authz(_Bindings, Params) ->
    Rules = get_rules(Params),
    NRules = lists:append(emqx_authz:lookup(), Rules),
    minirest:return(emqx_authz:update(NRules)).

push_authz(_Bindings, Params) ->
    Rules = get_rules(Params),
    NRules = lists:append(Rules, emqx_authz:lookup()),
    minirest:return(emqx_authz:update(NRules)).

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

get_rules(Params) ->
    {ok, Conf} = hocon:binary(jsx:encode(Params), #{format => richmap}),
    CheckConf = hocon_schema:check(emqx_authz_schema, Conf),
    #{<<"rules">> := Rules} = hocon_schema:richmap_to_map(CheckConf),
    Rules.

%%--------------------------------------------------------------------
%% EUnits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.
