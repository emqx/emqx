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
    return({ok, emqx_authz:lookup()}).

update_authz(_Bindings, Params) ->
    Rules = form_rules(Params),
    return(emqx_authz:update(replace, Rules)).

append_authz(_Bindings, Params) ->
    Rules = form_rules(Params),
    return(emqx_authz:update(tail, Rules)).

push_authz(_Bindings, Params) ->
    Rules = form_rules(Params),
    return(emqx_authz:update(head, Rules)).

%%------------------------------------------------------------------------------
%% Interval Funcs
%%------------------------------------------------------------------------------

form_rules(Params) ->
    Params.

%%--------------------------------------------------------------------
%% EUnits
%%--------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


-endif.

return(_) ->
%%    TODO: V5 api
    ok.