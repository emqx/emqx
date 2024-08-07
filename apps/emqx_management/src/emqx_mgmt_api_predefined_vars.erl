%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_predefined_vars).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").

-include("emqx_mgmt.hrl").

-behaviour(minirest_api).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([
    predefined_vars/2,
    delete_predefined_vars/2
]).

-define(TAGS, [<<"Predefined Vars">>]).

-define(SOURCE_TYPES, [env, plain, builtin]).

-define(FORMAT_FUN, {emqx_predefined_vars, to_sensitive_map}).

namespace() ->
    undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/predefined_vars", "/predefined_vars/:name"].

schema("/predefined_vars") ->
    #{
        'operationId' => predefined_vars,
        get => #{
            description => ?DESC(list_predefined_vars),
            tags => ?TAGS,
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => [
                    {data, hoconsc:mk(hoconsc:array(hoconsc:ref(predefined_vars)), #{})},
                    {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta), #{})}
                ]
            }
        },
        post => #{
            description => ?DESC(create_predefined_vars),
            tags => ?TAGS,
            'requestBody' => hoconsc:mk(hoconsc:ref(predefined_vars)),
            responses => #{
                200 => [{data, hoconsc:mk(hoconsc:array(hoconsc:ref(predefined_vars)), #{})}],
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST']
                )
            }
        }
    };
schema("/predefined_vars/:name") ->
    #{
        'operationId' => delete_predefined_vars,
        delete => #{
            description => ?DESC(delete_predefined_vars),
            tags => ?TAGS,
            parameters => [
                {name,
                    hoconsc:mk(binary(), #{
                        desc => ?DESC(name),
                        required => true,
                        in => path,
                        example => <<"EXAMPLE">>
                    })}
            ],
            responses => #{
                204 => <<"Delete predefined variable success">>,
                400 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND']
                )
            }
        }
    }.

fields(predefined_vars) ->
    [
        {name,
            hoconsc:mk(binary(), #{
                desc => ?DESC(emqx_predefined_vars, name),
                required => true,
                example => <<"EXAMPLE"/utf8>>
            })},
        {source_type,
            hoconsc:mk(hoconsc:enum(?SOURCE_TYPES), #{
                desc => ?DESC(emqx_predefined_vars, source_type),
                required => true,
                example => plain
            })},
        {source_value,
            hoconsc:mk(binary(), #{
                desc => ?DESC(emqx_predefined_vars, source_value),
                required => true,
                example => <<"Value">>
            })},
        {sensitive,
            hoconsc:mk(boolean(), #{
                desc => ?DESC(emqx_predefined_vars, sensitive),
                default => false
            })}
    ].

predefined_vars(get, #{query_string := Params}) ->
    Response = emqx_mgmt_api:paginate(emqx_predefined_vars:tables(), Params, ?FORMAT_FUN),
    {200, Response};
predefined_vars(post, #{body := Body}) ->
    case emqx_predefined_vars:add(Body) of
        {error, Reason} ->
            ErrorReason = io_lib:format("~p", [Reason]),
            {400, 'BAD_REQUEST', list_to_binary(ErrorReason)};
        {ok, _} ->
            {200, Body}
    end.

delete_predefined_vars(delete, #{bindings := #{name := Name}}) ->
    case emqx_predefined_vars:delete(Name) of
        {ok, _} ->
            {204};
        {error, Reason} ->
            ErrorReason = io_lib:format("~p", [Reason]),
            {400, 'BAD_REQUEST', list_to_binary(ErrorReason)}
    end.
