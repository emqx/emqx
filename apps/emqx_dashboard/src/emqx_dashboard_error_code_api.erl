%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_dashboard_error_code_api).

-behaviour(minirest_api).

-include_lib("emqx/include/http_api.hrl").
-include("emqx_dashboard.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    api_spec/0,
    fields/1,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    error_codes/2,
    error_code/2
]).

namespace() -> "dashboard".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/error_codes",
        "/error_codes/:code"
    ].

schema("/error_codes") ->
    #{
        'operationId' => error_codes,
        get => #{
            security => [],
            description => ?DESC(error_codes),
            tags => [<<"Error Codes">>],
            responses => #{
                200 => hoconsc:array(hoconsc:ref(?MODULE, error_code))
            }
        }
    };
schema("/error_codes/:code") ->
    #{
        'operationId' => error_code,
        get => #{
            security => [],
            description => ?DESC(error_codes),
            tags => [<<"Error Codes">>],
            parameters => [
                {code,
                    hoconsc:mk(hoconsc:enum(emqx_dashboard_error_code:all()), #{
                        desc => <<"API Error Codes">>,
                        in => path,
                        example => hd(emqx_dashboard_error_code:all())
                    })}
            ],
            responses => #{
                200 => hoconsc:ref(?MODULE, error_code)
            }
        }
    }.

fields(error_code) ->
    [
        {code, hoconsc:mk(string(), #{desc => <<"Code Name">>})},
        {description, hoconsc:mk(string(), #{desc => <<"Description">>})}
    ].

error_codes(_, _) ->
    {200, emqx_dashboard_error_code:list()}.

error_code(_, #{bindings := #{code := Name}}) ->
    case emqx_dashboard_error_code:look_up(Name) of
        {ok, Code} ->
            {200, Code};
        {error, not_found} ->
            Message = list_to_binary(io_lib:format("Code name ~p not found", [Name])),
            {404, ?NOT_FOUND, Message}
    end.
