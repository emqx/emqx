%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authz_api_cache).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    clean_cache/2
]).

-define(BAD_REQUEST, 'BAD_REQUEST').

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/authorization/cache"
    ].

%%--------------------------------------------------------------------
%% Schema for each URI
%%--------------------------------------------------------------------

schema("/authorization/cache") ->
    #{
        'operationId' => clean_cache,
        delete =>
            #{
                description => ?DESC(authorization_cache_delete),
                responses =>
                    #{
                        204 => <<"No Content">>,
                        400 => emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>)
                    }
            }
    }.

clean_cache(delete, _) ->
    case emqx_mgmt:clean_authz_cache_all() of
        ok ->
            {204};
        {error, Reason} ->
            {400, #{
                code => <<"BAD_REQUEST">>,
                message => bin(Reason)
            }}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

bin(Term) -> erlang:iolist_to_binary(io_lib:format("~p", [Term])).
