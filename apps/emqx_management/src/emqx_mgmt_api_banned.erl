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

-module(emqx_mgmt_api_banned).

-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").

-include("emqx_mgmt.hrl").

-behaviour(minirest_api).

-export([ api_spec/0
        , paths/0
        , schema/1
        , fields/1]).

-export([format/1]).

-export([ banned/2
        , delete_banned/2
        ]).

-define(TAB, emqx_banned).

-define(BANNED_TYPES, [clientid, username, peerhost]).

-define(FORMAT_FUN, {?MODULE, format}).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/banned", "/banned/:as/:who"].

schema("/banned") ->
    #{
        'operationId' =>  banned,
        get => #{
            description => <<"List banned">>,
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 =>[
                    {data, hoconsc:mk(hoconsc:array(hoconsc:ref(ban)), #{})},
                    {meta, hoconsc:mk(hoconsc:ref(meta), #{})}
                ]
            }
        },
        post => #{
            description => <<"Create banned">>,
            'requestBody' => hoconsc:mk(hoconsc:ref(ban)),
            responses => #{
                200 => [{data, hoconsc:mk(hoconsc:array(hoconsc:ref(ban)), #{})}],
                400 => emqx_dashboard_swagger:error_codes(['ALREADY_EXISTED'],
                                                          <<"Banned already existed">>)
            }
        }
    };
schema("/banned/:as/:who") ->
    #{
        'operationId' => delete_banned,
        delete => #{
            description => <<"Delete banned">>,
            parameters => [
                {as, hoconsc:mk(hoconsc:enum(?BANNED_TYPES), #{
                    desc => <<"Banned type">>,
                    in => path,
                    example => username})},
                {who, hoconsc:mk(binary(), #{
                    desc => <<"Client info as banned type">>,
                    in => path,
                    example => <<"Badass">>})}
                ],
            responses => #{
                204 => <<"Delete banned success">>,
                404 => emqx_dashboard_swagger:error_codes(['RESOURCE_NOT_FOUND'],
                                                          <<"Banned not found">>)
            }
        }
    }.

fields(ban) ->
    [
        {as, hoconsc:mk(hoconsc:enum(?BANNED_TYPES), #{
            desc => <<"Banned type clientid, username, peerhost">>,
            nullable => false,
            example => username})},
        {who, hoconsc:mk(binary(), #{
            desc => <<"Client info as banned type">>,
            nullable => false,
            example => <<"Badass">>})},
        {by, hoconsc:mk(binary(), #{
            desc => <<"Commander">>,
            nullable => true,
            example => <<"mgmt_api">>})},
        {reason, hoconsc:mk(binary(), #{
            desc => <<"Banned reason">>,
            nullable => true,
            example => <<"Too many requests">>})},
        {at, hoconsc:mk(binary(), #{
            desc => <<"Create banned time, rfc3339, now if not specified">>,
            nullable => true,
            validator => fun is_rfc3339/1,
            example => <<"2021-10-25T21:48:47+08:00">>})},
        {until, hoconsc:mk(binary(), #{
            desc => <<"Cancel banned time, rfc3339, now + 5 minute if not specified">>,
            nullable => true,
            validator => fun is_rfc3339/1,
            example => <<"2021-10-25T21:53:47+08:00">>})
        }
    ];
fields(meta) ->
    emqx_dashboard_swagger:fields(page) ++
        emqx_dashboard_swagger:fields(limit) ++
        [{count, hoconsc:mk(integer(), #{example => 1})}].

is_rfc3339(Time) ->
    try
        emqx_banned:to_timestamp(Time),
        ok
    catch _:_ -> {error, Time}
    end.

banned(get, #{query_string := Params}) ->
    Response = emqx_mgmt_api:paginate(?TAB, Params, ?FORMAT_FUN),
    {200, Response};
banned(post, #{body := Body}) ->
    case emqx_banned:create(emqx_banned:parse(Body)) of
        {ok, Banned} ->
            {200, format(Banned)};
        {error, {already_exist, Old}} ->
            {400, #{code => 'ALREADY_EXISTED', message => format(Old)}}
    end.

delete_banned(delete, #{bindings := Params}) ->
    case emqx_banned:look_up(Params) of
        [] ->
            #{as := As0, who := Who0} = Params,
            Message = list_to_binary(io_lib:format("~p: ~s not found", [As0, Who0])),
            {404, #{code => 'RESOURCE_NOT_FOUND', message => Message}};
        _ ->
            ok = emqx_banned:delete(Params),
            {204}
    end.

format(Banned) ->
    emqx_banned:format(Banned).
