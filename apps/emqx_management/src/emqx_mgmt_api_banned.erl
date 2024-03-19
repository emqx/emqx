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

-module(emqx_mgmt_api_banned).

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

-export([format/1]).

-export([
    banned/2,
    delete_banned/2
]).

-define(TAGS, [<<"Banned">>]).

-define(BANNED_TYPES, [clientid, username, peerhost, clientid_re, username_re, peerhost_net]).

-define(FORMAT_FUN, {?MODULE, format}).

namespace() ->
    undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/banned", "/banned/:as/:who"].

schema("/banned") ->
    #{
        'operationId' => banned,
        get => #{
            description => ?DESC(list_banned_api),
            tags => ?TAGS,
            parameters => [
                hoconsc:ref(emqx_dashboard_swagger, page),
                hoconsc:ref(emqx_dashboard_swagger, limit)
            ],
            responses => #{
                200 => [
                    {data, hoconsc:mk(hoconsc:array(hoconsc:ref(ban)), #{})},
                    {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta), #{})}
                ]
            }
        },
        post => #{
            description => ?DESC(create_banned_api),
            tags => ?TAGS,
            'requestBody' => hoconsc:mk(hoconsc:ref(ban)),
            responses => #{
                200 => [{data, hoconsc:mk(hoconsc:array(hoconsc:ref(ban)), #{})}],
                400 => emqx_dashboard_swagger:error_codes(
                    ['ALREADY_EXISTS', 'BAD_REQUEST'],
                    ?DESC(create_banned_api_response400)
                )
            }
        },
        delete => #{
            description => ?DESC(clear_banned_api),
            tags => ?TAGS,
            parameters => [],
            'requestBody' => [],
            responses => #{204 => <<"No Content">>}
        }
    };
schema("/banned/:as/:who") ->
    #{
        'operationId' => delete_banned,
        delete => #{
            description => ?DESC(delete_banned_api),
            tags => ?TAGS,
            parameters => [
                {as,
                    hoconsc:mk(hoconsc:enum(?BANNED_TYPES), #{
                        desc => ?DESC(as),
                        required => true,
                        in => path,
                        example => username
                    })},
                {who,
                    hoconsc:mk(binary(), #{
                        desc => ?DESC(who),
                        required => true,
                        in => path,
                        example => <<"Badass">>
                    })}
            ],
            responses => #{
                204 => <<"Delete banned success">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['NOT_FOUND'],
                    ?DESC(delete_banned_api_response404)
                )
            }
        }
    }.

fields(ban) ->
    [
        {as,
            hoconsc:mk(hoconsc:enum(?BANNED_TYPES), #{
                desc => ?DESC(as),
                required => true,
                example => username
            })},
        {who,
            hoconsc:mk(binary(), #{
                desc => ?DESC(who),
                required => true,
                example => <<"Banned name"/utf8>>
            })},
        {by,
            hoconsc:mk(binary(), #{
                desc => ?DESC(by),
                required => false,
                example => <<"mgmt_api">>
            })},
        {reason,
            hoconsc:mk(binary(), #{
                desc => ?DESC(reason),
                required => false,
                example => <<"Too many requests">>
            })},
        {at,
            hoconsc:mk(emqx_utils_calendar:epoch_second(), #{
                desc => ?DESC(at),
                required => false,
                example => <<"2021-10-25T21:48:47+08:00">>
            })},
        {until,
            hoconsc:mk(emqx_utils_calendar:epoch_second(), #{
                desc => ?DESC(until),
                required => false,
                example => <<"2021-10-25T21:53:47+08:00">>
            })}
    ].

banned(get, #{query_string := Params}) ->
    Response = emqx_mgmt_api:paginate(emqx_banned:tables(), Params, ?FORMAT_FUN),
    {200, Response};
banned(post, #{body := Body}) ->
    case emqx_banned:parse(Body) of
        {error, Reason} ->
            ErrorReason = io_lib:format("~p", [Reason]),
            {400, 'BAD_REQUEST', list_to_binary(ErrorReason)};
        Ban ->
            case emqx_banned:create(Ban) of
                {ok, Banned} ->
                    {200, format(Banned)};
                {error, {already_exist, Old}} ->
                    OldBannedFormat = emqx_utils_json:encode(format(Old)),
                    {400, 'ALREADY_EXISTS', OldBannedFormat}
            end
    end;
banned(delete, _) ->
    emqx_banned:clear(),
    {204}.

delete_banned(delete, #{bindings := Params}) ->
    case emqx_banned:look_up(Params) of
        [] ->
            #{as := As0, who := Who0} = Params,
            Message = list_to_binary(io_lib:format("~p: ~s not found", [As0, Who0])),
            {404, 'NOT_FOUND', Message};
        _ ->
            ok = emqx_banned:delete(Params),
            {204}
    end.

format(Banned) ->
    emqx_banned:format(Banned).
