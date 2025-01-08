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

-module(emqx_mgmt_api_banned).

-feature(maybe_expr, enable).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include("emqx_mgmt.hrl").

-behaviour(minirest_api).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([qs2ms/2, run_fuzzy_filter/2, format/1]).

-export([
    banned/2,
    delete_banned/2
]).

-define(TAGS, [<<"Banned">>]).

-define(BANNED_TYPES, [clientid, username, peerhost, clientid_re, username_re, peerhost_net]).

-define(FORMAT_FUN, {?MODULE, format}).

-define(BANNED_QSCHEMA, [
    {<<"username">>, binary},
    {<<"clientid">>, binary},
    {<<"peerhost">>, binary},
    {<<"like_clientid">>, binary},
    {<<"like_username">>, binary},
    {<<"like_peerhost">>, binary},
    {<<"like_peerhost_net">>, binary}
]).

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
            parameters => fields(get_banned_parameters),
            responses => #{
                200 => [
                    {data, hoconsc:mk(hoconsc:array(hoconsc:ref(ban)), #{})},
                    {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta), #{})}
                ],
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'],
                    ?DESC(create_banned_api_response400)
                )
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

fields(get_banned_parameters) ->
    [
        hoconsc:ref(emqx_dashboard_swagger, page),
        hoconsc:ref(emqx_dashboard_swagger, limit),
        {clientid,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => ?DESC(filter_clientid)
            })},
        {username,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => ?DESC(filter_username)
            })},
        {peerhost,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => ?DESC(filter_peerhost),
                example => <<"127.0.0.1">>
            })},
        {like_clientid,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => ?DESC(filter_like_clientid)
            })},
        {like_username,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => ?DESC(filter_like_username)
            })},
        {like_peerhost,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => ?DESC(filter_like_peerhost),
                example => <<"127.0.0.1">>
            })},
        {like_peerhost_net,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => ?DESC(filter_like_peerhost_net),
                example => <<"192.1.0.0/16">>
            })}
    ];
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
            hoconsc:mk(hoconsc:union([infinity, emqx_utils_calendar:epoch_second()]), #{
                desc => ?DESC(until),
                required => false,
                default => infinity,
                example => <<"2021-10-25T21:53:47+08:00">>
            })}
    ].

banned(get, #{query_string := Params}) ->
    case list_banned(Params) of
        {200, _Response} = Resp ->
            Resp;
        {error, Reason} ->
            {400, 'BAD_REQUEST', format_error(Reason)}
    end;
banned(post, #{body := Body}) ->
    case emqx_banned:parse(Body) of
        {error, Reason} ->
            {400, 'BAD_REQUEST', format_error(Reason)};
        {ok, Ban} ->
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

list_banned(Params) ->
    {_Cnt, {QS, FuzzyQS}} = emqx_mgmt_api:parse_qstring(Params, ?BANNED_QSCHEMA),
    list_banned(QS, FuzzyQS, Params).

%% Filters are logically mutually exclusive, two filters always get the zero set,
%% unless they are of the same type and one is exact and the other is fuzzy,
%% this case is meaningless, so only one filter is allowed in a query.
list_banned([], [], Params) ->
    Response = emqx_mgmt_api:paginate(emqx_banned:tables(), Params, ?FORMAT_FUN),
    {200, Response};
list_banned([{As, '=:=', Who}], [], Params) ->
    maybe
        Key = {As, _Who1} ?= emqx_banned:parse_who(#{<<"as">> => As, <<"who">> => Who}),
        Result = emqx_banned:look_up(Key),
        MetaIn = emqx_mgmt_api:parse_pager_params(Params),
        {200, #{
            meta => MetaIn#{hasnext => false, count => erlang:length(Result)},
            data => lists:map(fun format/1, Result)
        }}
    end;
list_banned([], [{_Type, like, Value}], Params) ->
    case re:compile(Value) of
        {ok, _} ->
            {200,
                emqx_mgmt_api:node_query_with_tabs(
                    node(),
                    emqx_banned:tables(),
                    Params,
                    ?BANNED_QSCHEMA,
                    fun ?MODULE:qs2ms/2,
                    fun ?MODULE:format/1
                )};
        {error, {Reason, Pos}} ->
            {error, #{
                message => <<"The filter is not a validation regex expression">>,
                reason => emqx_utils_conv:bin(Reason),
                position => Pos,
                filter => Value
            }}
    end;
list_banned(_QS, _FuzzyQS, _Params) ->
    {error, <<"too_many_filters">>}.

qs2ms(_Tab, {_QS, FuzzyQS}) ->
    #{
        match_spec => ms_from_qstring(FuzzyQS),
        fuzzy_fun => fuzzy_filter_fun(FuzzyQS)
    }.

ms_from_qstring([{username, like, _UserName}]) ->
    ets:fun2ms(fun(#banned{who = {Type, _}} = Banned) when
        Type =:= username; Type =:= username_re
    ->
        Banned
    end);
ms_from_qstring([{clientid, like, _ClientId}]) ->
    ets:fun2ms(fun(#banned{who = {Type, _}} = Banned) when
        Type =:= clientid; Type =:= clientid_re
    ->
        Banned
    end);
ms_from_qstring([{peerhost, like, _Peerhost}]) ->
    ets:fun2ms(fun(#banned{who = {peerhost, _}} = Banned) ->
        Banned
    end);
ms_from_qstring([{peerhost_net, like, _PeerhostNet}]) ->
    ets:fun2ms(fun(#banned{who = {peerhost_net, _}} = Banned) ->
        Banned
    end);
ms_from_qstring(_) ->
    [].

%% Fuzzy username funcs
fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    #banned{who = {DataType, DataValue}},
    [{TargetType, like, TargetValue}]
) ->
    Validator = get_fuzzy_validator(DataType, TargetType),
    Validator(DataValue, TargetValue);
run_fuzzy_filter(_, []) ->
    false.

get_fuzzy_validator(username, username) ->
    fun binary_fuzzy_validator/2;
get_fuzzy_validator(username_re, username) ->
    fun binary_fuzzy_validator/2;
get_fuzzy_validator(clientid, clientid) ->
    fun binary_fuzzy_validator/2;
get_fuzzy_validator(clientid_re, clientid) ->
    fun binary_fuzzy_validator/2;
get_fuzzy_validator(peerhost, peerhost) ->
    fun(Peerhost, Target) ->
        Peerhost1 = inet:ntoa(Peerhost),
        binary_fuzzy_validator(Peerhost1, Target)
    end;
get_fuzzy_validator(peerhost_net, peerhost_net) ->
    fun(CIDR, Target) ->
        CIDRBinary = erlang:list_to_binary(esockd_cidr:to_string(CIDR)),
        binary_fuzzy_validator(CIDRBinary, Target)
    end;
get_fuzzy_validator(_, _) ->
    fun(_, _) ->
        false
    end.

binary_fuzzy_validator({_RE, Data}, Target) ->
    re:run(Data, Target) =/= nomatch;
binary_fuzzy_validator(Data, Target) ->
    re:run(Data, Target) =/= nomatch.

format(Banned) ->
    emqx_banned:format(Banned).

format_error(Error) when is_binary(Error) ->
    Error;
format_error(Error) when is_map(Error) ->
    emqx_utils_json:encode(Error);
format_error(Reason) ->
    ErrorReason = io_lib:format("~p", [Reason]),
    erlang:iolist_to_binary(ErrorReason).
