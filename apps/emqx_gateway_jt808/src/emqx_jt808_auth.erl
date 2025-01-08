%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_jt808_auth).

-include("emqx_jt808.hrl").

-export([
    init/1,
    register/2,
    authenticate/2
]).

-export_type([auth/0]).

init(#{allow_anonymous := true}) ->
    #auth{registry = undefined, authentication = undefined, allow_anonymous = true};
init(#{allow_anonymous := Anonymous = false, registry := Reg, authentication := Auth}) ->
    #auth{registry = Reg, authentication = Auth, allow_anonymous = Anonymous}.

register(_RegFrame, #auth{registry = undefined, allow_anonymous = true}) ->
    {ok, anonymous};
register(_RegFrame, #auth{registry = undefined, allow_anonymous = false}) ->
    {error, registry_server_not_existed};
register(RegFrame, #auth{registry = RegUrl}) ->
    #{
        <<"header">> := #{<<"phone">> := Phone},
        <<"body">> := FBody
    } = RegFrame,
    Params = maps:merge(FBody, #{<<"phone">> => Phone}),
    case request(RegUrl, Params) of
        {ok, 200, Body} ->
            case emqx_utils_json:safe_decode(Body, [return_maps]) of
                {ok, #{<<"code">> := 0, <<"authcode">> := Authcode}} ->
                    {ok, Authcode};
                {ok, #{<<"code">> := Code}} ->
                    {error, Code};
                _ ->
                    {error, {invailed_resp, Body}}
            end;
        {ok, Code, Body} ->
            {error, {unknown_resp, Code, Body}};
        {error, Reason} ->
            {error, Reason}
    end.

authenticate(_AuthFrame, #auth{authentication = undefined, allow_anonymous = true}) ->
    {ok, #{auth_result => true, anonymous => true}};
authenticate(_AuthFrame, #auth{authentication = undefined, allow_anonymous = false}) ->
    {ok, #{auth_result => false, anonymous => false}};
authenticate(AuthFrame, #auth{authentication = AuthUrl}) ->
    #{
        <<"header">> := #{<<"phone">> := Phone},
        <<"body">> := #{<<"code">> := AuthCode}
    } = AuthFrame,
    case request(AuthUrl, #{<<"code">> => AuthCode, <<"phone">> => Phone}) of
        {ok, 200, _} ->
            {ok, #{auth_result => true, anonymous => false}};
        {ok, _, _} ->
            {ok, #{auth_result => false, anonymous => false}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Inernal functions
%%--------------------------------------------------------------------

request(Url, Params) ->
    RetryOpts = #{times => 3, interval => 1000, backoff => 2.0},
    Req = {Url, [], "application/json", emqx_utils_json:encode(Params)},
    reply(request_(post, Req, [{autoredirect, true}], [{body_format, binary}], RetryOpts)).

request_(
    Method,
    Req,
    HTTPOpts,
    Opts,
    RetryOpts = #{
        times := Times,
        interval := Interval,
        backoff := BackOff
    }
) ->
    case httpc:request(Method, Req, HTTPOpts, Opts) of
        {error, _Reason} when Times > 0 ->
            timer:sleep(trunc(Interval)),
            RetryOpts1 = RetryOpts#{
                times := Times - 1,
                interval := Interval * BackOff
            },
            request_(Method, Req, HTTPOpts, Opts, RetryOpts1);
        Other ->
            Other
    end.

reply({ok, {{_, Code, _}, _Headers, Body}}) ->
    {ok, Code, Body};
reply({error, Error}) ->
    {error, Error}.
