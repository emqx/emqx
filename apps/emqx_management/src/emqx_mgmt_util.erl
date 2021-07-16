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

-module(emqx_mgmt_util).

-export([ strftime/1
        , datetime/1
        , kmg/1
        , ntoa/1
        , merge_maps/2
        , batch_operation/3
        ]).

-export([ request_body_schema/1
        , request_body_array_schema/1
        , response_schema/1
        , response_schema/2
        , response_array_schema/2
        , not_found_schema/1
        , not_found_schema/2
        , batch_response_schema/1]).

-export([urldecode/1]).

-define(KB, 1024).
-define(MB, (1024*1024)).
-define(GB, (1024*1024*1024)).

%%--------------------------------------------------------------------
%% Strftime
%%--------------------------------------------------------------------

strftime({MegaSecs, Secs, _MicroSecs}) ->
    strftime(datetime(MegaSecs * 1000000 + Secs));

strftime(Secs) when is_integer(Secs) ->
    strftime(datetime(Secs));

strftime({{Y,M,D}, {H,MM,S}}) ->
    lists:flatten(
        io_lib:format(
            "~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w", [Y, M, D, H, MM, S])).

datetime(Timestamp) when is_integer(Timestamp) ->
    Epoch = calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}),
    Universal = calendar:gregorian_seconds_to_datetime(Timestamp + Epoch),
    calendar:universal_time_to_local_time(Universal).

kmg(Byte) when Byte > ?GB ->
    kmg(Byte / ?GB, "G");
kmg(Byte) when Byte > ?MB ->
    kmg(Byte / ?MB, "M");
kmg(Byte) when Byte > ?KB ->
    kmg(Byte / ?KB, "K");
kmg(Byte) ->
    Byte.
kmg(F, S) ->
    iolist_to_binary(io_lib:format("~.2f~s", [F, S])).

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
    inet_parse:ntoa(IP).

merge_maps(Default, New) ->
    maps:fold(fun(K, V, Acc) ->
        case maps:get(K, Acc, undefined) of
            OldV when is_map(OldV),
                      is_map(V) -> Acc#{K => merge_maps(OldV, V)};
            _ -> Acc#{K => V}
        end
    end, Default, New).

urldecode(S) ->
    emqx_http_lib:uri_decode(S).

%%%==============================================================================================
%% schema util

request_body_array_schema(Schema) when is_map(Schema) ->
    json_content_schema("", #{type => array, items => Schema});
request_body_array_schema(Ref) when is_binary(Ref) ->
    json_content_schema("", #{type => array, items => minirest:ref(Ref)}).

request_body_schema(Schema) when is_map(Schema) ->
    json_content_schema("", Schema);
request_body_schema(Ref) when is_binary(Ref) ->
    json_content_schema("", minirest:ref(Ref)).

response_array_schema(Description, Schema) when is_map(Schema) ->
    json_content_schema(Description, #{type => array, items => Schema});
response_array_schema(Description, Ref) when is_binary(Ref) ->
    json_content_schema(Description, #{type => array, items => minirest:ref(Ref)}).

response_schema(Description) ->
    json_content_schema(Description).

response_schema(Description, Schema) when is_map(Schema) ->
    json_content_schema(Description, Schema);
response_schema(Description, Ref) when is_binary(Ref) ->
    json_content_schema(Description, minirest:ref(Ref)).

not_found_schema(Description) ->
    not_found_schema(Description, ["RESOURCE_NOT_FOUND"]).

not_found_schema(Description, Enum) ->
    Schema = #{
        type => object,
        properties => #{
            code => #{
                type => string,
                enum => Enum},
            reason => #{
                type => string}}},
    json_content_schema(Description, Schema).

batch_response_schema(DefName) when is_binary(DefName) ->
    Schema = #{
        type => object,
        properties => #{
            success => #{
                type => integer,
                description => <<"Success count">>},
            failed => #{
                type => integer,
                description => <<"Failed count">>},
            detail => #{
                type => array,
                description => <<"Failed object & reason">>,
                items => #{
                    type => object,
                    properties =>
                    #{
                        data => minirest:ref(DefName),
                        reason => #{
                            type => <<"string">>}}}}}},
    json_content_schema("", Schema).

json_content_schema(Description, Schema) ->
    Content =
        #{content => #{
            'application/json' => #{
                schema => Schema}}},
    case Description of
        "" ->
            Content;
        _ ->
            maps:merge(#{description => Description}, Content)
    end.

json_content_schema(Description) ->
    #{description => Description}.

%%%==============================================================================================
batch_operation(Module, Function, ArgsList) ->
    Failed = batch_operation(Module, Function, ArgsList, []),
    Len = erlang:length(Failed),
    Success = erlang:length(ArgsList) - Len,
    Fun = fun({Args, Reason}, Detail) -> [#{data => Args, reason => io_lib:format("~p", [Reason])} | Detail] end,
    #{success => Success, failed => Len, detail => lists:foldl(Fun, [], Failed)}.

batch_operation(_Module, _Function, [], Failed) ->
    lists:reverse(Failed);
batch_operation(Module, Function, [Args | ArgsList], Failed) ->
    case erlang:apply(Module, Function, Args) of
        ok ->
            batch_operation(Module, Function, ArgsList, Failed);
        {error ,Reason} ->
            batch_operation(Module, Function, ArgsList, [{Args, Reason} | Failed])
    end.
