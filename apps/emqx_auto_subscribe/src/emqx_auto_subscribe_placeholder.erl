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
-module(emqx_auto_subscribe_placeholder).

-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx/include/logger.hrl").

-export([generate/1]).

-export([to_topic_table/3]).

-spec generate(list() | map()) -> list() | map().
generate(Topics) when is_list(Topics) ->
    [generate(Topic) || Topic <- Topics];
generate(T = #{topic := Topic}) ->
    T#{placeholder => generate(Topic, [])}.

-spec to_topic_table(list(), map(), map()) -> list().
to_topic_table(PHs, ClientInfo, ConnInfo) ->
    Fold = fun(
        #{
            qos := Qos,
            rh := RH,
            rap := RAP,
            nl := NL,
            placeholder := PlaceHolder,
            topic := RawTopic
        },
        Acc
    ) ->
        case to_topic(PlaceHolder, ClientInfo, ConnInfo, []) of
            {error, Reason} ->
                ?SLOG(warning, #{
                    msg => "auto_subscribe_ignored",
                    topic => RawTopic,
                    reason => Reason
                }),
                Acc;
            <<>> ->
                ?SLOG(warning, #{
                    msg => "auto_subscribe_ignored",
                    topic => RawTopic,
                    reason => empty_topic
                }),
                Acc;
            Topic0 ->
                {Topic, Opts} = emqx_topic:parse(Topic0),
                [{Topic, Opts#{qos => Qos, rh => RH, rap => RAP, nl => NL}} | Acc]
        end
    end,
    lists:foldl(Fold, [], PHs).

%%--------------------------------------------------------------------
%% internal

generate(<<"">>, Result) ->
    lists:reverse(Result);
generate(<<?PH_S_CLIENTID, Tail/binary>>, Result) ->
    generate(Tail, [clientid | Result]);
generate(<<?PH_S_USERNAME, Tail/binary>>, Result) ->
    generate(Tail, [username | Result]);
generate(<<?PH_S_HOST, Tail/binary>>, Result) ->
    generate(Tail, [host | Result]);
generate(<<?PH_S_PORT, Tail/binary>>, Result) ->
    generate(Tail, [port | Result]);
generate(<<Char:8, Tail/binary>>, []) ->
    generate(Tail, [<<Char:8>>]);
generate(<<Char:8, Tail/binary>>, [R | Result]) when is_binary(R) ->
    generate(Tail, [<<R/binary, Char:8>> | Result]);
generate(<<Char:8, Tail/binary>>, [R | Result]) when is_atom(R) ->
    generate(Tail, [<<Char:8>> | [R | Result]]).

to_topic([], _, _, Res) ->
    list_to_binary(lists:reverse(Res));
to_topic([Binary | PTs], C, Co, Res) when is_binary(Binary) ->
    to_topic(PTs, C, Co, [Binary | Res]);
to_topic([clientid | PTs], C = #{clientid := ClientID}, Co, Res) ->
    to_topic(PTs, C, Co, [ClientID | Res]);
to_topic([username | _], #{username := undefined}, _, _) ->
    {error, username_undefined};
to_topic([username | PTs], C = #{username := Username}, Co, Res) ->
    to_topic(PTs, C, Co, [Username | Res]);
to_topic([host | PTs], C, Co = #{peername := {Host, _}}, Res) ->
    HostBinary = list_to_binary(inet:ntoa(Host)),
    to_topic(PTs, C, Co, [HostBinary | Res]);
to_topic([port | PTs], C, Co = #{peername := {_, Port}}, Res) ->
    PortBinary = integer_to_binary(Port),
    to_topic(PTs, C, Co, [PortBinary | Res]).
