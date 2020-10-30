%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc
%%
%% Stomp Frame:
%%
%% COMMAND
%% header1:value1
%% header2:value2
%%
%% Body^@
%%
%% Stomp 1.2 BNF grammar:
%%
%% NULL                = <US-ASCII null (octet 0)>
%% LF                  = <US-ASCII line feed (aka newline) (octet 10)>
%% CR                  = <US-ASCII carriage return (octet 13)>
%% EOL                 = [CR] LF
%% OCTET               = <any 8-bit sequence of data>
%%
%% frame-stream        = 1*frame
%%
%% frame               = command EOL
%%                       *( header EOL )
%%                       EOL
%%                       *OCTET
%%                       NULL
%%                       *( EOL )
%%
%% command             = client-command | server-command
%%
%% client-command      = "SEND"
%%                       | "SUBSCRIBE"
%%                       | "UNSUBSCRIBE"
%%                       | "BEGIN"
%%                       | "COMMIT"
%%                       | "ABORT"
%%                       | "ACK"
%%                       | "NACK"
%%                       | "DISCONNECT"
%%                       | "CONNECT"
%%                       | "STOMP"
%%
%% server-command      = "CONNECTED"
%%                       | "MESSAGE"
%%                       | "RECEIPT"
%%                       | "ERROR"
%%
%% header              = header-name ":" header-value
%% header-name         = 1*<any OCTET except CR or LF or ":">
%% header-value        = *<any OCTET except CR or LF or ":">
%%
%% @end

-module(emqx_stomp_frame).

-include("emqx_stomp.hrl").

-export([ parser/1
        , serialize/1
        ]).

-export([ make/2
        , make/3
        , format/1
        ]).

-define(NULL,  0).
-define(CR,    $\r).
-define(LF,    $\n).
-define(BSL,   $\\).
-define(COLON, $:).

-define(IS_ESC(Ch), Ch == ?CR; Ch == ?LF; Ch == ?BSL; Ch == ?COLON).

-record(parser_state, {cmd, headers = [], hdname, acc = <<>>, limit}).

-record(frame_limit, {max_header_num, max_header_length, max_body_length}).

-type(parser() :: fun((binary()) -> {ok, stomp_frame(), binary()}
                                  | {more, parser()}
                                  | {error, any()})).

%% @doc Initialize a parser
-spec parser([proplists:property()]) -> parser().
parser(Opts) ->
    fun(Bin) -> parse(none, Bin, #parser_state{limit = limit(Opts)}) end.

limit(Opts) ->
    #frame_limit{max_header_num     = g(max_header_num,    Opts, ?MAX_HEADER_NUM),
                 max_header_length  = g(max_header_length, Opts, ?MAX_BODY_LENGTH),
                 max_body_length    = g(max_body_length,   Opts, ?MAX_BODY_LENGTH)}.

g(Key, Opts, Val) ->
    proplists:get_value(Key, Opts, Val).

%% @doc Parse frame
-spec(parse(Phase :: atom(), binary(), #parser_state{}) ->
    {ok, stomp_frame(), binary()} | {more, parser()} | {error, any()}).
parse(none, <<>>, State) ->
    {more, fun(Bin) -> parse(none, Bin, State) end};
parse(none, <<?LF, Bin/binary>>, State) ->
    parse(none, Bin, State);
parse(none, Bin, State) ->
    parse(command, Bin, State);

parse(Phase, <<>>, State) ->
    {more, fun(Bin) -> parse(Phase, Bin, State) end};
parse(Phase, <<?CR>>, State) ->
    {more, fun(Bin) -> parse(Phase, <<?CR, Bin/binary>>, State) end};
parse(Phase, <<?CR, ?LF, Rest/binary>>, State) ->
    parse(Phase, <<?LF, Rest/binary>>, State);
parse(_Phase, <<?CR, _Ch:8, _Rest/binary>>, _State) ->
    {error, linefeed_expected};
parse(Phase, <<?BSL>>, State) when Phase =:= hdname; Phase =:= hdvalue ->
    {more, fun(Bin) -> parse(Phase, <<?BSL, Bin/binary>>, State) end};
parse(Phase, <<?BSL, Ch:8, Rest/binary>>, State) when Phase =:= hdname; Phase =:= hdvalue ->
    parse(Phase, Rest, acc(unescape(Ch), State));

parse(command, <<?LF, Rest/binary>>, State = #parser_state{acc = Acc}) ->
    parse(headers, Rest, State#parser_state{cmd = Acc, acc = <<>>});
parse(command, <<Ch:8, Rest/binary>>, State) ->
    parse(command, Rest, acc(Ch, State));

parse(headers, <<?LF, Rest/binary>>, State) ->
    parse(body, Rest, State, content_len(State#parser_state{acc = <<>>}));
parse(headers, Bin, State) ->
    parse(hdname, Bin, State);

parse(hdname, <<?LF, _Rest/binary>>, _State) ->
    {error, unexpected_linefeed};
parse(hdname, <<?COLON, Rest/binary>>, State = #parser_state{acc = Acc}) ->
    parse(hdvalue, Rest, State#parser_state{hdname = Acc, acc = <<>>});
parse(hdname, <<Ch:8, Rest/binary>>, State) ->
    parse(hdname, Rest, acc(Ch, State));

parse(hdvalue, <<?LF, Rest/binary>>, State = #parser_state{headers = Headers, hdname = Name, acc = Acc}) ->
    parse(headers, Rest, State#parser_state{headers = add_header(Name, Acc, Headers), hdname = undefined, acc = <<>>});
parse(hdvalue, <<Ch:8, Rest/binary>>, State) ->
    parse(hdvalue, Rest, acc(Ch, State)).

parse(body, <<>>, State, Length) ->
    {more, fun(Bin) -> parse(body, Bin, State, Length) end};
parse(body, Bin, State, none) ->
    case binary:split(Bin, <<?NULL>>) of
        [Chunk, Rest] ->
            {ok, new_frame(acc(Chunk, State)), Rest};
        [Chunk] ->
            {more, fun(More) -> parse(body, More, acc(Chunk, State), none) end}
    end;
parse(body, Bin, State, Len) when byte_size(Bin) >= (Len+1) ->
    <<Chunk:Len/binary, ?NULL, Rest/binary>> = Bin,
    {ok, new_frame(acc(Chunk, State)), Rest};
parse(body, Bin, State, Len) ->
    {more, fun(More) -> parse(body, More, acc(Bin, State), Len - byte_size(Bin)) end}.

add_header(Name, Value, Headers) ->
    case lists:keyfind(Name, 1, Headers) of
        {Name, _} -> Headers;
        false     -> [{Name, Value} | Headers]
    end.

content_len(#parser_state{headers = Headers}) ->
    case lists:keyfind(<<"content-length">>, 1, Headers) of
        {_, Val} -> list_to_integer(binary_to_list(Val));
        false    -> none
    end.

new_frame(#parser_state{cmd = Cmd, headers = Headers, acc = Acc}) ->
    #stomp_frame{command = Cmd, headers = Headers, body = Acc}.

acc(Chunk, State = #parser_state{acc = Acc}) when is_binary(Chunk) ->
    State#parser_state{acc = <<Acc/binary, Chunk/binary>>};
acc(Ch, State = #parser_state{acc = Acc}) ->
    State#parser_state{acc = <<Acc/binary, Ch:8>>}.

%% \r (octet 92 and 114) translates to carriage return (octet 13)
%% \n (octet 92 and 110) translates to line feed (octet 10)
%% \c (octet 92 and 99) translates to : (octet 58)
%% \\ (octet 92 and 92) translates to \ (octet 92)
unescape($r)  -> ?CR;
unescape($n)  -> ?LF;
unescape($c)  -> ?COLON;
unescape($\\) -> ?BSL;
unescape(_Ch) -> {error, cannnot_unescape}.

serialize(#stomp_frame{command = Cmd, headers = Headers, body = Body}) ->
    Headers1 = lists:keydelete(<<"content-length">>, 1, Headers),
    Headers2 =
    case iolist_size(Body) of
        0   -> Headers1;
        Len -> Headers1 ++ [{<<"content-length">>, Len}]
    end,
    [Cmd, ?LF, [serialize(header, Header) || Header <- Headers2], ?LF, Body, 0].

serialize(header, {Name, Val}) when is_integer(Val) ->
    [escape(Name), ?COLON, integer_to_list(Val), ?LF];
serialize(header, {Name, Val}) ->
    [escape(Name), ?COLON, escape(Val), ?LF].

escape(Bin) when is_binary(Bin) ->
    << <<(escape(Ch))/binary>> || <<Ch>> <= Bin >>;
escape(?CR)    -> <<?BSL, $r>>;
escape(?LF)    -> <<?BSL, $n>>;
escape(?BSL)   -> <<?BSL, ?BSL>>;
escape(?COLON) -> <<?BSL, $c>>;
escape(Ch)     -> <<Ch>>.


%% @doc Make a frame
make(<<"CONNECTED">>, Headers) ->
    #stomp_frame{command = <<"CONNECTED">>,
                 headers = [{<<"server">>, ?STOMP_SERVER} | Headers]};

make(Command, Headers) ->
    #stomp_frame{command = Command, headers = Headers}.

make(Command, Headers, Body) ->
    #stomp_frame{command = Command, headers = Headers, body = Body}.

%% @doc Format a frame
format(Frame) -> serialize(Frame).

