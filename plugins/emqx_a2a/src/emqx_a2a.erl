%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a).

-moduledoc """
Hook callbacks for A2A agent orchestration.

Intercepts messages published to `$a2a/request/+` topics, parses
JSON-RPC requests, and dispatches to the session manager which
spawns agent worker processes.
""".

-export([hook/0, unhook/0, on_message_publish/1]).

-include_lib("emqx/include/emqx_hooks.hrl").
-include("emqx_a2a.hrl").

-define(PUBLISH_HOOK, {?MODULE, on_message_publish, []}).

hook() ->
    ok = emqx_hooks:put('message.publish', ?PUBLISH_HOOK, ?HP_SCHEMA_VALIDATION + 2).

unhook() ->
    emqx_hooks:del('message.publish', ?PUBLISH_HOOK).

on_message_publish(Message) ->
    Topic = emqx_message:topic(Message),
    case Topic of
        <<"$a2a/", _/binary>> ->
            case match_request_topic(Topic) of
                {ok, AgentOrWorkflow} ->
                    handle_a2a_request(AgentOrWorkflow, Message);
                nomatch ->
                    {ok, Message}
            end;
        _ ->
            {ok, Message}
    end.

%% Match $a2a/request/{id}
match_request_topic(Topic) ->
    Prefix = emqx_a2a_config:request_topic_prefix(),
    PrefixLen = byte_size(Prefix),
    case Topic of
        <<Prefix:PrefixLen/binary, Rest/binary>> when Rest =/= <<>> ->
            {ok, Rest};
        _ ->
            nomatch
    end.

handle_a2a_request(TargetId, Message) ->
    Payload = emqx_message:payload(Message),
    case parse_jsonrpc_request(Payload) of
        {ok, Request} ->
            %% Extract MQTT v5 properties once
            Props = emqx_message:get_header('User-Property', Message, []),
            ReplyTopic = get_reply_topic(Props, TargetId),
            Correlation = get_correlation(Props),
            ?LOG(info, #{
                msg => "a2a_request",
                target => TargetId,
                reply_topic => ReplyTopic
            }),
            emqx_a2a_session_mgr:dispatch(TargetId, Request, ReplyTopic, Correlation),
            %% Stop the message — we've handled it, don't route to subscribers
            {stop, Message};
        {error, Reason} ->
            ?LOG(warning, #{msg => "bad_a2a_request", reason => Reason}),
            {ok, Message}
    end.

parse_jsonrpc_request(Payload) ->
    try emqx_utils_json:decode(Payload, [return_maps]) of
        #{<<"method">> := <<"tasks/send">>, <<"params">> := Params} = Req ->
            Text = extract_text(Params),
            Variables = extract_variables(Params),
            RequestId = maps:get(<<"id">>, Req, generate_id()),
            {ok, #{
                text => Text,
                variables => Variables,
                request_id => RequestId,
                params => Params
            }};
        #{<<"method">> := Method} ->
            {error, #{cause => unsupported_method, method => Method}};
        %% Also accept simple format: {"text": "...", "variables": {...}}
        #{<<"text">> := Text} = Simple ->
            Variables = maps:get(<<"variables">>, Simple, #{}),
            {ok, #{
                text => Text,
                variables => Variables,
                request_id => generate_id(),
                params => Simple
            }};
        _ ->
            {error, invalid_request}
    catch
        _:_ -> {error, invalid_json}
    end.

extract_text(#{<<"message">> := #{<<"parts">> := Parts}}) ->
    TextParts = [T || #{<<"type">> := <<"text">>, <<"text">> := T} <- Parts],
    iolist_to_binary(lists:join(<<"\n">>, TextParts));
extract_text(#{<<"message">> := #{<<"text">> := Text}}) ->
    Text;
extract_text(#{<<"text">> := Text}) ->
    Text;
extract_text(_) ->
    <<>>.

extract_variables(#{<<"metadata">> := #{<<"variables">> := Vars}}) when is_map(Vars) ->
    Vars;
extract_variables(#{<<"variables">> := Vars}) when is_map(Vars) ->
    Vars;
extract_variables(_) ->
    #{}.

get_reply_topic(Props, TargetId) when is_list(Props) ->
    case proplists:get_value(<<"response-topic">>, Props) of
        undefined -> <<"$a2a/reply/", TargetId/binary>>;
        RT -> RT
    end;
get_reply_topic(_, TargetId) ->
    <<"$a2a/reply/", TargetId/binary>>.

get_correlation(Props) when is_list(Props) ->
    proplists:get_value(<<"correlation-data">>, Props, <<>>);
get_correlation(_) ->
    <<>>.

generate_id() ->
    emqx_guid:to_hexstr(emqx_guid:gen()).
