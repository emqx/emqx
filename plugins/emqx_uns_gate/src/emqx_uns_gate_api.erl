%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_uns_gate_api).

-export([handle/3]).

handle(get, [<<"status">>], _Request) ->
    {ok, 200, #{}, #{
        plugin => <<"emqx_uns_gate">>,
        enabled => emqx_uns_gate_config:enabled(),
        on_mismatch => emqx_uns_gate_config:on_mismatch(),
        allow_intermediate_publish => emqx_uns_gate_config:allow_intermediate_publish(),
        exempt_topics => emqx_uns_gate_config:exempt_topics()
    }};
handle(get, [<<"stats">>], Request) ->
    Stats = emqx_uns_gate_metrics:snapshot(),
    case wants_html(Request) of
        true ->
            {ok, 200, #{<<"content-type">> => <<"text/html; charset=utf-8">>}, stats_html(Stats)};
        false ->
            {ok, 200, #{}, Stats}
    end;
handle(get, [<<"model">>], _Request) ->
    case emqx_uns_gate_store:active_model() of
        {ok, Entry} ->
            {ok, 200, #{}, Entry};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"No active model">>}}
    end;
handle(get, [<<"models">>], _Request) ->
    {ok, Entries} = emqx_uns_gate_store:list_models(),
    {ok, 200, #{}, #{data => Entries}};
handle(get, [<<"models">>, Id], _Request) ->
    case emqx_uns_gate_store:get_model(Id) of
        {ok, Entry} ->
            {ok, 200, #{}, Entry};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Model not found">>}}
    end;
handle(post, [<<"models">>], Request) ->
    Body = maps:get(body, Request, #{}),
    Activate = get_activate_flag(Body),
    Model = get_model_body(Body),
    case emqx_uns_gate_store:put_model(Model, Activate) of
        {ok, Entry} ->
            {ok, 200, #{}, Entry};
        {error, Reason} ->
            bad_model(Reason)
    end;
handle(post, [<<"models">>, Id, <<"activate">>], _Request) ->
    case emqx_uns_gate_store:activate(Id) of
        ok ->
            {ok, 200, #{}, #{id => Id, active => true}};
        {error, not_found} ->
            {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Model not found">>}}
    end;
handle(post, [<<"validate">>, <<"topic">>], Request) ->
    Topic = get_topic(maps:get(body, Request, #{})),
    case Topic of
        <<>> ->
            {error, 400, #{}, #{
                code => <<"BAD_REQUEST">>,
                message => <<"topic is required">>
            }};
        _ ->
            Result = emqx_uns_gate_store:validate_topic(Topic),
            {ok, 200, #{}, #{
                topic => Topic,
                result => format_validate_result(Result)
            }}
    end;
handle(_Method, _Path, _Request) ->
    {error, not_found}.

bad_model(Reason) ->
    {error, 400, #{}, #{
        code => <<"BAD_MODEL">>,
        message => iolist_to_binary(io_lib:format("~p", [Reason]))
    }}.

get_activate_flag(#{<<"activate">> := V}) ->
    normalize_bool(V);
get_activate_flag(#{activate := V}) ->
    normalize_bool(V);
get_activate_flag(_) ->
    false.

get_model_body(#{<<"model">> := Model}) when is_map(Model) ->
    Model;
get_model_body(#{model := Model}) when is_map(Model) ->
    Model;
get_model_body(Body) ->
    Body.

get_topic(#{<<"topic">> := Topic}) ->
    to_bin(Topic);
get_topic(#{topic := Topic}) ->
    to_bin(Topic);
get_topic(_) ->
    <<>>.

format_validate_result(allow) ->
    #{valid => true};
format_validate_result({deny, Reason}) ->
    #{valid => false, reason => Reason}.

normalize_bool(true) -> true;
normalize_bool(<<"true">>) -> true;
normalize_bool("true") -> true;
normalize_bool(_) -> false.

to_bin(V) when is_binary(V) -> V;
to_bin(V) when is_atom(V) -> atom_to_binary(V, utf8);
to_bin(V) when is_list(V) -> unicode:characters_to_binary(V);
to_bin(_) -> <<>>.

wants_html(Request) ->
    Query = maps:get(query_string, Request, #{}),
    case maps:get(<<"format">>, Query, <<>>) of
        <<"html">> ->
            true;
        _ ->
            Headers = maps:get(headers, Request, #{}),
            Accept = to_bin(maps:get(<<"accept">>, Headers, <<>>)),
            AcceptLower = unicode:characters_to_binary(string:lowercase(Accept)),
            binary:match(AcceptLower, <<"text/html">>) =/= nomatch
    end.

stats_html(Stats) ->
    MsgTotal = maps:get(messages_total, Stats, 0),
    MsgAllowed = maps:get(messages_allowed, Stats, 0),
    MsgDropped = maps:get(messages_dropped, Stats, 0),
    Exempt = maps:get(exempt, Stats, 0),
    TopicInvalid = maps:get(topic_invalid, Stats, 0),
    PayloadInvalid = maps:get(payload_invalid, Stats, 0),
    NotEndpoint = maps:get(not_endpoint, Stats, 0),
    Uptime = maps:get(uptime_seconds, Stats, 0),
    iolist_to_binary([
        <<"<!doctype html><html><head><meta charset=\"utf-8\">">>,
        <<"<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">">>,
        <<"<title>UNS Gate Metrics</title>">>,
        <<"<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:20px;}table{border-collapse:collapse;width:100%;max-width:920px;}th,td{border:1px solid #ddd;padding:8px 10px;text-align:left;}th{background:#f7f7f7;width:280px;}</style>">>,
        <<"</head><body><h1>UNS Gate Metrics</h1>">>,
        <<"<p>Endpoint: <code>/api/v5/plugin_api/emqx_uns_gate/stats</code></p>">>,
        <<"<table>">>,
        row(<<"messages_total">>, MsgTotal),
        row(<<"messages_allowed">>, MsgAllowed),
        row(<<"messages_dropped">>, MsgDropped),
        row(<<"exempt">>, Exempt),
        row(<<"topic_invalid">>, TopicInvalid),
        row(<<"payload_invalid">>, PayloadInvalid),
        row(<<"not_endpoint">>, NotEndpoint),
        row(<<"uptime_seconds">>, Uptime),
        <<"</table></body></html>">>
    ]).

row(Key, Value) ->
    KeyBin = to_bin(Key),
    ValBin = to_bin(integer_to_binary(Value)),
    [<<"<tr><th>">>, KeyBin, <<"</th><td>">>, ValBin, <<"</td></tr>">>].
