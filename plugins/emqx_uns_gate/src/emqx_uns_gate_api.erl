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
