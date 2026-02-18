%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_uns_gate_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([start/2, stop/1]).
-export([on_config_changed/2, on_handle_api_call/4]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_uns_gate_sup:start_link(),
    _ = spawn(fun() -> ensure_default_plugin_config_retry(5) end),
    ok = emqx_uns_gate_config:load(),
    ok = emqx_uns_gate:hook(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_uns_gate:unhook().

on_config_changed(_OldConfig, NewConfig) ->
    emqx_uns_gate_config:update(NewConfig).

on_handle_api_call(Method, PathRemainder, Request, _Context) ->
    emqx_uns_gate_api:handle(Method, PathRemainder, Request).

ensure_default_plugin_config() ->
    NameVsn = plugin_name_vsn(),
    Missing = '$uns_gate_missing_config',
    case emqx_plugins:get_config(NameVsn, Missing) of
        Missing ->
            Defaults = #{
                <<"enabled">> => true,
                <<"on_mismatch">> => <<"deny">>,
                <<"allow_intermediate_publish">> => false,
                <<"validate_payload">> => true,
                <<"exempt_topics">> => [<<"$SYS/#">>, <<"$share/#">>]
            },
            case emqx_plugins:update_config(NameVsn, Defaults) of
                ok -> ok;
                {error, _} -> ok
            end;
        _ ->
            ok
    end.

ensure_default_plugin_config_retry(0) ->
    ok;
ensure_default_plugin_config_retry(N) ->
    timer:sleep(200),
    NameVsn = plugin_name_vsn(),
    Missing = '$uns_gate_missing_config',
    case emqx_plugins:get_config(NameVsn, Missing) of
        Missing ->
            ensure_default_plugin_config(),
            ensure_default_plugin_config_retry(N - 1);
        _ ->
            ok
    end.

plugin_name_vsn() ->
    App = <<"emqx_uns_gate">>,
    Vsn =
        case application:get_key(emqx_uns_gate, vsn) of
            {ok, V} when is_list(V) -> unicode:characters_to_binary(V);
            {ok, V} when is_binary(V) -> V;
            _ -> <<"1.0.0">>
        end,
    <<App/binary, "-", Vsn/binary>>.
