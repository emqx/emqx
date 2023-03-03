%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_telemetry).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-include_lib("kernel/include/file.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-logger_header("[Telemetry]").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-export([ start_link/1
        , stop/0
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_continue/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-export([ enable/0
        , disable/0
        , is_enabled/0
        , get_uuid/0
        , get_telemetry/0
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-import(proplists, [ get_value/2
                   , get_value/3
                   ]).

-record(telemetry, {
          id :: non_neg_integer(),

          uuid :: binary(),

          enabled :: boolean()
        }).

-record(state, {
          uuid :: undefined | binary(),

          enabled :: undefined | boolean(),

          url :: undefined | string(),

          report_interval :: undefined | non_neg_integer(),

          timer = undefined :: undefined | reference()
        }).

%% The count of 100-nanosecond intervals between the UUID epoch
%% 1582-10-15 00:00:00 and the UNIX epoch 1970-01-01 00:00:00.
-define(GREGORIAN_EPOCH_OFFSET, 16#01b21dd213814000).

-define(UNIQUE_ID, 9527).

-define(TELEMETRY, emqx_telemetry).

-define(HTTP_TIMEOUT, 10).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = ekka_mnesia:create_table(?TELEMETRY,
             [{type, set},
              {disc_copies, [node()]},
              {local_content, true},
              {record_name, telemetry},
              {attributes, record_info(fields, telemetry)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?TELEMETRY, disc_copies).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Opts], []).

stop() ->
    gen_server:stop(?MODULE).

enable() ->
    gen_server:call(?MODULE, enable, infinity).

disable() ->
    gen_server:call(?MODULE, disable, infinity).

is_enabled() ->
    gen_server:call(?MODULE, is_enabled, infinity).

get_uuid() ->
    gen_server:call(?MODULE, get_uuid, infinity).

get_telemetry() ->
    gen_server:call(?MODULE, get_telemetry, infinity).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Opts]) ->
    State = #state{url = get_value(url, Opts),
                   report_interval = timer:seconds(get_value(report_interval, Opts))},
    NState = case mnesia:dirty_read(?TELEMETRY, ?UNIQUE_ID) of
                 [] ->
                     Enabled = case search_telemetry_enabled() of
                                   {error, not_found} ->
                                       get_value(enabled, Opts);
                                   {M, F} ->
                                       erlang:apply(M, F, [])
                               end,
                     UUID = generate_uuid(),
                     mnesia:dirty_write(?TELEMETRY, #telemetry{id = ?UNIQUE_ID,
                                                               uuid = UUID,
                                                               enabled = Enabled}),
                     State#state{enabled = Enabled, uuid = UUID};
                 [#telemetry{uuid = UUID, enabled = Enabled} | _] ->
                     State#state{enabled = Enabled, uuid = UUID}
             end,
    case official_version(emqx_app:get_release()) of
        true ->
            _ = erlang:send(self(), first_report),
            {ok, NState};
        false ->
            {ok, NState#state{enabled = false}}
    end.

handle_call(enable, _From, State = #state{uuid = UUID}) ->
    mnesia:dirty_write(?TELEMETRY, #telemetry{id = ?UNIQUE_ID,
                                              uuid = UUID,
                                              enabled = true}),
    _ = erlang:send(self(), first_report),
    {reply, ok, State#state{enabled = true}};

handle_call(disable, _From, State = #state{uuid = UUID}) ->
    mnesia:dirty_write(?TELEMETRY, #telemetry{id = ?UNIQUE_ID,
                                              uuid = UUID,
                                              enabled = false}),
    {reply, ok, State#state{enabled = false}};

handle_call(is_enabled, _From, State = #state{enabled = Enabled}) ->
    {reply, Enabled, State};

handle_call(get_uuid, _From, State = #state{uuid = UUID}) ->
    {reply, {ok, UUID}, State};

handle_call(get_telemetry, _From, State) ->
    {reply, {ok, get_telemetry(State)}, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_continue(Continue, State) ->
    ?LOG(error, "Unexpected continue: ~p", [Continue]),
    {noreply, State}.

handle_info(first_report, State) ->
    case is_pid(erlang:whereis(emqx)) of
        true ->
            report_telemetry(State),
            {noreply, ensure_report_timer(State)};
        false ->
            _ = erlang:send_after(1000, self(), first_report),
            {noreply, State}
    end;
handle_info({timeout, TRef, time_to_report_telemetry_data}, State = #state{timer = TRef,
                                                                           enabled = false}) ->
    {noreply, State};
handle_info({timeout, TRef, time_to_report_telemetry_data}, State = #state{timer = TRef}) ->
    report_telemetry(State),
    {noreply, ensure_report_timer(State)};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

official_version(Version) ->
    Pt = "^\\d+\\.\\d+(?:(-(?:alpha|beta|rc)\\.[1-9][0-9]*)|\\.\\d+)$",
    match =:= re:run(Version, Pt, [{capture, none}]).

ensure_report_timer(State = #state{report_interval = ReportInterval}) ->
    State#state{timer = emqx_misc:start_timer(ReportInterval, time_to_report_telemetry_data)}.

license() ->
    case search_telemetry_license() of
        {error, not_found} ->
            [{edition, <<"community">>}];
        {M, F} ->
            erlang:apply(M, F, [])
    end.

os_info() ->
    case erlang:system_info(os_type) of
        {unix,darwin} ->
            [Name | _] = string:tokens(os:cmd("sw_vers -productName"), "\n"),
            [Version | _] = string:tokens(os:cmd("sw_vers -productVersion"), "\n"),
            [{os_name, Name},
             {os_version, Version}];
        {unix, _} ->
            case file:read_file("/etc/os-release") of
                {error, _} ->
                    [{os_name, "Unknown"},
                     {os_version, "Unknown"}];
                {ok, FileContent} ->
                    OSInfo = lists:foldl(fun(Line, Acc) ->
                                                 [Var, Value] = string:tokens(Line, "="),
                                                 NValue = case Value of
                                                              _ when is_list(Value) ->
                                                                  lists:nth(1, string:tokens(Value, "\""));
                                                              _ ->
                                                                  Value
                                                          end,
                                                 [{Var, NValue} | Acc]
                                         end, [], string:tokens(binary:bin_to_list(FileContent), "\n")),
                    [{os_name, get_value("NAME", OSInfo)},
                     {os_version, get_value("VERSION", OSInfo, get_value("VERSION_ID", OSInfo, get_value("PRETTY_NAME", OSInfo)))}]
            end;
        {win32, nt} ->
            Ver = os:cmd("ver"),
            case re:run(Ver, "[a-zA-Z ]+ \\[Version ([0-9]+[\.])+[0-9]+\\]", [{capture, none}]) of
                match ->
                    [NVer | _] = string:tokens(Ver, "\r\n"),
                    {match, [Version]} = re:run(NVer, "([0-9]+[\.])+[0-9]+", [{capture, first, list}]),
                    [Name | _] = string:split(NVer, " [Version "),
                    [{os_name, Name},
                     {os_version, Version}];
                nomatch ->
                    [{os_name, "Unknown"},
                     {os_version, "Unknown"}]
            end
    end.

otp_version() ->
    erlang:system_info(otp_release).

uptime() ->
    element(1, erlang:statistics(wall_clock)).

nodes_uuid() ->
    Nodes = lists:delete(node(), ekka_mnesia:running_nodes()),
    lists:foldl(fun(Node, Acc) ->
                    case rpc:call(Node, ?MODULE, get_uuid, []) of
                        {badrpc, _Reason} ->
                            Acc;
                        {ok, UUID} ->
                            [UUID | Acc]
                    end
                end, [], Nodes).

active_plugins() ->
    lists:foldl(fun(#plugin{name = Name, active = Active}, Acc) ->
                        case Active of
                            true -> [Name | Acc];
                            false -> Acc
                        end
                    end, [], emqx_plugins:list()).

active_modules() ->
    lists:foldl(fun({Name, Persistent}, Acc) ->
                    case Persistent of
                        true -> [Name | Acc];
                        false -> Acc
                    end
                end, [], emqx_modules:list()).

num_clients() ->
    emqx_stats:getstat('connections.max').

messages_sent() ->
    emqx_metrics:val('messages.sent').

messages_received() ->
    emqx_metrics:val('messages.received').

generate_uuid() ->
    MicroSeconds = erlang:system_time(microsecond),
    Timestamp = MicroSeconds * 10 + ?GREGORIAN_EPOCH_OFFSET,
    <<TimeHigh:12, TimeMid:16, TimeLow:32>> = <<Timestamp:60>>,
    <<ClockSeq:32>> = crypto:strong_rand_bytes(4),
    <<First:7, _:1, Last:40>> = crypto:strong_rand_bytes(6),
    <<NTimeHigh:16>> = <<16#01:4, TimeHigh:12>>,
    <<NClockSeq:16>> = <<1:1, 0:1, ClockSeq:14>>,
    <<Node:48>> = <<First:7, 1:1, Last:40>>,
    list_to_binary(io_lib:format("~.16B-~.16B-~.16B-~.16B-~.16B", [TimeLow, TimeMid, NTimeHigh, NClockSeq, Node])).

get_telemetry(#state{uuid = UUID}) ->
    OSInfo = os_info(),
    [{emqx_version, bin(emqx_app:get_release())},
     {license, license()},
     {os_name, bin(get_value(os_name, OSInfo))},
     {os_version, bin(get_value(os_version, OSInfo))},
     {otp_version, bin(otp_version())},
     {up_time, uptime()},
     {uuid, UUID},
     {nodes_uuid, nodes_uuid()},
     {active_plugins, active_plugins()},
     {active_modules, active_modules()},
     {num_clients, num_clients()},
     {messages_received, messages_received()},
     {messages_sent, messages_sent()}].

report_telemetry(State = #state{url = URL}) ->
    Data = get_telemetry(State),
    case emqx_json:safe_encode(Data) of
        {ok, Bin} ->
            _ = httpc_request(post, URL, [], Bin),
            ?tp(debug, telemetry_data_reported, #{});
        {error, Reason} ->
            %% debug? why?
            ?tp(debug, telemetry_data_encode_error, #{data => Data, reason => Reason})
    end.

%% we might set url = undefined in testcase
httpc_request(_, undefined, _, _) ->
    ignore;
httpc_request(Method, URL, Headers, Body) ->
    HTTPOptions = [{timeout, timer:seconds(?HTTP_TIMEOUT)}, {ssl, [{verify, verify_none}]}],
    Options = [],
    httpc:request(Method, {URL, Headers, "application/json", Body}, HTTPOptions, Options).

ignore_lib_apps(Apps) ->
    LibApps = [kernel, stdlib, sasl, appmon, eldap, erts,
               syntax_tools, ssl, crypto, mnesia, os_mon,
               inets, goldrush, gproc, runtime_tools,
               snmp, otp_mibs, public_key, asn1, ssh, hipe,
               common_test, observer, webtool, xmerl, tools,
               test_server, compiler, debugger, eunit, et,
               wx],
    [AppName || {AppName, _, _} <- Apps, not lists:member(AppName, LibApps)].

search_telemetry_license() ->
    search_function(telemetry_license).

search_telemetry_enabled() ->
    search_function(telemetry_enabled).

search_function(Name) ->
    case search_attrs(Name) of
        [] ->
            {error, not_found};
        Callbacks ->
            case lists:filter(fun({M, F}) ->
                                  erlang:function_exported(M, F, 0)
                              end, Callbacks) of
                [] -> {error, not_found};
                [{M, F} | _] -> {M, F}
            end
    end.

search_attrs(Name) ->
    Apps = ignore_lib_apps(application:loaded_applications()),
    search_attrs(Name, Apps).

search_attrs(Name, Apps) ->
    lists:foldl(fun(App, Acc) ->
                    {ok, Modules} = application:get_key(App, modules),
                    Attrs = lists:foldl(fun(Module, Acc0) ->
                                                case proplists:get_value(Name, module_attributes(Module), undefined) of
                                                    undefined -> Acc0;
                                                    Attrs0 -> Acc0 ++ Attrs0
                                                end
                                            end, [], Modules),
                    Acc ++ Attrs
                end, [], Apps).

module_attributes(Module) ->
    try Module:module_info(attributes)
    catch
        error:undef -> [];
        error:Reason -> error(Reason)
    end.

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(A) when is_atom(A) ->
    atom_to_binary(A);
bin(B) when is_binary(B) ->
    B.
