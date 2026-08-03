%%--------------------------------------------------------------------
%% Copyright (c) 2019, 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_dist).

-export([
    listen/1,
    listen/2,
    select/1,
    accept/1,
    accept_connection/5,
    setup/5,
    close/1,
    childspecs/0
]).

-export([port/1]).

-define(DEFAULT_PORT, 4370).
-define(MIN_RAND_PORT, 10000).
-define(MAX_PORT_LIMIT, 60000).

-elvis([{elvis_style, invalid_dynamic_call, disable}]).

%% This is not called since OTP 23 if listen/2 is exported.
%% kept for backward compatibility.
listen(Name) ->
    listen(Name, undefined).

listen(Name, Host) ->
    %% Here we figure out what port we want to listen on.
    Port = port(Name),

    case Port > 0 of
        true ->
            %% Set both "min" and "max" variables, to force the port number to
            %% this one.
            ok = application:set_env(kernel, inet_dist_listen_min, Port),
            ok = application:set_env(kernel, inet_dist_listen_max, Port);
        false ->
            ok = application:set_env(kernel, inet_dist_listen_min, ?MIN_RAND_PORT),
            ok = application:set_env(kernel, inet_dist_listen_max, ?MAX_PORT_LIMIT)
    end,
    %% Finally run the real function!
    with_module(fun(M) -> do_listen(M, Name, Host, Port) end).

do_listen(M, Name, Host, Port) ->
    case try_listen(M, Name, Host) of
        {error, eaddrinuse} when Port > 0 ->
            {error, "port " ++ integer_to_list(Port) ++ " is in use"};
        Other ->
            Other
    end.

%% The `undefined` in the first clause is from listen/1
try_listen(M, Name, undefined) -> M:listen(Name);
try_listen(M, Name, Host) -> M:listen(Name, Host).

select(Node) ->
    with_module(fun(M) -> M:select(Node) end).

accept(Listen) ->
    with_module(fun(M) -> M:accept(Listen) end).

accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime) ->
    with_module(fun(M) ->
        M:accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime)
    end).

setup(Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    with_module(fun(M) ->
        M:setup(Node, Type, MyNode, LongOrShortNames, SetupTime)
    end).

close(Listen) ->
    with_module(fun(M) -> M:close(Listen) end).

childspecs() ->
    with_module(fun(M) -> M:childspecs() end).

with_module(Fun) ->
    try
        Proto = resolve_proto(),
        Module = list_to_atom(Proto ++ "_dist"),
        Fun(Module)
    catch
        C:E ->
            %% this exception is caught by net_kernel
            error({failed_to_call_ekka_dist_module, C, E})
    end.

resolve_proto() ->
    %% FIXME: change application to emqx
    Fallback = atom_to_list(application:get_env(ekka, proto_dist, inet_tcp)),
    %% the -proto_dist boot arg is 'ekka'
    %% and there is a lack of a 'ekka_tls' module,
    %% Also when starting remote console etc, there is no application env to
    %% read from, so we have to find another way to pass the module name (prefix)
    Mod =
        case os:getenv("EKKA_PROTO_DIST_MOD") of
            false -> Fallback;
            "" -> Fallback;
            M -> M
        end,
    case Mod of
        "inet_tcp" -> ok;
        "inet_tls" -> ok;
        "inet6_tcp" -> ok;
        "inet6_tls" -> ok;
        Other -> error({unsupported_proto_dist, Other})
    end,
    Mod.

%% @doc Figure out dist port from node's name.
-spec port(node() | string()) -> inet:port_number().
port(Name) when is_atom(Name) ->
    port(atom_to_list(Name));
port("remsh" ++ _) ->
    %% outgoing port for remsh,
    %% it should never accept incoming connections
    %% i.e. no one else should need to know the actual port
    0;
port(Name) when is_list(Name) ->
    %% Figure out the base port.  If not specified using the
    %% inet_dist_base_port kernel environment variable, default to
    %% 4370, one above the epmd port.
    BasePort = application:get_env(kernel, inet_dist_base_port, ?DEFAULT_PORT),

    %% Now, figure out our "offset" on top of the base port.  The
    %% offset is the integer just to the left of the @ sign in our node
    %% name.  If there is no such number, the offset is 0.
    %%
    %% Also handle the case when no hostname was specified.
    BasePort + offset(Name).

%% @doc Figure out the offset by node's name
offset(NodeName) ->
    ShortName = re:replace(NodeName, "@.*$", ""),
    case re:run(ShortName, "[0-9]+$", [{capture, first, list}]) of
        nomatch ->
            0;
        {match, [OffsetAsString]} ->
            (list_to_integer(OffsetAsString) rem ?MAX_PORT_LIMIT)
    end.
