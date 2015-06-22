%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd utility functions. 
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_util).

-author("Feng Lee <feng@emqtt.io>").

-export([apply_module_attributes/1,
         all_module_attributes/1,
         cancel_timer/1]).

-export([integer_to_binary/1]).

%% only {F, Args}...
apply_module_attributes(Name) ->
    [{Module, [apply(Module, F, Args) || {F, Args} <- Attrs]} || 
        {_App, Module, Attrs} <- all_module_attributes(Name)].

%% copy from rabbit_misc.erl
all_module_attributes(Name) ->
    Targets =
        lists:usort(
          lists:append(
            [[{App, Module} || Module <- Modules] ||
                {App, _, _}   <- ignore_lib_apps(application:loaded_applications()),
                {ok, Modules} <- [application:get_key(App, modules)]])),
    lists:foldl(
      fun ({App, Module}, Acc) ->
              case lists:append([Atts || {N, Atts} <- module_attributes(Module),
                                         N =:= Name]) of
                  []   -> Acc;
                  Atts -> [{App, Module, Atts} | Acc]
              end
      end, [], Targets).

%% copy from rabbit_misc.erl
module_attributes(Module) ->
    case catch Module:module_info(attributes) of
        {'EXIT', {undef, [{Module, module_info, _} | _]}} ->
            [];
        {'EXIT', Reason} ->
            exit(Reason);
        V ->
            V
    end.

ignore_lib_apps(Apps) ->
    LibApps = [kernel, stdlib, sasl,
               syntax_tools, ssl, crypto,
               mnesia, os_mon, inets,
               goldrush, lager, gproc,
               runtime_tools, snmp, otp_mibs,
               public_key, asn1, ssh,
               common_test, observer, webtool,
               xmerl, tools, test_server,
               compiler, debugger, eunit,
               et, gen_logger, wx,
               hipe, esockd, mochiweb],
    [App || App = {Name, _, _} <- Apps, not lists:member(Name, LibApps)].


cancel_timer(undefined) -> 
	undefined;
cancel_timer(Ref) -> 
	catch erlang:cancel_timer(Ref).

integer_to_binary(I) when is_integer(I) ->
    list_to_binary(integer_to_list(I)).


