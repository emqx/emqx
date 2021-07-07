#!/usr/bin/env escript

%% This script reads up emqx.conf and split the sections
%% and dump sections to separate files.
%% Sections are grouped between CONFIG_SECTION_BGN and
%% CONFIG_SECTION_END pairs
%%
%% NOTE: this feature is so far not used in opensource
%% edition due to backward-compatibility reasons.

-mode(compile).

main(_) ->
    BaseConf = "apps/emqx/etc/emqx.conf",
    {ok, Bin} = file:read_file(BaseConf),
    Apps = filelib:wildcard("emqx_*", "apps/"),
    Conf = lists:foldl(fun(App, Acc) ->
        case lists:member(App, ["emqx_exhook",
                                "emqx_exproto",
                                "emqx_lwm2m",
                                "emqx_sn",
                                "emqx_coap",
                                "emqx_stomp",
                                "emqx_dashboard"]) of
            true -> Acc;
            false ->
                Filename = filename:join([apps, App, "etc", App]) ++ ".conf",
                case filelib:is_regular(Filename) of
                    true ->
                        {ok, Bin1} = file:read_file(Filename),
                        [Acc, io_lib:nl(), Bin1];
                    false -> Acc
                end
        end
    end, Bin, Apps),
    ok = file:write_file("apps/emqx/etc/emqx.conf.all", Conf).
