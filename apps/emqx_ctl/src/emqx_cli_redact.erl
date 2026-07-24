%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cli_redact).

-export([
    redact_all/1,
    redact_options/2
]).

-define(REDACTED, "******").

redact_all(Args) ->
    [?REDACTED || _ <- Args].

redact_options(Args, Options) ->
    redact_options(Args, Options, []).

redact_options([], _Options, Acc) ->
    lists:reverse(Acc);
redact_options([Option, Value | Rest], Options, Acc) ->
    case lists:member(Option, Options) of
        true -> redact_options(Rest, Options, [?REDACTED, Option | Acc]);
        false -> redact_options([Value | Rest], Options, [Option | Acc])
    end;
redact_options([Arg], _Options, Acc) ->
    lists:reverse([Arg | Acc]).
