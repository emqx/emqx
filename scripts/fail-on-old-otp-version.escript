#!/usr/bin/env escript

-mode(compile).

main(_) ->
    OtpRelease = list_to_integer(erlang:system_info(otp_release)),
    case OtpRelease < 21 of
        true ->
            io:format(standard_error, "ERROR: Erlang/OTP version ~p found. required_min=21, recommended=23~n", [OtpRelease]),
            halt(1);
        false ->
            ok
    end.
