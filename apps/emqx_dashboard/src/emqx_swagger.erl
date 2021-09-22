-module(emqx_swagger).

-include_lib("typerefl/include/types.hrl").
-import(hoconsc, [mk/2]).

-define(MAX_ROW_LIMIT, 100).

%% API
-export([fields/1]).
-export([error_codes/1, error_codes/2]).

fields(page) ->
    [{page,
        mk(integer(),
            #{
                in => query,
                desc => <<"Page number of the results to fetch.">>,
                default => 1,
                example => 1})
    }];
fields(limit) ->
    [{limit,
        mk(range(1, ?MAX_ROW_LIMIT),
            #{
                in => query,
                desc => iolist_to_binary([<<"Results per page(max ">>,
                    integer_to_binary(?MAX_ROW_LIMIT), <<")">>]),
                default => ?MAX_ROW_LIMIT,
                example => 50
            })
    }].

error_codes(Codes) ->
    error_codes(Codes, <<"Error code to troubleshoot problems.">>).

error_codes(Codes = [_ | _], MsgExample) ->
    [code(Codes), message(MsgExample)].

message(Example) ->
    {message, mk(string(), #{
        desc => <<"Detailed description of the error.">>,
        example => Example
    })}.

code(Codes) ->
    {code, mk(hoconsc:enum(Codes), #{})}.
