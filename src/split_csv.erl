-module(split_csv).

-export([split/2]).

-export([insert/1]).

split(File, Size) ->
    {_, Pid} = file:open(File, []),
    start_split(Pid, Size, 1, 1, []),
    ok.

start_split(Pid, Size, FileSeq, Link, Acc) when Link =:= Size ->
    file:write_file(lists:concat([FileSeq,".csv"]), Acc),
    case file:read_line(Pid) of
        eof -> ok;
        {ok, Bin} ->
            start_split(Pid, Size, FileSeq + 1, 1, [Bin|[]])

    end;
start_split(Pid, Size, FileSeq, Link, Acc) ->
    case file:read_line(Pid) of
        eof -> ok;
        {ok, Bin} ->
            start_split(Pid, Size, FileSeq, Link+1, [Bin|Acc])

    end.

insert(Size) ->   
    Bin = [io_lib:format("~s~n", [lists:concat(["hello", "_", Seq])])  || Seq <-lists:seq(1, Size)],
    file:write_file("test.csv", Bin).