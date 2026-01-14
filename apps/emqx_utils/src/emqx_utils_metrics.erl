%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_metrics).

-export([
    format_hists/2,
    format_hist/3,
    print_hists/2,
    print_hist/3
]).

-define(FORMAT_WIDTH, 30).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

format_hists(WorkerId, Id) ->
    #{hists := Hists} = emqx_metrics_worker:get_metrics(WorkerId, Id),
    lists:map(fun(HistName) -> format_hist(WorkerId, Id, HistName) end, maps:keys(Hists)).

format_hist(WorkerId, Id, HistName) ->
    #{hists := #{HistName := #{count := Count, bucket_counts := IncrementalBucketCounts}}} =
        emqx_metrics_worker:get_metrics(WorkerId, Id),
    do_format_hist(Id, HistName, Count, IncrementalBucketCounts).

print_hists(WorkerId, Id) ->
    io:format("~s~n", [format_hists(WorkerId, Id)]).

print_hist(WorkerId, Id, HistName) ->
    io:format("~s~n", [format_hist(WorkerId, Id, HistName)]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_format_hist(Id, HistName, Count, IncrementalBucketCounts) ->
    BucketCounts = bucket_counts(IncrementalBucketCounts),
    {_, Counts} = lists:unzip(BucketCounts),
    MaxCount = lists:max(Counts),
    Rows0 = [format_row(BucketCount, MaxCount) || BucketCount <- BucketCounts],
    Rows1 = format_labels(Rows0),
    [
        io_lib:format("------~n~p.~p~n", [Id, HistName]),
        lists:map(
            fun([Label, Bar, RowCount]) ->
                io_lib:format("~s ~s~s~n", [Label, Bar, RowCount])
            end,
            Rows1
        ),
        io_lib:format("total: ~p~n", [Count])
    ].

%% Convert from incremental counts to individual bucket counts.
bucket_counts(IncrementalBucketCounts) ->
    trim_zeroes(do_bucket_counts([{0, 0} | IncrementalBucketCounts])).

trim_zeroes(BucketCounts0) ->
    BucketCounts1 = do_trim_zeroes(BucketCounts0),
    lists:reverse(do_trim_zeroes(lists:reverse(BucketCounts1))).

do_bucket_counts([{_, PrevCount}, {Bucket, Count} | IncrementalBucketCounts]) ->
    [{Bucket, Count - PrevCount} | do_bucket_counts([{Bucket, Count} | IncrementalBucketCounts])];
do_bucket_counts(_) ->
    [].

do_trim_zeroes([{Bucket, 0}, {_, 0} | Rest]) ->
    do_trim_zeroes([{Bucket, 0} | Rest]);
do_trim_zeroes(Other) ->
    Other.

format_row({Bucket, Count}, 0) ->
    [
        iolist_to_binary(io_lib:format("~p", [Bucket])),
        "|",
        iolist_to_binary(format_count(Count))
    ];
format_row({Bucket, Count}, MaxCount) ->
    Width = (?FORMAT_WIDTH * Count) div MaxCount,
    Bar = iolist_to_binary(["|", lists:duplicate(Width, $=)]),
    [iolist_to_binary(format_bucket(Bucket)), Bar, iolist_to_binary(format_count(Count))].

format_labels(Rows) ->
    LabelWidths = [size(Label) || [Label | _] <- Rows],
    MaxLabelWidth = lists:max(LabelWidths),
    [[pad_label(Label, MaxLabelWidth) | Rest] || [Label | Rest] <- Rows].

pad_label(Label, Width) ->
    PaddingWidth = Width - size(Label),
    Padding = lists:duplicate(PaddingWidth, " "),
    iolist_to_binary([Label, Padding]).

format_bucket(infinity) -> "inf";
format_bucket(Bucket) -> io_lib:format("~pms", [Bucket]).

format_count(0) -> "";
format_count(Count) -> io_lib:format(" ~p", [Count]).
