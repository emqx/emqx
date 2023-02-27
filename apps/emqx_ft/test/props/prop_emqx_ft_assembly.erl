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

-module(prop_emqx_ft_assembly).

-include_lib("proper/include/proper.hrl").

-import(emqx_proper_types, [scaled/2]).

-define(COVERAGE_TIMEOUT, 5000).

prop_coverage() ->
    ?FORALL(
        {Filesize, Segsizes},
        {filesize_t(), segsizes_t()},
        ?FORALL(
            Fragments,
            noshrink(fragments_t(Filesize, Segsizes)),
            ?TIMEOUT(
                ?COVERAGE_TIMEOUT,
                begin
                    ASM1 = append_fragments(mk_assembly(Filesize), Fragments),
                    {Time, ASM2} = timer:tc(emqx_ft_assembly, update, [ASM1]),
                    measure(
                        #{"Fragments" => length(Fragments), "Time" => Time},
                        case emqx_ft_assembly:status(ASM2) of
                            complete ->
                                Coverage = emqx_ft_assembly:coverage(ASM2),
                                measure(
                                    #{"CoverageLength" => length(Coverage)},
                                    is_coverage_complete(Coverage)
                                );
                            {incomplete, {missing, {segment, _, _}}} ->
                                measure("CoverageLength", 0, true)
                        end
                    )
                end
            )
        )
    ).

prop_coverage_likely_incomplete() ->
    ?FORALL(
        {Filesize, Segsizes, Hole},
        {filesize_t(), segsizes_t(), filesize_t()},
        ?FORALL(
            Fragments,
            noshrink(fragments_t(Filesize, Segsizes, Hole)),
            ?TIMEOUT(
                ?COVERAGE_TIMEOUT,
                begin
                    ASM1 = append_fragments(mk_assembly(Filesize), Fragments),
                    {Time, ASM2} = timer:tc(emqx_ft_assembly, update, [ASM1]),
                    measure(
                        #{"Fragments" => length(Fragments), "Time" => Time},
                        case emqx_ft_assembly:status(ASM2) of
                            complete ->
                                % NOTE: this is still possible due to the nature of `SUCHTHATMAYBE`
                                IsComplete = emqx_ft_assembly:coverage(ASM2),
                                collect(complete, is_coverage_complete(IsComplete));
                            {incomplete, {missing, {segment, _, _}}} ->
                                collect(incomplete, true)
                        end
                    )
                end
            )
        )
    ).

prop_coverage_complete() ->
    ?FORALL(
        {Filesize, Segsizes},
        {filesize_t(), ?SUCHTHAT([BaseSegsize | _], segsizes_t(), BaseSegsize > 0)},
        ?FORALL(
            {Fragments, MaxCoverage},
            noshrink({fragments_t(Filesize, Segsizes), coverage_t(Filesize, Segsizes)}),
            begin
                % Ensure that we have complete coverage
                ASM1 = append_fragments(mk_assembly(Filesize), Fragments ++ MaxCoverage),
                {Time, ASM2} = timer:tc(emqx_ft_assembly, update, [ASM1]),
                measure(
                    #{"CoverageMax" => length(MaxCoverage), "Time" => Time},
                    case emqx_ft_assembly:status(ASM2) of
                        complete ->
                            Coverage = emqx_ft_assembly:coverage(ASM2),
                            measure(
                                #{"Coverage" => length(Coverage)},
                                is_coverage_complete(Coverage)
                            );
                        {incomplete, _} ->
                            false
                    end
                )
            end
        )
    ).

measure(NamedSamples, Test) ->
    maps:fold(fun(Name, Sample, Acc) -> measure(Name, Sample, Acc) end, Test, NamedSamples).

is_coverage_complete([]) ->
    true;
is_coverage_complete(Coverage = [_ | Tail]) ->
    is_coverage_complete(Coverage, Tail).

is_coverage_complete([_], []) ->
    true;
is_coverage_complete(
    [{_Node1, #{fragment := {segment, #{offset := O1, size := S1}}}} | Rest],
    [{_Node2, #{fragment := {segment, #{offset := O2}}}} | Tail]
) ->
    (O1 + S1 == O2) andalso is_coverage_complete(Rest, Tail).

mk_assembly(Filesize) ->
    emqx_ft_assembly:append(emqx_ft_assembly:new(Filesize), node(), mk_filemeta(Filesize)).

append_fragments(ASMIn, Fragments) ->
    lists:foldl(
        fun({Node, Frag}, ASM) ->
            emqx_ft_assembly:append(ASM, Node, Frag)
        end,
        ASMIn,
        Fragments
    ).

mk_filemeta(Filesize) ->
    #{
        path => "MANIFEST.json",
        fragment => {filemeta, #{name => ?MODULE_STRING, size => Filesize}}
    }.

mk_segment(Offset, Size) ->
    #{
        path => "SEG" ++ integer_to_list(Offset) ++ integer_to_list(Size),
        fragment => {segment, #{offset => Offset, size => Size}}
    }.

fragments_t(Filesize, Segsizes = [BaseSegsize | _]) ->
    NSegs = Filesize / max(1, BaseSegsize),
    scaled(1 + NSegs, list({node_t(), fragment_t(Filesize, Segsizes)})).

fragments_t(Filesize, Segsizes = [BaseSegsize | _], Hole) ->
    NSegs = Filesize / max(1, BaseSegsize),
    scaled(1 + NSegs, list({node_t(), fragment_t(Filesize, Segsizes, Hole)})).

fragment_t(Filesize, Segsizes, Hole) ->
    ?SUCHTHATMAYBE(
        #{fragment := {segment, #{offset := Offset, size := Size}}},
        fragment_t(Filesize, Segsizes),
        (Hole rem Filesize) =< Offset orelse (Hole rem Filesize) > (Offset + Size)
    ).

fragment_t(Filesize, Segsizes) ->
    ?LET(
        Segsize,
        oneof(Segsizes),
        ?LET(
            Index,
            range(0, Filesize div max(1, Segsize)),
            mk_segment(Index * Segsize, min(Segsize, Filesize - (Index * Segsize)))
        )
    ).

coverage_t(Filesize, [Segsize | _]) ->
    NSegs = Filesize div max(1, Segsize),
    [
        {remote_node_t(), mk_segment(I * Segsize, min(Segsize, Filesize - (I * Segsize)))}
     || I <- lists:seq(0, NSegs)
    ].

filesize_t() ->
    scaled(4000, non_neg_integer()).

segsizes_t() ->
    ?LET(
        BaseSize,
        segsize_t(),
        oneof([
            [BaseSize, BaseSize * 2],
            [BaseSize, BaseSize * 2, BaseSize * 3],
            [BaseSize, BaseSize * 2, BaseSize * 5]
        ])
    ).

segsize_t() ->
    scaled(50, non_neg_integer()).

remote_node_t() ->
    oneof([
        'emqx42@emqx.local',
        'emqx43@emqx.local',
        'emqx44@emqx.local'
    ]).

node_t() ->
    oneof([
        node(),
        'emqx42@emqx.local',
        'emqx43@emqx.local',
        'emqx44@emqx.local'
    ]).
