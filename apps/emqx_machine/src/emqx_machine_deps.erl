%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_machine_deps).

-moduledoc """
This module is used to track implicit and explicit cross-application
dependencies in EMQX.
This includes test dependencies as well.

It is used to generate the `emqx_machine_rl.hrl` defining the run levels used by EMQX.

Instructions:

1. Install [Z3](https://github.com/Z3Prover/z3)
2. Declare a new dependency node in this file by updating `vertices()` list.
3. Specify the additional constraints.
   - Inter-dependencies can be added to edges lists.

   - If the node should activate after the clusterer is ready,
     specify `range(...) -> {?classy_rl_cluster, ...}`

   - If the node's run level is used during vote or is sent over the network,
     it must be pinned.

4. Build EMQX and run

   ```
   emqx_machine_deps:generate("<path-to-repo>/emqx/apps/emqx_machine/include/emqx_machine_rl.hrl").
   ```

""".

%% API:
-export([gen_smt/0, solve/0, generate/1]).
-export([vertices/0, range/1, pinned_levels/0, edges/0]).

-include_lib("classy/include/classy_rl.hrl").
-include_lib("mria/include/mria.hrl").

-define(min_level, ?mria_rl_ready).
-define(max_level, ?classy_rl_ready).

%%================================================================================
%% Dependencies and constraints
%%================================================================================

-doc """
List of vertices.
Anything that can depend on something or be a dependency of something is a vertex.
""".
vertices() ->
    [
        emqx_rl_boot,
        emqx_rl_ready,

        emqx_rl_config_table_create,
        emqx_rl_config_table_ready,

        emqx_rl_listener
    ].

-doc """
This function allows to restrict the closed interval of allowed run levels for a vertex.
""".
range(emqx_rl_config_table_ready) ->
    {?min_level, ?classy_rl_cluster};
range(_) ->
    {?min_level, ?max_level}.

-doc """
Some run levels should never change.
For example, if they are used by `classy_vote' (which persists the level in storage)
or if they are sent across network.
""".
pinned_levels() ->
    #{
        emqx_rl_boot => ?mria_rl_ready,
        emqx_rl_ready => ?classy_rl_ready,
        %% Pinned to leave some run level space before ready:
        emqx_rl_listener => ?classy_rl_ready - 20
    }.

-doc """
List of dependencies.

```
{A, B, Options}
```
means "B depends on A".
""".
edges() ->
    [
        {emqx_rl_config_table_create, emqx_rl_config_table_ready, []}
    ].

-doc """
Free-form constraints.
""".
free_form() ->
    ~"""
    """.

%%================================================================================
%% API functions
%%================================================================================

-doc """
Solve the system and get the result.
""".
solve() ->
    File = "/tmp/emqx_machine_deps.smt2",
    ok = file:write_file(File, gen_smt()),
    case os:cmd("z3 " ++ File) of
        "sat\n" ++ Rest ->
            parse(Rest);
        Output ->
            io:put_chars(standard_error, Output),
            exit(unsat)
    end.

-doc """
Solve the system and generate `emqx_machine_rl.hrl`.
""".
generate(Filename) ->
    RunLevels = solve(),
    file:write_file(
        Filename,
        [
            ~"""
            %%--------------------------------------------------------------------
            %% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
            %%--------------------------------------------------------------------

            %% WARNING: this code is automatically generated. Follow
            %% instructions in emqx_machine_deps.erl to regenerate
            %% this file.
            -ifndef(EMQX_MACHINE_RL_HRL).
            -define(EMQX_MACHINE_RL_HRL, true).

            """,
            [io_lib:format("-define(~s, ~s).~n", [A, B]) || {A, B} <- RunLevels],
            ~"""
            -endif.
            """
        ]
    ).

-doc """
Convert constraints into Lib-SMT2 format.
""".
%% erlfmt-ignore
gen_smt() ->
    to_sexp(
      [ ~"""
         ; Preamble
         (set-logic QF_LIA)
         (set-option :produce-models true)
         (set-option :produce-unsat-cores true)
         ; Vertices
         """
      | gen_vertices()
      ] ++
      [ ~"; Ranges"
      | gen_ranges()
      ] ++
      [ ~"; Pinned run levels"
      | gen_pinned()
      ] ++
      [ ~"; Edges"
      | gen_edges()
      ] ++
      [  ~"""
         ; Upper and lower bounds
         """
      , gen_upper_bound()
      , gen_lower_bound()
      ] ++
      [ free_form()
      , ~"""
         ; Results
         """
      , [~"check-sat"]
      , [~"get-value", vertices()]
      , [exit]
      ]).

%%================================================================================
%% Internal functions
%%================================================================================

gen_vertices() ->
    [
        [~"declare-const", I, ~"Int"]
     || I <- vertices()
    ].

%% erlfmt-ignore
gen_ranges() ->
    [begin
         {Min0, Max0} = range(I),
         Min = max(?min_level, Min0),
         Max = min(?max_level, Max0),
         [assert,
          [~"and"
          , [~"<=", Min, I]
          , [~"<=", I, Max]]]
     end
     || I <- vertices()
    ].

gen_pinned() ->
    [
        [assert, [~"=", Level, Vtx]]
     || Vtx := Level <- pinned_levels()
    ].

gen_edges() ->
    lists:map(
        fun({From, To, _}) ->
            [assert, [~"<", From, To]]
        end,
        edges()
    ).

%% erlfmt-ignore
gen_lower_bound() ->
    [assert,
      [~"and" | [[~">", I, emqx_rl_boot] ||
                    I <- vertices(),
                    I =/= emqx_rl_boot]]
    ].

%% erlfmt-ignore
gen_upper_bound() ->
    [assert,
      [~"and" | [[~"<", I, emqx_rl_ready] ||
                    I <- vertices(),
                    I =/= emqx_rl_ready]]
    ].

to_sexp(L) ->
    lists:join($\n, [to_sexp1(I) || I <- L]).

to_sexp1(L) when is_list(L) ->
    [$(, lists:join($\s, [to_sexp1(I) || I <- L]), $)];
to_sexp1(I) when is_integer(I) ->
    integer_to_binary(I);
to_sexp1(A) when is_atom(A) ->
    atom_to_binary(A);
to_sexp1(Str) when is_binary(Str) ->
    Str.

parse(S) ->
    pairs(string:lexemes(S, " ()\t\r\n")).

pairs([A, N | T]) -> [{A, N} | pairs(T)];
pairs([]) -> [].
