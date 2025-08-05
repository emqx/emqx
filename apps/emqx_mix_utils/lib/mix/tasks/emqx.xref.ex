defmodule Mix.Tasks.Emqx.Xref do
  use Mix.Task

  alias EMQXUmbrella.MixProject, as: UMP

  @requirements ["compile", "loadpaths"]

  @shortdoc "Run xref analysis"

  @moduledoc """
  Runs xref analysis.

  ## Options

  Currently, no options are available.

  ## Examples

      $ mix emqx.xref
  """

  @xref :xref

  @checks [
    :undefined_function_calls,
    :undefined_functions,
    :locals_not_used,
    :deprecated_function_calls,
    :deprecated_functions
  ]

  @exclusions MapSet.new([
                EMQXUmbrella.MixProject
              ])

  @impl true
  def run(_args) do
    start_xref()
    set_library_path()
    set_default()
    add_umbrella_app_dirs()
    check_failures = run_checks()
    query_failures = run_queries()
    stop_xref()

    case {check_failures, query_failures} do
      {[], []} ->
        Mix.shell().info([
          :green,
          "No xref issues found"
        ])

        :ok

      _ ->
        Mix.shell().info([
          :red,
          "Xref encountered issues:\n\n",
          inspect(
            %{
              check_failures: check_failures,
              query_failures: query_failures
            },
            limit: :infinity,
            pretty: true
          )
        ])

        System.halt(1)
    end

    :ok
  end

  def run_checks() do
    exclusions = gather_exclusions()

    Enum.reduce(@checks, [], fn check, acc ->
      {:ok, results} = :xref.analyse(@xref, check)

      mods =
        Enum.flat_map(results, fn
          {m, _f, _a} ->
            [m]

          {{ms, _fs, _as}, {_mt, _ft, _at}} ->
            [ms]

          _ ->
            []
        end)

      mod_ignores =
        mods
        |> Enum.flat_map(&get_module_ignores/1)
        |> MapSet.new()

      ignores = MapSet.union(exclusions, mod_ignores)

      ignored? = fn vertex ->
        mod =
          case vertex do
            {m, _, _} ->
              m

            _ ->
              vertex
          end

        MapSet.member?(ignores, vertex) || MapSet.member?(ignores, mod)
      end

      results =
        Enum.reject(results, fn
          {src, dest} ->
            ignored?.(src) || ignored?.(dest)

          vertex ->
            ignored?.(vertex)
        end)

      if results == [] do
        acc
      else
        [{check, results} | acc]
      end
    end)
  end

  def run_queries() do
    UMP.xref_queries()
    |> Enum.flat_map(fn {query, expected} ->
      {:ok, result} = :xref.q(@xref, to_charlist(query))

      if result == expected do
        []
      else
        [{query, expected, result}]
      end
    end)
  end

  def get_module_ignores(mod) do
    try do
      attrs = mod.module_info(:attributes)

      Keyword.get_values(attrs, :ignore_xref)
      |> List.flatten()
      |> Enum.map(fn
        {f, a} -> {mod, f, a}
        {m, f, a} -> {m, f, a}
        m when is_atom(m) -> m
      end)
    rescue
      UndefinedFunctionError ->
        []
    end
  end

  def start_xref() do
    case :xref.start(@xref) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} ->
        :xref.stop(@xref)
        start_xref()
    end
  end

  def stop_xref() do
    :xref.stop(@xref)
  end

  def set_library_path() do
    :code.get_path()
    |> Enum.filter(&File.dir?/1)
    |> then(&:xref.set_library_path(@xref, &1))
  end

  def set_default() do
    :xref.set_default(@xref, warnings: false, verbose: false)
  end

  def add_umbrella_app_dirs() do
    Mix.Dep.Umbrella.cached()
    |> Stream.reject(fn %Mix.Dep{app: app} ->
      ## build-only application
      app == :emqx_mix_utils
    end)
    |> Stream.flat_map(&Mix.Dep.load_paths/1)
    |> Enum.each(fn ebin_dir ->
      {:ok, _} = :xref.add_directory(@xref, to_charlist(ebin_dir))
    end)
  end

  def gather_exclusions() do
    Mix.Dep.Umbrella.cached()
    |> Stream.flat_map(fn umbrella_dep ->
      %Mix.Dep{app: app, opts: opts} = umbrella_dep

      Mix.Project.in_project(app, opts[:path], [], fn umbrella_mix_mod ->
        project_opts = umbrella_mix_mod.project()
        Keyword.get(project_opts, :xref_ignores, [])
      end)
    end)
    |> MapSet.new()
    |> MapSet.union(@exclusions)
  end
end
