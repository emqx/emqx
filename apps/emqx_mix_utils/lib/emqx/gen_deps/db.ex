defmodule Emqx.GenDeps.DB do
  @moduledoc """
  Compute "used-by" dependency relationships for EMQX apps using XRef.
  """

  @doc """
  Get all transitive "used-by" dependencies between applications.

  ## Returns
  Map of App => Set of apps that call it
  """
  def transitive_dependents() do
    {:ok, uses} = query("closure strict (AE + IE + MDE + CommE) | EMQX || EMQX")

    for {app_used_by, app_uses} <- uses,
        app_used_by != app_uses,
        reduce: %{} do
      acc -> Map.update(acc, app_uses, [app_used_by], &[app_used_by | &1])
    end
  end

  @doc """
  Find App -> App dependencies from include_lib directives.

  ## Returns
  List of `{app, dep}` tuples.
  """
  def find_include_dependencies() do
    apps = Mix.Dep.Umbrella.cached()
    app_names = for app <- apps, do: app.app

    for app <- apps,
        File.dir?(Path.join(app.opts[:build], "ebin")),
        dep <- get_include_dependencies(app),
        Enum.member?(app_names, dep) do
      {app.app, dep}
    end
  end

  @doc """
  Start and populate internal graph database of application relationships.
  """
  def initialize() do
    :ok = ensure_xref()

    case query("EMQX") do
      {:ok, [_ | _]} = ok ->
        ok

      _undefined ->
        build_xref()
        define_include_edges()
        define_mixdeps_edges()
        define_common_edges()
        query("EMQX")
    end
  end

  def query(query) do
    :xref.q(__MODULE__, to_charlist(query))
  end

  defp build_xref() do
    apps = Mix.Dep.Umbrella.cached()
    app_names = apps |> Enum.map(fn app -> app.app end)
    app_dirs = apps |> Stream.map(fn app -> to_charlist(app.opts[:build]) end)

    # Populate XRef database:
    Enum.each(app_dirs, fn lib -> {:ok, _} = :xref.add_application(__MODULE__, lib) end)

    # Aliases:
    {:ok, _} = query("EMQX := (App) #{as_set(app_names)}")
  end

  defp define_include_edges() do
    define_set("IE", find_include_dependencies())
  end

  defp define_mixdeps_edges() do
    apps = Mix.Dep.Umbrella.cached()
    app_names = apps |> Enum.map(fn app -> app.app end) |> MapSet.new()

    mix_deps =
      for app <- apps, dep <- app.deps, MapSet.member?(app_names, dep.app) do
        {app.app, dep.app}
      end

    define_set("MDE", mix_deps)
  end

  defp define_common_edges() do
    apps = Mix.Dep.Umbrella.cached()
    app_names = apps |> Enum.map(fn app -> app.app end)
    # Set emqx and emqx_conf to be used by all apps
    common = [:emqx, :emqx_conf]
    edges = for app <- common, dep <- app_names, do: {dep, app}
    define_set("CommE", edges)
  end

  defp define_set(name, edges) do
    if edges != [] do
      query("#{name} := #{as_set(edges)}")
    else
      # Forcefully define as empty set:
      query("#{name} := (AE - AE)")
    end
  end

  defp as_set(elems) do
    "[#{elems |> Enum.map(&as_elem/1) |> Enum.join(", ")}]"
  end

  defp as_elem({from, to}), do: "#{from} -> #{to}"
  defp as_elem(name) when is_atom(name), do: to_string(name)

  def ensure_xref() do
    case :xref.start(__MODULE__) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} ->
        :xref.stop(__MODULE__)
        ensure_xref()
    end
  end

  # Internal functions for include_lib directives

  defp get_include_dependencies(app) do
    app_path = app.opts[:dest]
    erl_files = Path.wildcard(Path.join([app_path, "src", "**", "*.erl"]))
    hrl_files = Path.wildcard(Path.join([app_path, "{include,src}", "**", "*.hrl"]))

    Enum.reduce(erl_files ++ hrl_files, [], fn file_path, acc ->
      case find_include_directives(file_path, %{app: app, file: file_path}) do
        {:ok, deps, warn} ->
          Enum.each(warn, &Mix.shell().info/1)
          deps ++ acc

        {:error, error} ->
          Mix.shell().error("#{app.app}: #{inspect(error)}")
          acc
      end
    end)
  end

  defp find_include_directives(file_path, st) do
    with {:ok, res} <-
           File.open(file_path, [:read], fn fd ->
             scan_include_erl_forms(fd, 1, [], [], st)
           end) do
      res
    end
  end

  # Adopted from `epp.erl'
  defp scan_include_erl_forms(fd, loc, acc, acc_warn, st) do
    case :io.scan_erl_form(fd, "", loc, []) do
      {:ok, tokens, loc} ->
        with {:ok, found, warn} <- scan_include_tokens(tokens, st) do
          scan_include_erl_forms(fd, loc, found ++ acc, warn ++ acc_warn, st)
        end

      {:eof, _} ->
        {:ok, acc, acc_warn}

      {:error, info, _} ->
        {:error, info}
    end
  end

  # Adopted from `epp.erl'
  defp scan_include_tokens([{:-, _lh}, {:atom, li, :include_lib} | rest] = tokens, st) do
    case coalesce_strings(rest) do
      [{:"(", _}, {:string, af, name}, {:")", _}, {:dot, _}] ->
        with true <- length(name) > 0 and is_integer(hd(name)),
             # Extract app name from path (first component)
             [app_name, _ | _] <- String.split(to_string(name), "/", parts: 2) do
          {:ok, [String.to_atom(app_name)], []}
        else
          _ ->
            {:ok, [], [warn("malformed `include_lib' path: #{name}", af, st)]}
        end

      _ ->
        {:ok, [], [warn("unrecognized `include_lib' attribute: #{inspect(tokens)}", li, st)]}
    end
  end

  defp scan_include_tokens(_, _st) do
    {:ok, [], []}
  end

  # Adopted from `epp.erl'
  defp coalesce_strings([{:string, a, s} | tokens]), do: coalesce_strings(tokens, a, [s])
  defp coalesce_strings([t | tokens]), do: [t | coalesce_strings(tokens)]
  defp coalesce_strings([]), do: []

  defp coalesce_strings([{:string, _, s} | tokens], a, s0) do
    coalesce_strings(tokens, a, [s | s0])
  end

  defp coalesce_strings(tokens, a, s) do
    [{:string, a, List.flatten(Enum.reverse(s))} | coalesce_strings(tokens)]
  end

  defp warn(message, loc, %{app: app, file: path}) do
    app_path = app.opts[:dest]
    "#{app.app}: #{Path.relative_to(path, app_path)}:#{loc}: #{message}"
  end
end
