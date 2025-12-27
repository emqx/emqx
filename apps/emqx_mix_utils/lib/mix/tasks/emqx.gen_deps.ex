defmodule Mix.Tasks.Emqx.GenDeps do
  @moduledoc """
  Generate "used-by" dependency relationships for EMQX apps.

  This task analyzes BEAM files to determine which apps use (depend on) each app.
  The output is in "used-by" format (reverse dependency direction), meaning if app1 uses app2,
  the output will show "app2: app1" (app2 is used by app1).

    Analysis methods:
    1. Remote function calls: Uses XRef to scan all BEAM files and find all application edges (AE).
    2. Include directives: Parses -include_lib() directives from BEAM files' abstract code to find
       header file dependencies between apps.
    3. Explicit Mix intra-umbrella dependencies.

  Special handling:
  - emqx and emqx_conf are set to be used by all other apps
  - Self-dependencies are excluded
  - Transitive closure is computed: if app1 uses app2, and app2 uses app3, then app1 transitively uses app3

  Output format (deps.txt):
    app_name: user1 user2 user3
    app_name: all          (if used by all apps)
    app_name: none         (if used by no other apps)

  ## Usage

      mix emqx.gen_deps

  ## Options

  None currently supported.
  """

  use Mix.Task

  @shortdoc "Generate dependency relationships file (deps.txt)"

  @requirements ["compile", "loadpaths"]

  @impl Mix.Task
  def run(_args) do
    output_file = "deps.txt"

    Mix.shell().info("Scanning application dependecies in #{Mix.Project.build_path()}...")

    # Get all emqx apps from apps/ directory
    {:ok, emqx_apps} = Mix.Tasks.Emqx.GenDeps.DB.initialize()
    Mix.shell().info("Found #{length(emqx_apps)} apps")

    # Build used_by relationships (reverse dependency direction)
    deps_list = build_deps_map(emqx_apps)

    # Write to file
    File.write!(output_file, deps_list)

    Mix.shell().info("Used-by relationships written to #{output_file}")
  end

  defp build_deps_map(emqx_apps) do
    # Set emqx and emqx_conf to be used by all apps
    common_deps = %{
      :emqx => emqx_apps,
      :emqx_conf => emqx_apps,
      :emqx_mix_utils => emqx_apps
    }

    transitive_deps = Mix.Tasks.Emqx.GenDeps.DB.transitive_dependents()

    # Final step: Convert sets to sorted lists and format output
    # Format: app1: app2 app3 (where app2 and app3 transitively use app1, space-separated)
    # If UsedBySet + {App} = AllApps, output "all" instead
    emqx_apps
    |> Enum.sort()
    |> Enum.reduce([], fn app, acc ->
      # Check if UsedBySet + {App} = AllApps
      used_by = common_deps[app] || transitive_deps[app] || []
      cond do
        emqx_apps -- [app | used_by] == [] ->
          ["#{app}: all" | acc]

        used_by == [] ->
          ["#{app}: none" | acc]

        true ->
          used_by_str = used_by |> Enum.sort() |> Enum.map(&to_string/1) |> Enum.join(" ")
          ["#{app}: #{used_by_str}" | acc]
      end
    end)
    |> Enum.reverse()
    |> Enum.join("\n")
  end

  defmodule DB do
    @moduledoc """
    Compute "used-by" dependency relationships for EMQX apps using XRef.
    """

    @doc """
    Get all transitive "used-by" dependencies between applications.

    ## Returns
    Map of App => Set of apps that call it
    """
    def transitive_dependents() do
      {:ok, uses} = query("closure strict (AE + IE + MDE) | EMQX || EMQX")

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
      deps = find_include_dependencies()

      if deps != [] do
        query("IE := #{as_set(deps)}")
      else
        # Define as empty set:
        query("IE := (AE - AE)")
      end
    end

    defp define_mixdeps_edges() do
      apps = Mix.Dep.Umbrella.cached()
      app_names = apps |> Enum.map(fn app -> app.app end) |> MapSet.new()
      mix_deps = for app <- apps, dep <- app.deps, MapSet.member?(app_names, dep.app) do
        {app.app, dep.app}
      end

      query("MDE := #{as_set(mix_deps)}")
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
      # Find all BEAM files in ebin directory
      ebin_path = Path.join(app.opts[:build], "ebin")
      pattern = Path.join(ebin_path, "*.beam")
      beam_files = Path.wildcard(pattern)

      Enum.reduce(beam_files, [], fn beam_file, acc ->
        case get_include_lib_from_beam(beam_file) do
          {:ok, deps} -> deps ++ acc
          :error -> acc
        end
      end)
    end

    defp get_include_lib_from_beam(beam_file) do
      case :beam_lib.chunks(String.to_charlist(beam_file), [:abstract_code]) do
        {:ok, {_module, [{:abstract_code, {:raw_abstract_v1, forms}}]}} ->
          deps = extract_include_lib_from_forms(forms)
          {:ok, deps}

        {:ok, {_module, [{:abstract_code, :no_abstract_code}]}} ->
          # BEAM file compiled without debug_info, skip
          :error

        {:error, :beam_lib, _} ->
          :error
      end
    end

    defp extract_include_lib_from_forms(forms) do
      Enum.flat_map(forms, fn form ->
        case form do
          {:attribute, _line, :include_lib, path} ->
            case extract_app_from_include_lib_path(path) do
              {:ok, app} -> [app]
              :error -> []
            end

          _ ->
            []
        end
      end)
    end

    defp extract_app_from_include_lib_path(path) do
      # Path in AST is a string (list of integers) like "emqx/include/file.hrl"
      cond do
        is_list(path) and length(path) > 0 and is_integer(hd(path)) ->
          # Extract app name from path (first component)
          [app_name | _] = String.split(List.to_string(path), "/", parts: 2)
          {:ok, String.to_atom(app_name)}

        true ->
          :error
      end
    end
  end
end
