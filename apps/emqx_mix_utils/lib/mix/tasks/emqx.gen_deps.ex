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
    # Convert sets to sorted lists and format output
    # Format: app1: app2 app3 (where app2 and app3 transitively use app1, space-separated)
    # If UsedBySet + {App} = AllApps, output "all" instead
    transitive_deps = Mix.Tasks.Emqx.GenDeps.DB.transitive_dependents()
    emqx_apps
    |> Enum.sort()
    |> Enum.reduce([], fn app, acc ->
      # Check if UsedBySet + {App} = AllApps
      used_by = transitive_deps[app] || []
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

    defp define_common_edges() do
      apps = Mix.Dep.Umbrella.cached()
      app_names = apps |> Enum.map(fn app -> app.app end)
      # Set emqx and emqx_conf to be used by all apps
      common = [:emqx, :emqx_conf]
      edges = for app <- common, dep <- app_names, do: {dep, app}
      query("CommE := #{as_set(edges)}")
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
      pattern = Path.join([app_path, "src", "*.erl"])
      erl_files = Path.wildcard(pattern)

      Enum.reduce(erl_files, [], fn erl_path, acc ->
        case find_include_directives(erl_path, %{app: app, stack: []}) do
          {:ok, deps, warn} ->
            Enum.each(warn, &Mix.shell().info/1)
            deps ++ acc
          {:error, error} ->
            Mix.shell().error("#{app.app}: #{inspect(error)}")
            acc
        end
      end)
    end

    def find_include_directives(file_path, st) do
      with {:ok, st} <- push_scan(file_path, st),
           {:ok, res} <-
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
          case scan_include_tokens(tokens, st) do
            {:ok, found, warn} ->
              scan_include_erl_forms(fd, loc, found ++ acc, warn ++ acc_warn, st)

            error ->
              error
          end

        {:eof, _} ->
          {:ok, acc, acc_warn}

        {:error, info, _} ->
          {:error, info}
      end
    end

    # Adopted from `epp.erl'
    defp scan_include_tokens([{:-, _lh}, {:atom, li, attr} | rest] = tokens, st)
         when attr == :include or attr == :include_lib do
      case coalesce_strings(rest) do
        [{:"(", _}, {:string, af, name}, {:")", _}, {:dot, _}] when attr == :include_lib ->
          with true <- length(name) > 0 and is_integer(hd(name)),
               # Extract app name from path (first component)
               [app_name, _ | _] <- String.split(to_string(name), "/", parts: 2) do
            {:ok, [String.to_atom(app_name)], []}
          else
            _ ->
              {:ok, [], [warn("malformed `include_lib' path: #{name}", af, st)]}
          end

        [{:"(", _}, {:string, af, name}, {:")", _}, {:dot, _}] when attr == :include ->
          case find_include_file(name, st) do
            {:ok, hrl_path} ->
              find_include_directives(hrl_path, st)

            {:error, reason} ->
              reason = inspect(reason)
              {:ok, [], [warn("cannot find include file `#{name}': #{reason}", af, st)]}
            end

          _ ->
          {:ok, [], [warn("unrecognized `#{attr}' attribute: #{inspect(tokens)}", li, st)]}
      end
    end

    defp scan_include_tokens(_, _st) do
      {:ok, [], []}
    end

    defp find_include_file(name, %{app: app, stack: [path_top | _]}) do
      app_path = app.opts[:dest]
      candidates = [
        Path.dirname(path_top),
        app_path,
        Path.join(app_path, "src"),
        Path.join(app_path, "include")
      ]

      Enum.reduce_while(candidates, {:error, :enoent}, fn path, _ ->
        hrl_path = Path.join([path, name])

        case File.lstat(hrl_path) do
          {:ok, %File.Stat{type: :regular}} ->
            {:halt, {:ok, hrl_path}}

          {:ok, _} ->
            {:cont, {:error, :badfile}}

          error ->
            {:cont, error}
        end
      end)
    end

    defp coalesce_strings([{:string, a, s} | tokens]), do: coalesce_strings(tokens, a, [s])
    defp coalesce_strings([t | tokens]), do: [t | coalesce_strings(tokens)]
    defp coalesce_strings([]), do: []

    defp coalesce_strings([{:string, _, s} | tokens], a, s0) do
      coalesce_strings(tokens, a, [s | s0])
    end

    defp coalesce_strings(tokens, a, s) do
      [{:string, a, List.flatten(Enum.reverse(s))} | coalesce_strings(tokens)]
    end

    defp push_scan(path, %{stack: stack} = st) when length(stack) < 8 do
      {:ok, %{st | stack: [path | stack]}}
    end

    defp push_scan(_path, %{stack: stack}) do
      {:error, {"too many nested includes", stack}}
    end

    defp warn(message, loc, %{app: app, stack: stack}) do
      app_path = app.opts[:dest]
      path_initial = List.last(stack)
      path_top = hd(stack)
      file_initial = Path.relative_to(path_initial, app_path)
      file_top = Path.relative_to(path_top, Path.dirname(path_initial))

      full_location =
        if path_initial == path_top do
          "#{file_top}:#{loc}"
        else
          "#{file_initial} @ #{file_top}:#{loc}"
        end

      "#{app.app}: #{full_location}: #{message}"
    end
  end
end
