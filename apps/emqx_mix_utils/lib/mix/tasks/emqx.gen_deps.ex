defmodule Mix.Tasks.Emqx.GenDeps do
  @moduledoc """
  Generate "used-by" dependency relationships for EMQX apps.

  This task analyzes BEAM files to determine which apps use (depend on) each app.
  The output is in "used-by" format (reverse dependency direction), meaning if app1 uses app2,
  the output will show "app2: app1" (app2 is used by app1).

  Analysis methods:
  1. Remote function calls: Uses xref to scan all BEAM files and find all remote calls (XC query).
     Maps caller and callee modules to their respective apps using module-to-app mapping from .app files.
  2. Include directives: Parses -include_lib() directives from BEAM files' abstract code to find
     header file dependencies between apps.

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

  ## Requirements

  - Project must be compiled (BEAM files in _build/emqx-enterprise/lib/)
  """

  use Mix.Task

  @shortdoc "Generate dependency relationships file (deps.txt)"

  @requirements []

  @impl Mix.Task
  def run(_args) do
    build_paths = [
      "_build/emqx-enterprise"
    ]

    beam_paths =
      Enum.map(build_paths, fn path ->
        Path.join([path, "lib/emqx_mix_utils/ebin"])
      end)
      |> Enum.filter(&File.dir?/1)

    Enum.each(beam_paths, &Code.prepend_path/1)

    # Ensure the module is available
    unless Code.ensure_loaded?(EMQX.Mix.Deps) do
      Mix.shell().error("""
      Error: EMQX.Mix.Deps module not found.
      Please ensure emqx_mix_utils is compiled first by running:
        MIX_ENV=emqx-enterprise mix compile emqx_mix_utils
      """)

      System.halt(1)
    end

    build_dir = "_build/emqx-enterprise"
    apps_dir = "apps"
    output_file = "deps.txt"

    Mix.shell().info("Scanning beam files in #{build_dir}...")

    # Get all emqx apps from apps/ directory
    emqx_apps = get_emqx_apps(apps_dir)
    Mix.shell().info("Found #{length(emqx_apps)} emqx apps")

    # Build used_by relationships (reverse dependency direction)
    deps_list = build_deps_map(build_dir, emqx_apps)

    # Write to file
    File.write!(output_file, deps_list)

    Mix.shell().info("Used-by relationships written to #{output_file}")
  end

  defp get_emqx_apps(apps_dir) do
    case File.ls(apps_dir) do
      {:ok, files} ->
        files
        |> Enum.filter(fn name ->
          # All apps in apps/ directory are emqx apps
          # Check if it's a directory
          path = Path.join([apps_dir, name])
          File.dir?(path)
        end)
        |> Enum.map(fn name ->
          String.to_atom(name)
        end)

      {:error, _} ->
        []
    end
  end

  defp build_deps_map(build_dir, emqx_apps) do
    # Filter to only apps that have beam directories
    apps_with_beams =
      Enum.filter(emqx_apps, fn app ->
        app_name = Atom.to_string(app)
        beam_path = Path.join([build_dir, "lib", app_name, "ebin"])
        File.dir?(beam_path)
      end)

    # Build module-to-app map by reading .app files
    # e.g., emqx_zone_schema -> emqx (from emqx.app modules list)
    module_to_app_map = build_module_to_app_map(build_dir, apps_with_beams)

    # Use xref to get all remote calls from all apps at once
    lib_dir = Path.join([build_dir, "lib"])
    callee_to_caller_map = EMQX.Mix.Deps.get_call_dependents(lib_dir, module_to_app_map)

    # Collect included-to-including map from include_lib directives
    # When App1 includes headers from App2, add App1 to App2's including set
    # Use BEAM files to extract include_lib directives from AST
    included_to_including_map = EMQX.Mix.Deps.get_include_dependents(lib_dir, emqx_apps)

    # Set emqx and emqx_conf to be used by all apps
    all = MapSet.new(emqx_apps)

    common_deps_map = %{
      :emqx => all,
      :emqx_conf => all
    }

    # Compute transitive closure: merge all maps and find all apps that transitively use each app
    used_by_transitive =
      EMQX.Mix.Deps.compute_transitive_closure(
        [callee_to_caller_map, included_to_including_map, common_deps_map],
        emqx_apps
      )

    # Final step: Convert sets to sorted lists and format output
    # Format: app1: app2 app3 (where app2 and app3 transitively use app1, space-separated)
    # If UsedBySet + {App} = AllApps, output "all" instead
    used_by_transitive
    |> Enum.sort_by(fn {app, _} -> app end)
    |> Enum.reduce([], fn {app, used_by_set}, acc ->
      used_by_list = MapSet.to_list(used_by_set)
      filtered = Enum.reject(used_by_list, fn user -> user == app end)
      app_str = Atom.to_string(app)

      # Check if UsedBySet + {App} = AllApps
      used_by_with_self = MapSet.put(used_by_set, app)

      cond do
        MapSet.equal?(used_by_with_self, all) ->
          ["#{app_str}: all\n" | acc]

        filtered == [] ->
          ["#{app_str}: none\n" | acc]

        true ->
          sorted = Enum.sort(filtered)
          used_by_str = Enum.join(Enum.map(sorted, &Atom.to_string/1), " ")
          ["#{app_str}: #{used_by_str}\n" | acc]
      end
    end)
    |> Enum.reverse()
    |> Enum.join()
  end

  defp build_module_to_app_map(build_dir, apps_with_beams) do
    Enum.reduce(apps_with_beams, %{}, fn app, acc ->
      app_name = Atom.to_string(app)
      app_file = Path.join([build_dir, "lib", app_name, "ebin", "#{app_name}.app"])

      {:ok, [{:application, _app_atom, app_data}]} = :file.consult(String.to_charlist(app_file))
      modules = Keyword.get(app_data, :modules, [])

      Enum.reduce(modules, acc, fn module, map_acc ->
        Map.put(map_acc, module, app)
      end)
    end)
  end
end
