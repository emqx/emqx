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
  - Transitive closure is computed: if app1 uses app2, and app2 uses app3, then app1 transitively
    uses app3

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
    {:ok, emqx_apps} = Emqx.GenDeps.DB.initialize()
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
    transitive_deps = Emqx.GenDeps.DB.transitive_dependents()

    emqx_apps
    |> Enum.sort()
    |> Enum.reduce([], fn app, acc ->
      # Check if UsedBySet + {App} = AllApps
      used_by = transitive_deps[app] || []

      cond do
        emqx_apps -- [app | used_by] == [] ->
          ["#{app}: all\n" | acc]

        used_by == [] ->
          ["#{app}: none\n" | acc]

        :otherwise ->
          used_by_str = used_by |> Enum.sort() |> Enum.map(&to_string/1) |> Enum.join(" ")
          ["#{app}: #{used_by_str}\n" | acc]
      end
    end)
    |> Enum.reverse()
    |> Enum.join()
  end
end
