#!/usr/bin/env elixir

alias EMQXUmbrella.MixProject

{:ok, _} = Application.ensure_all_started(:mix)
# note: run from the project root
File.cwd!()
|> Path.join("mix.exs")
|> Code.compile_file()

inputs = MixProject.check_profile!()
profile = Mix.env()

# need to use this information because we might have compiled all
# applications in the test profile, and thus filter what's in the
# release lib directory.
rel_apps = MixProject.applications(inputs.release_type, inputs.edition_type)

apps =
  rel_apps
  |> Keyword.keys()
  |> Enum.filter(&(to_string(&1) =~ "emqx"))
  |> Enum.reject(&(&1 in [:emqx_mix]))

:xref.start(:xref)
:xref.set_default(:xref, warnings: false)
rel_dir = ~c"_build/#{profile}/lib/"
:xref.add_release(:xref, rel_dir)

{:ok, calls} = :xref.q(:xref, ~c"(App) (XC | [#{Enum.join(apps, ",")}] || mria:create_table/_)")

emqx_calls =
  calls
  |> Enum.map(&elem(&1, 0))
  |> Enum.filter(&(to_string(&1) =~ "emqx_"))
  |> MapSet.new()

Path.wildcard(rel_dir ++ "*/ebin")
|> Enum.each(fn dir ->
  dir
  |> to_charlist()
  |> :code.add_pathz()
end)

Path.wildcard(rel_dir ++ "*")
|> Enum.map(fn dir ->
  dir
  |> Path.basename()
  |> String.to_atom()
  |> Application.load()
end)

reboot_apps = :emqx_machine_boot.sorted_reboot_apps() |> MapSet.new()

missing_reboot_apps = MapSet.difference(emqx_calls, reboot_apps)

if MapSet.size(missing_reboot_apps) != 0 do
  IO.puts(
    :stderr,
    IO.ANSI.format([
      :red,
      "Some applications are missing from `emqx_machine_boot:sorted_reboot_apps/0`!\n",
      "Missing applications:\n",
      Enum.map(missing_reboot_apps, fn app ->
        "  * #{app}\n"
      end),
      "\n",
      :green,
      "Hint: maybe add them to `emqx_machine_boot:basic_reboot_apps_edition/1`\n",
      "\n",
      :yellow,
      "Applications that call `mria:create_table` need to be added to that list;\n",
      " otherwise, when a node joins a cluster, it might lose tables.\n"
    ])
  )

  System.halt(1)
end
