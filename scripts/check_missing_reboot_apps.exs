#!/usr/bin/env elixir

{parsed, _argv, _errors = []} =
  OptionParser.parse(
    System.argv(),
    strict: [profile: :string]
  )

profile = Keyword.fetch!(parsed, :profile)

:xref.start(:xref)
:xref.set_default(:xref, warnings: false)
rel_dir = '_build/#{profile}/lib/'
:xref.add_release(:xref, rel_dir)

{:ok, calls} = :xref.q(:xref, '(App) (XC || "mria":"create_table"/".*")')

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
end
