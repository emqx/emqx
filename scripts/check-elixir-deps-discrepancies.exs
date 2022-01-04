#!/usr/bin/env elixir

# ensure we have a fresh rebar.lock

case File.stat("rebar.lock") do
  {:ok, _} ->
    File.rm!("rebar.lock")

  _ ->
    :ok
end

{_, 0} =
  File.cwd!()
  |> Path.join("rebar3")
  |> System.cmd(["tree"], into: IO.stream())

{:ok, props} = :file.consult("rebar.lock")

{[{_, rebar_deps}], [props]} = Enum.split_with(props, &is_tuple/1)

# dpendencies declared as package versions have a "secondary index"
pkg_idx =
  props
  |> Keyword.fetch!(:pkg_hash)
  |> Map.new()

rebar_deps =
  Map.new(rebar_deps, fn {name, ref, _} ->
    ref =
      case ref do
        {:pkg, _, _} ->
          pkg_idx
          |> Map.fetch!(name)
          |> String.downcase()

        {:git, _, {:ref, ref}} ->
          to_string(ref)
      end

    {name, ref}
  end)

{mix_deps, []} = Code.eval_file("mix.lock")

mix_deps =
  Map.new(mix_deps, fn {name, ref} ->
    ref =
      case ref do
        {:git, _, ref, _} ->
          ref

        {:hex, _, _, ref, _, _, _, _} ->
          ref
      end

    {to_string(name), ref}
  end)

diffs =
  Enum.reduce(rebar_deps, %{}, fn {name, rebar_ref}, acc ->
    mix_ref = mix_deps[name]

    cond do
      mix_ref && mix_ref != rebar_ref ->
        Map.put(acc, name, {rebar_ref, mix_ref})

      is_nil(mix_ref) ->
        Map.put(acc, name, {rebar_ref, nil})

      :otherwise ->
        acc
    end
  end)

if diffs == %{} do
  IO.puts(
    IO.ANSI.green() <>
      "* Mix and Rebar3 dependencies OK!" <>
      IO.ANSI.reset()
  )

  System.halt(0)
else
  IO.puts(
    IO.ANSI.red() <>
      "* Discrepancies between Elixir and Rebar3 dependencies found!" <>
      IO.ANSI.reset()
  )

  Enum.each(diffs, fn {name, {rebar_ref, mix_ref}} ->
    IO.puts(
      IO.ANSI.red() <>
        "  * #{name}\n" <>
        "    * Rebar3 ref: #{rebar_ref}\n" <>
        "    * Mix ref: #{mix_ref}\n" <>
        IO.ANSI.reset()
    )
  end)

  IO.puts(
    IO.ANSI.red() <>
      "Update `mix.exs` to match Rebar3's references (use `overwrite: true` if necessary) " <>
      "and/or run `mix deps.get` to update and try again" <>
      IO.ANSI.reset()
  )

  System.halt(1)
end
