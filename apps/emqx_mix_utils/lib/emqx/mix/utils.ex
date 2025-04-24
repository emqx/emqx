defmodule EMQX.Mix.Utils do
  def clear_screen() do
    k = {__MODULE__, :clear_screen}
    case {System.get_env("CLEAR_SCREEN"), :persistent_term.get(k, false)} do
      {"true", false} ->
        IO.write(:stdio, "\x1b[H\x1b[2J")
        IO.write(:stderr, "\x1b[H\x1b[2J")
        IO.write(:stdio, "\x1b[H\x1b[3J")
        IO.write(:stderr, "\x1b[H\x1b[3J")
        :persistent_term.put(k, true)

      _ ->
        :ok
    end
  end
end
