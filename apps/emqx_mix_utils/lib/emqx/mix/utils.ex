defmodule EMQX.Mix.Utils do
  @clear_screen_pt_key {__MODULE__, :clear_screen}

  def clear_screen() do
    case {System.get_env("CLEAR_SCREEN"), :persistent_term.get(@clear_screen_pt_key, false)} do
      {"true", false} ->
        IO.write(:stdio, "\x1b[H\x1b[2J")
        IO.write(:stderr, "\x1b[H\x1b[2J")
        IO.write(:stdio, "\x1b[H\x1b[3J")
        IO.write(:stderr, "\x1b[H\x1b[3J")
        :persistent_term.put(@clear_screen_pt_key, true)

      _ ->
        :ok
    end
  end

  def reset_clear_screen() do
    :persistent_term.erase(@clear_screen_pt_key)
  end
end
