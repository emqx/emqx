defmodule EmqxReleaseHelperTest do
  use ExUnit.Case
  doctest EmqxReleaseHelper

  test "greets the world" do
    assert EmqxReleaseHelper.hello() == :world
  end
end
