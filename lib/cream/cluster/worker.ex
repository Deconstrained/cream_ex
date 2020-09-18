require Logger

defmodule Cream.Cluster.Worker do
  @moduledoc false

  use GenServer

  alias Cream.Continuum

  def start_link(options) do
    GenServer.start_link(__MODULE__, options)
  end

  def init(options) do
    connection_map = options[:connection_map]
    namespace = Map.get(options, :namespace, nil)
    digest_class = Map.get(options, :digest_class, :md5)
    Logger.debug "Starting: #{inspect connection_map}"

    continuum = connection_map
      |> Map.keys
      |> Continuum.new

    {:ok, %{continuum: continuum, connection_map: connection_map, namespace: namespace, digest_class: digest_class}}
  end

  def handle_call({:set, pairs, options}, _from, state) do

    pairs_by_conn = Enum.map(pairs, fn {key, value} -> {dalli_key(key), value} end)
      |> Enum.group_by(fn {key, _value} -> find_conn(state, key) end)

    reply = Enum.reduce pairs_by_conn, %{}, fn {conn, pairs}, acc ->
      {:ok, responses} = Memcache.multi_set(conn, pairs, options)

      Enum.zip(pairs, responses)
        |> Enum.reduce(acc, fn {pair, status}, acc ->
          {key, _value} = pair
          status = case status do
            {:ok} -> :ok
            status -> status
          end
          Map.put(acc, key, status)
        end)
    end

    {:reply, reply, state}
  end

  def handle_call({:get, keys, options}, _from, state) do
    keys_by_conn = valid_keys(state, keys) |> Enum.group_by fn key ->
      find_conn(state, key)
    end

    reply = Enum.reduce keys_by_conn, %{}, fn {conn, keys}, acc ->
      case Memcache.multi_get(conn, keys, options) do
        {:ok, map} -> Map.merge(acc, map)
        _ -> acc # TODO something better than silently ignore?
      end
    end

    {:reply, reply, state}
  end

  def handle_call({:with_conn, keys, func}, _from, state) do
    keys_by_conn_and_server = valid_keys(state, keys) |> Enum.group_by fn key ->
      find_conn_and_server(state, key)
    end

    keys_by_conn_and_server
      |> Enum.reduce(%{}, fn {{conn, server}, keys}, acc ->
        Map.put(acc, server, func.(conn, keys))
      end)
      |> reply(state)
  end

  def handle_call({:flush, options}, _from, state) do
    ttl = Keyword.get(options, :ttl, 0)

    reply = Enum.map state.connection_map, fn {_server, conn} ->
      case Memcache.Connection.execute(conn, :FLUSH, [ttl]) do
        {:ok} -> :ok
        whatever -> whatever
      end
    end

    {:reply, reply, state}
  end

  def handle_call({:delete, keys}, _from, state) do
    valid_keys(state, keys)
      |> Enum.group_by(&find_conn(state, &1))
      |> Enum.reduce(%{}, fn {conn, keys}, results ->
        commands = Enum.map(keys, &{:DELETEQ, [&1]})
        case Memcache.Connection.execute_quiet(conn, commands) do
          {:ok, conn_results} -> Enum.zip(keys, conn_results)
            |> Enum.reduce(results, fn {key, conn_results}, results ->
              case conn_results do
                {:ok} -> Map.put(results, key, :ok)
                error -> Map.put(results, key, error)
              end
            end)
          {:error, _} -> Enum.reduce(keys, results, fn key, results ->
            Map.put(results, key, {:error, "connection error"})
          end)
        end
      end)
      |> reply(state)
  end

  defp reply(reply, state) do
    {:reply, reply, state }
  end

  defp find_conn_and_server(state, key) do
    {:ok, server} = Continuum.find(state.continuum, key)
    conn = state.connection_map[server]
    {conn, server}
  end

  defp find_conn(state, key) do
    find_conn_and_server(state, key) |> elem(0)
  end

  defp dalli_key(state, key) do
    namespace = Map.get(state, :namespace)
    key = key_with_namespace(key, namespace)
    if String.length(key) > 250 do
      max_length_before_namespace = 212 - String.length(namespace || '')
      # Lowercase hex digest for compatibility with Ruby's Digest
      key_hash = :crypto.hash(key, state[:digest_class], :md5)) |> Base.encode16(case: :lower)
      key = "#{String.slice(key, 0, max_length_before_namespace}:md5:#{key_hash}"
    end
    key
  end

  defp key_with_namespace(key, nil), do: key
  defp key_with_namespace(key, ''), do: key
  defp key_with_namespace(key, namespace), do: "#{namespace}:#{key}"


  defp valid_keys(state, keys) do
    Enum.map(keys, &dalli_key(state, &1))
    |> Enum.filter(&(String.length(&1)>0))
  end


end
