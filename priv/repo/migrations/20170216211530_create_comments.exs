defmodule Repo.Migrations.CreateComments do
  use Ecto.Migration

  def change do
    create table(:comments) do
      add :post_id, references(:posts), null: false
      add :body, :string, null: false
      timestamps()
    end
  end
end
