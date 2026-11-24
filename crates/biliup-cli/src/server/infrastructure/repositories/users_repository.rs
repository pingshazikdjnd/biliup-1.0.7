use crate::server::core::users::{User, UsersRepository};
use crate::server::infrastructure::connection_pool::ConnectionPool;
use anyhow::Context;
use async_trait::async_trait;

#[derive(Clone)]
pub struct SqliteUsersStreamersRepository {
    pool: ConnectionPool,
}

impl SqliteUsersStreamersRepository {
    pub fn new(pool: ConnectionPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UsersRepository for SqliteUsersStreamersRepository {
    async fn create_user(&self, user: User) -> anyhow::Result<User> {
        sqlx::query_as::<_, User>(
            r#"
        insert into users (name, value, platform)
        values (?, ?, ?)
        returning id, name, value, platform
            "#,
        )
        .bind(user.name)
        .bind(user.value)
        .bind(user.platform)
        .fetch_one(&self.pool)
        .await
        .context("an unexpected error occured while creating the user")
    }

    async fn get_users(&self) -> anyhow::Result<Vec<User>> {
        sqlx::query_as::<_, User>(
            r#"
        select id, name, value, platform
        from users
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("unexpected error while querying for users")
    }

    async fn delete_user(&self, id: i64) -> anyhow::Result<()> {
        sqlx::query("delete from users where id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .context("an unexpected error occurred while deleting user")?;
        Ok(())
    }

    async fn update_user(&self, _user: User) -> anyhow::Result<User> {
        todo!()
    }

    async fn get_user_by_id(&self, _id: i64) -> anyhow::Result<User> {
        todo!()
    }
}
