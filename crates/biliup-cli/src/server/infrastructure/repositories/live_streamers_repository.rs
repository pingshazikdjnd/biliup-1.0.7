use crate::server::core::live_streamers::{
    AddLiveStreamerDto, LiveStreamerEntity, LiveStreamersRepository,
};
use crate::server::infrastructure::connection_pool::ConnectionPool;
use anyhow::Context;
use async_trait::async_trait;

#[derive(Clone)]
pub struct SqliteLiveStreamersRepository {
    pool: ConnectionPool,
}

impl SqliteLiveStreamersRepository {
    pub fn new(pool: ConnectionPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl LiveStreamersRepository for SqliteLiveStreamersRepository {
    async fn create_streamer(&self, dto: AddLiveStreamerDto) -> anyhow::Result<LiveStreamerEntity> {
        let split_time = dto.split_time.map(|t| t as i64);
        let split_size = dto.split_size.map(|s| s as i64);
        sqlx::query_as::<_, LiveStreamerEntity>(
            r#"
        insert into live_streamers (url, remark, filename, split_time, split_size, upload_id)
        values (?, ?, ?, ?, ?, ?)
        returning id, url, remark, filename, split_time, split_size, upload_id
            "#,
        )
        .bind(dto.url)
        .bind(dto.remark)
        .bind(dto.filename)
        .bind(split_time)
        .bind(split_size)
        .bind(dto.upload_id)
        .fetch_one(&self.pool)
        .await
        .context("an unexpected error occurred while creating the streamer")
    }

    async fn delete_streamer(&self, id: i64) -> anyhow::Result<()> {
        sqlx::query("delete from live_streamers where id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .context("an unexpected error occurred while deleting the streamer")?;
        Ok(())
    }

    async fn update_streamer(
        &self,
        entity: LiveStreamerEntity,
    ) -> anyhow::Result<LiveStreamerEntity> {
        sqlx::query_as::<_, LiveStreamerEntity>(
            r#"
        update live_streamers
        set url = ?, remark = ?, filename = ?, split_time = ?, split_size = ?, upload_id = ?
        where id = ?
        returning id, url, remark, filename, split_time, split_size, upload_id
            "#,
        )
        .bind(entity.url)
        .bind(entity.remark)
        .bind(entity.filename)
        .bind(entity.split_time)
        .bind(entity.split_size)
        .bind(entity.upload_id)
        .bind(entity.id)
        .fetch_one(&self.pool)
        .await
        .context("an unexpected error occurred while updating the streamer")
    }

    async fn get_streamers(&self) -> anyhow::Result<Vec<LiveStreamerEntity>> {
        sqlx::query_as::<_, LiveStreamerEntity>(
            r#"
        select id, url, remark, filename, split_time, split_size, upload_id from live_streamers
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("an unexpected error occurred retrieving streamers")
    }

    async fn get_streamer_by_url(&self, url: &str) -> anyhow::Result<LiveStreamerEntity> {
        sqlx::query_as::<_, LiveStreamerEntity>(
            r#"
        select id, url, remark, filename, split_time, split_size, upload_id
        from live_streamers
        where url = ?
            "#,
        )
        .bind(url)
        .fetch_one(&self.pool)
        .await
        .context("an unexpected error occurred retrieving streamer by url")
    }

    async fn get_streamer_by_id(&self, id: i64) -> anyhow::Result<LiveStreamerEntity> {
        sqlx::query_as::<_, LiveStreamerEntity>(
            r#"
        select id, url, remark, filename, split_time, split_size, upload_id
        from live_streamers
        where id = ?
            "#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .context("an unexpected error occurred retrieving streamer by id")
    }
}
