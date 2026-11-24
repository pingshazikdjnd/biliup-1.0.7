use crate::server::core::upload_streamers::{StudioEntity, UploadStreamersRepository};
use crate::server::infrastructure::connection_pool::ConnectionPool;

use anyhow::Context;
use async_trait::async_trait;

#[derive(Clone)]
pub struct SqliteUploadStreamersRepository {
    pool: ConnectionPool,
}

impl SqliteUploadStreamersRepository {
    pub fn new(pool: ConnectionPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UploadStreamersRepository for SqliteUploadStreamersRepository {
    async fn create_streamer(&self, studio: StudioEntity) -> anyhow::Result<StudioEntity> {
        sqlx::query_as::<_, StudioEntity>(
            r#"insert into upload_streamers(
                template_name,
                user,
                copyright,
                source,
                tid,
                cover,
                title,
                "desc",
                dynamic,
                tag,
                dtime,
                interactive,
                mission_id,
                dolby,
                lossless_music,
                no_reprint,
                charging_pay,
                up_selection_reply,
                up_close_reply,
                up_close_danmu
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            returning
                id,
                template_name,
                user,
                copyright,
                source,
                tid,
                cover,
                title,
                "desc",
                dynamic,
                tag,
                dtime,
                interactive,
                mission_id,
                dolby,
                lossless_music,
                no_reprint,
                charging_pay,
                up_selection_reply,
                up_close_reply,
                up_close_danmu"#,
        )
        .bind(studio.template_name)
        .bind(studio.user)
        .bind(studio.copyright)
        .bind(studio.source)
        .bind(studio.tid)
        .bind(studio.cover)
        .bind(studio.title)
        .bind(studio.desc)
        .bind(studio.dynamic)
        .bind(studio.tag)
        .bind(studio.dtime)
        .bind(studio.interactive)
        .bind(studio.mission_id)
        .bind(studio.dolby)
        .bind(studio.lossless_music)
        .bind(studio.no_reprint)
        .bind(studio.charging_pay)
        .bind(studio.up_selection_reply)
        .bind(studio.up_close_reply)
        .bind(studio.up_close_danmu)
        .fetch_one(&self.pool)
        .await
        .context("an unexpected error occurred while creating the streamer")
    }

    async fn delete_streamer(&self, id: i64) -> anyhow::Result<()> {
        sqlx::query("delete from upload_streamers where id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .context("an unexpected error occurred while deleting the upload template")?;
        Ok(())
    }

    async fn update_streamer(&self, studio: StudioEntity) -> anyhow::Result<StudioEntity> {
        sqlx::query_as::<_, StudioEntity>(
            r#"
        update upload_streamers
        set template_name = ?,
            user = ?,
            copyright = ?,
            source = ?,
            tid = ?,
            cover = ?,
            title = ?,
            "desc" = ?,
            dynamic = ?,
            tag = ?,
            dtime = ?,
            interactive = ?,
            mission_id = ?,
            dolby = ?,
            lossless_music = ?,
            no_reprint = ?,
            charging_pay = ?,
            up_selection_reply = ?,
            up_close_reply = ?,
            up_close_danmu = ?
        where id = ?
        returning
            id,
            template_name,
            user,
            copyright,
            source,
            tid,
            cover,
            title,
            "desc",
            dynamic,
            tag,
            dtime,
            interactive,
            mission_id,
            dolby,
            lossless_music,
            no_reprint,
            charging_pay,
            up_selection_reply,
            up_close_reply,
            up_close_danmu
            "#,
        )
        .bind(studio.template_name)
        .bind(studio.user)
        .bind(studio.copyright)
        .bind(studio.source)
        .bind(studio.tid)
        .bind(studio.cover)
        .bind(studio.title)
        .bind(studio.desc)
        .bind(studio.dynamic)
        .bind(studio.tag)
        .bind(studio.dtime)
        .bind(studio.interactive)
        .bind(studio.mission_id)
        .bind(studio.dolby)
        .bind(studio.lossless_music)
        .bind(studio.no_reprint)
        .bind(studio.charging_pay)
        .bind(studio.up_selection_reply)
        .bind(studio.up_close_reply)
        .bind(studio.up_close_danmu)
        .bind(studio.id)
        .fetch_one(&self.pool)
        .await
        .context("an unexpected error occurred while updating the upload template")
    }

    async fn get_streamers(&self) -> anyhow::Result<Vec<StudioEntity>> {
        sqlx::query_as::<_, StudioEntity>(
            r#"select
            id,
            template_name,
            user,
            copyright,
            "source",
            tid,
            cover,
            title,
            "desc",
            "dynamic",
            tag,
            dtime,
            interactive,
            mission_id,
            dolby,
            lossless_music,
            no_reprint,
            charging_pay,
            up_selection_reply,
            up_close_reply,
            up_close_danmu
        from upload_streamers"#,
        )
        .fetch_all(&self.pool)
        .await
        .context("an unexpected error occurred retrieving streamers")
    }

    async fn get_streamer_by_id(&self, id: i64) -> anyhow::Result<StudioEntity> {
        sqlx::query_as::<_, StudioEntity>(
            r#"select
            id,
            template_name,
            user,
            copyright,
            "source",
            tid,
            cover,
            title,
            "desc",
            "dynamic",
            tag,
            dtime,
            interactive,
            mission_id,
            dolby,
            lossless_music,
            no_reprint,
            charging_pay,
            up_selection_reply,
            up_close_reply,
            up_close_danmu
        from upload_streamers
        where id = ?"#,
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .context("an unexpected error occurred retrieving streamers")
    }
}
