use crate::server::core::download_actor::DownloadActorHandle;
use crate::server::core::live_streamers::{
    AddLiveStreamerDto, DynLiveStreamersRepository, DynLiveStreamersService, LiveStreamerDto,
    LiveStreamerEntity,
};
use crate::server::core::upload_streamers::{DynUploadStreamersRepository, StudioEntity};
use crate::server::core::users::{DynUsersRepository, User};
use crate::server::errors::AppResult;

use axum::extract::{Path, State};
use axum::{Extension, Json};
use std::sync::LazyLock;

static STREAMER_MUTATION_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

pub async fn get_streamers_endpoint(
    Extension(streamers_service): Extension<DynLiveStreamersService>,
    Extension(download_actor_handle): Extension<DownloadActorHandle>,
) -> AppResult<Json<Vec<LiveStreamerDto>>> {
    let map = download_actor_handle.get_streamers();
    let mut vec = streamers_service.get_streamers().await?;
    for live in vec.iter_mut() {
        live.status = map.get(&live.url).copied().unwrap_or_default()
    }
    Ok(Json(vec))
}

pub async fn get_streamer_endpoint(
    Extension(streamers_service): Extension<DynLiveStreamersService>,
    Extension(download_actor_handle): Extension<DownloadActorHandle>,
    Path(id): Path<i64>,
) -> AppResult<Json<LiveStreamerDto>> {
    Ok(Json(streamers_service.get_streamer_by_id(id).await?))
}

pub async fn add_streamer_endpoint(
    Extension(streamers_service): Extension<DynLiveStreamersService>,
    Extension(download_actor_handle): Extension<DownloadActorHandle>,
    Json(request): Json<AddLiveStreamerDto>,
) -> AppResult<Json<LiveStreamerDto>> {
    let _guard = STREAMER_MUTATION_LOCK.lock().await;
    let created = streamers_service.add_streamer(request).await?;
    download_actor_handle.add_streamer(&created.url);
    Ok(Json(created))
}

pub async fn delete_streamer_endpoint(
    State(state): State<DynLiveStreamersRepository>,
    Extension(download_actor_handle): Extension<DownloadActorHandle>,
    Path(id): Path<i64>,
) -> AppResult<Json<()>> {
    let _guard = STREAMER_MUTATION_LOCK.lock().await;
    let existing = state.get_streamer_by_id(id).await?;
    state.delete_streamer(id).await?;
    download_actor_handle.remove_streamer(&existing.url);
    Ok(Json(()))
}

pub async fn update_streamer_endpoint(
    State(state): State<DynLiveStreamersRepository>,
    Extension(download_actor_handle): Extension<DownloadActorHandle>,
    Path(id): Path<i64>,
    Json(mut request): Json<LiveStreamerEntity>,
) -> AppResult<Json<LiveStreamerDto>> {
    let _guard = STREAMER_MUTATION_LOCK.lock().await;
    let old_url = state.get_streamer_by_id(id).await?.url;
    request.id = id;
    let updated = state.update_streamer(request).await?;
    if old_url != updated.url {
        download_actor_handle.remove_streamer(&old_url);
    }
    download_actor_handle.update_streamer(&updated.url);
    Ok(Json(updated.into_dto()))
}

pub async fn add_upload_streamer_endpoint(
    Extension(streamers_service): Extension<DynUploadStreamersRepository>,
    Json(request): Json<StudioEntity>,
) -> AppResult<Json<StudioEntity>> {
    Ok(Json(streamers_service.create_streamer(request).await?))
}

pub async fn delete_template_endpoint(
    Extension(streamers_service): Extension<DynUploadStreamersRepository>,
    Path(id): Path<i64>,
) -> AppResult<Json<()>> {
    Ok(Json(streamers_service.delete_streamer(id).await?))
}

pub async fn update_template_endpoint(
    Extension(streamers_service): Extension<DynUploadStreamersRepository>,
    Path(id): Path<i64>,
    Json(mut request): Json<StudioEntity>,
) -> AppResult<Json<StudioEntity>> {
    request.id = id;
    Ok(Json(streamers_service.update_streamer(request).await?))
}

pub async fn get_upload_streamers_endpoint(
    Extension(streamers_service): Extension<DynUploadStreamersRepository>,
) -> AppResult<Json<Vec<StudioEntity>>> {
    Ok(Json(streamers_service.get_streamers().await?))
}

pub async fn get_upload_streamer_endpoint(
    Extension(streamers_service): Extension<DynUploadStreamersRepository>,
    Path(id): Path<i64>,
) -> AppResult<Json<StudioEntity>> {
    Ok(Json(streamers_service.get_streamer_by_id(id).await?))
}

pub async fn get_users_endpoint(
    State(state): State<DynUsersRepository>,
) -> AppResult<Json<Vec<User>>> {
    Ok(Json(state.get_users().await?))
}

pub async fn add_user_endpoint(
    State(state): State<DynUsersRepository>,
    Json(request): Json<User>,
) -> AppResult<Json<User>> {
    Ok(Json(state.create_user(request).await?))
}

pub async fn delete_user_endpoint(
    State(state): State<DynUsersRepository>,
    Path(id): Path<i64>,
) -> AppResult<Json<()>> {
    Ok(Json(state.delete_user(id).await?))
}
