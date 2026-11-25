use crate::server::core::users::DynUsersRepository;
use crate::server::errors::{AppError, AppResult};
use axum::extract::{Query, State};
use axum::Json;
use biliup::uploader::credential::login_by_cookies;
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

const MAX_PROXY_BODY_BYTES: usize = 10 * 1024 * 1024;
const PROXY_TIMEOUT: Duration = Duration::from_secs(15);

static PROXY_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(PROXY_TIMEOUT)
        .build()
        .expect("failed to build proxy reqwest client")
});

fn validate_proxy_url(raw_url: &str) -> AppResult<reqwest::Url> {
    let parsed = reqwest::Url::parse(raw_url)
        .map_err(|_| AppError::BadRequest("invalid url parameter".into()))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(AppError::BadRequest("unsupported url scheme".into()));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AppError::BadRequest(
            "credentials in url are not allowed".into(),
        ));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| AppError::BadRequest("missing host in url".into()))?
        .trim_end_matches('.')
        .to_ascii_lowercase();
    if host != "hdslb.com" && !host.ends_with(".hdslb.com") {
        return Err(AppError::BadRequest("disallowed proxy host".into()));
    }
    Ok(parsed)
}

pub async fn archive_pre_endpoint(
    Query(_params): Query<HashMap<String, String>>,
) -> AppResult<Json<serde_json::Value>> {
    let bili = login_by_cookies("cookies.json", None).await?;
    Ok(Json(bili.archive_pre().await?))
}

async fn ensure_registered_cookie_file(
    users_repo: &DynUsersRepository,
    user: &str,
) -> AppResult<()> {
    let users = users_repo.get_users().await?;
    if !users.iter().any(|item| item.value == user) {
        return Err(AppError::Forbidden("unregistered cookie file".into()));
    }
    Ok(())
}

pub async fn get_myinfo_endpoint(
    State(users_repo): State<DynUsersRepository>,
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<Json<serde_json::Value>> {
    let user = params
        .get("user")
        .filter(|u| !u.is_empty())
        .ok_or_else(|| AppError::BadRequest("missing user parameter".into()))?;
    ensure_registered_cookie_file(&users_repo, user).await?;
    let bili = login_by_cookies(user, None).await?;
    Ok(Json(bili.my_info().await?))
}

pub async fn get_proxy_endpoint(
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<Bytes> {
    let raw_url = params
        .get("url")
        .filter(|u| !u.is_empty())
        .ok_or_else(|| AppError::BadRequest("missing url parameter".into()))?;
    let parsed = validate_proxy_url(raw_url)?;

    let mut resp = PROXY_CLIENT.get(parsed).send().await?;
    if let Some(len) = resp.content_length()
        && len > MAX_PROXY_BODY_BYTES as u64
    {
        return Err(AppError::PayloadTooLarge(
            "proxy response exceeds 10MB limit".into(),
        ));
    }

    let mut buf = BytesMut::new();
    while let Some(chunk) = resp.chunk().await? {
        if buf.len().saturating_add(chunk.len()) > MAX_PROXY_BODY_BYTES {
            return Err(AppError::PayloadTooLarge(
                "proxy response exceeds 10MB limit".into(),
            ));
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::{ensure_registered_cookie_file, get_myinfo_endpoint, validate_proxy_url};
    use crate::server::core::users::{DynUsersRepository, User, UsersRepository};
    use crate::server::errors::AppError;
    use async_trait::async_trait;
    use axum::extract::{Query, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use std::collections::HashMap;
    use std::sync::Arc;

    struct FakeUsersRepo {
        users: Vec<User>,
    }

    #[async_trait]
    impl UsersRepository for FakeUsersRepo {
        async fn create_user(&self, user: User) -> anyhow::Result<User> {
            Ok(user)
        }

        async fn get_users(&self) -> anyhow::Result<Vec<User>> {
            Ok(self.users.iter().map(|u| User {
                id: u.id,
                name: u.name.clone(),
                value: u.value.clone(),
                platform: u.platform.clone(),
            }).collect())
        }

        async fn delete_user(&self, _id: i64) -> anyhow::Result<()> {
            Ok(())
        }

        async fn update_user(&self, user: User) -> anyhow::Result<User> {
            Ok(user)
        }

        async fn get_user_by_id(&self, _id: i64) -> anyhow::Result<User> {
            anyhow::bail!("not implemented")
        }
    }

    #[test]
    fn test_validate_proxy_url_allowlist() {
        assert!(validate_proxy_url("https://i0.hdslb.com/bfs/archive/abc.jpg").is_ok());
        assert!(validate_proxy_url("http://hdslb.com/test.png").is_ok());
        assert!(validate_proxy_url("https://evil-hdslb.com/a.jpg").is_err());
        assert!(validate_proxy_url("https://hdslb.com.evil.com/a.jpg").is_err());
        assert!(validate_proxy_url("file:///etc/passwd").is_err());
        assert!(validate_proxy_url("http://127.0.0.1:8080/secret").is_err());
        assert!(validate_proxy_url("https://user:pass@i0.hdslb.com/a.jpg").is_err());
    }

    #[tokio::test]
    async fn test_get_myinfo_cookie_allowlist() {
        let repo: DynUsersRepository = Arc::new(FakeUsersRepo {
            users: vec![User {
                id: 1,
                name: "tester".into(),
                value: "data/registered.json".into(),
                platform: "bilibili".into(),
            }],
        });

        // 缺少 user 参数 -> 400
        let missing_res = get_myinfo_endpoint(State(repo.clone()), Query(HashMap::new()))
            .await
            .unwrap_err();
        assert_eq!(missing_res.into_response().status(), StatusCode::BAD_REQUEST);

        // 未登记的路径 -> 403 Forbidden
        let mut bad_params = HashMap::new();
        bad_params.insert("user".into(), "../etc/passwd".into());
        let bad_err = get_myinfo_endpoint(State(repo.clone()), Query(bad_params))
            .await
            .unwrap_err();
        assert!(matches!(bad_err, AppError::Forbidden(_)));
        assert_eq!(bad_err.into_response().status(), StatusCode::FORBIDDEN);

        // 已登记路径可通过白名单校验
        assert!(ensure_registered_cookie_file(&repo, "data/registered.json").await.is_ok());
        assert!(ensure_registered_cookie_file(&repo, "data/other.json").await.is_err());
    }
}
