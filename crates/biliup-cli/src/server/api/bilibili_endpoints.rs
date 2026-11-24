use crate::server::errors::{AppError, AppResult};
use axum::extract::Query;
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

pub async fn get_myinfo_endpoint(
    Query(params): Query<HashMap<String, String>>,
) -> AppResult<Json<serde_json::Value>> {
    let user = params
        .get("user")
        .filter(|u| !u.is_empty())
        .ok_or_else(|| AppError::BadRequest("missing user parameter".into()))?;
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
    use super::validate_proxy_url;

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
}
