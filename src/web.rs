//! Steam Community web session for features that only exist on the web, such as sending
//! images in friend chat. The cookie is an access token obtained over the CM connection via
//! `RunCommand::GetWebAccessToken`.

use reqwest::multipart::Form;
use serde::Deserialize;
use sha1::{Digest, Sha1};

use crate::{
    connection::{Connection, ConnectionState},
    error::{Error, Result},
    protobuf::{
        CAuthenticationAccessTokenGenerateForAppRequest,
        CAuthenticationAccessTokenGenerateForAppResponse,
    },
    service_method::{self, ServiceMethod},
};

const GENERATE_ACCESS_TOKEN_METHOD: &str = "Authentication.GenerateAccessTokenForApp#1";
const BEGIN_FILE_UPLOAD_URL: &str = "https://steamcommunity.com/chat/beginfileupload/?l=english";
const COMMIT_FILE_UPLOAD_URL: &str = "https://steamcommunity.com/chat/commitfileupload/";
const CHAT_REFERER: &str = "https://steamcommunity.com/chat/";

pub struct WebSession {
    http: reqwest::Client,
    session_id: String,
    cookie: String,
}

impl WebSession {
    /// Create a web session from an access token (see `RunCommand::GetWebAccessToken`). For
    /// SteamClient tokens the access token itself is the `steamLoginSecure` cookie.
    pub fn new(steamid: u64, access_token: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36")
            .build()?;
        let session_id: String = (0..12)
            .map(|_| format!("{:02x}", rand::random::<u8>()))
            .collect();
        let cookie =
            format!("steamLoginSecure={steamid}%7C%7C{access_token}; sessionid={session_id}");
        Ok(Self {
            http,
            session_id,
            cookie,
        })
    }

    /// Upload an image and post it into the friend chat with `steamid`. Returns the image URL.
    pub async fn send_image_to_user(
        &self,
        steamid: u64,
        image: Vec<u8>,
        spoiler: bool,
    ) -> Result<String> {
        let size = imagesize::blob_size(&image)
            .map_err(|_| Error::InvalidResponse("image dimensions could not be read"))?;
        let mime = match imagesize::image_type(&image) {
            Ok(imagesize::ImageType::Png) => "png",
            Ok(imagesize::ImageType::Jpeg) => "jpeg",
            Ok(imagesize::ImageType::Gif) => "gif",
            Ok(imagesize::ImageType::Webp) => "webp",
            _ => {
                return Err(Error::Unsupported(
                    "image type (use png, jpeg, gif or webp)",
                ));
            }
        };
        let file_type = format!("image/{mime}");
        let file_name = format!(
            "{}_image.{}",
            unix_millis(),
            if mime == "jpeg" { "jpg" } else { mime }
        );
        let file_sha: String = Sha1::digest(&image)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let (width, height) = (size.width.to_string(), size.height.to_string());

        // hmac/timestamp have been seen both at the top level and inside `result`.
        #[derive(Deserialize)]
        struct BeginResponse {
            success: i32,
            hmac: Option<String>,
            timestamp: Option<serde_json::Value>,
            result: Option<BeginResult>,
        }
        #[derive(Deserialize)]
        struct BeginResult {
            ugcid: serde_json::Value,
            url_host: String,
            url_path: String,
            #[serde(default)]
            use_https: serde_json::Value,
            #[serde(default)]
            request_headers: Vec<UploadHeader>,
            hmac: Option<String>,
            timestamp: Option<serde_json::Value>,
        }
        #[derive(Deserialize)]
        struct UploadHeader {
            name: String,
            value: String,
        }

        let begin_form = Form::new()
            .text("sessionid", self.session_id.clone())
            .text("l", "english")
            .text("file_size", image.len().to_string())
            .text("file_name", file_name.clone())
            .text("file_sha", file_sha.clone())
            .text("file_image_width", width.clone())
            .text("file_image_height", height.clone())
            .text("file_type", file_type.clone());
        let begin: BeginResponse = self
            .community_post(BEGIN_FILE_UPLOAD_URL, begin_form)
            .await?;
        if begin.success != 1 {
            return Err(Error::Protocol(format!(
                "beginfileupload failed with eresult {}",
                begin.success
            )));
        }
        let Some(mut upload) = begin.result else {
            return Err(Error::InvalidResponse("beginfileupload returned no result"));
        };
        let (Some(hmac), Some(timestamp)) = (
            begin.hmac.or(upload.hmac.take()),
            begin.timestamp.or(upload.timestamp.take()),
        ) else {
            return Err(Error::InvalidResponse(
                "beginfileupload returned no hmac/timestamp",
            ));
        };

        // Seen as `1` as well as `true`.
        let use_https = matches!(upload.use_https, serde_json::Value::Bool(true))
            || upload.use_https.as_i64() == Some(1);
        let scheme = if use_https { "https" } else { "http" };
        let mut put = self
            .http
            .put(format!("{scheme}://{}{}", upload.url_host, upload.url_path));
        for header in &upload.request_headers {
            put = put.header(header.name.as_str(), header.value.as_str());
        }
        put.body(image).send().await?.error_for_status()?;

        #[derive(Deserialize)]
        struct CommitResponse {
            success: i32,
            result: Option<CommitResult>,
        }
        #[derive(Deserialize)]
        struct CommitResult {
            success: i32,
            details: Option<CommitDetails>,
        }
        #[derive(Deserialize)]
        struct CommitDetails {
            url: Option<String>,
        }

        let commit_form = Form::new()
            .text("sessionid", self.session_id.clone())
            .text("l", "english")
            .text("file_name", file_name)
            .text("file_sha", file_sha)
            .text("success", "1")
            .text("ugcid", json_scalar(&upload.ugcid))
            .text("file_type", file_type)
            .text("file_image_width", width)
            .text("file_image_height", height)
            .text("timestamp", json_scalar(&timestamp))
            .text("hmac", hmac)
            .text("friend_steamid", steamid.to_string())
            .text("spoiler", if spoiler { "1" } else { "0" });
        let commit: CommitResponse = self
            .community_post(COMMIT_FILE_UPLOAD_URL, commit_form)
            .await?;
        if commit.success != 1 {
            return Err(Error::Protocol(format!(
                "commitfileupload failed with eresult {}",
                commit.success
            )));
        }
        let result = commit.result.ok_or(Error::InvalidResponse(
            "commitfileupload returned no result",
        ))?;
        if result.success != 1 {
            return Err(Error::Protocol(format!(
                "commitfileupload result failed with eresult {}",
                result.success
            )));
        }
        result
            .details
            .and_then(|details| details.url)
            .ok_or(Error::InvalidResponse(
                "commitfileupload returned no image url",
            ))
    }

    async fn community_post<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        form: Form,
    ) -> Result<T> {
        let response = self
            .http
            .post(url)
            .header(reqwest::header::COOKIE, &self.cookie)
            .header(reqwest::header::REFERER, CHAT_REFERER)
            .multipart(form)
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        serde_json::from_str(&body).map_err(|error| {
            let excerpt: String = body.chars().take(200).collect();
            Error::Protocol(format!(
                "{url} returned HTTP {status}, unexpected body ({error}): {excerpt}"
            ))
        })
    }
}

/// Exchange a refresh token for an access token over the logged-on CM connection.
pub async fn generate_access_token(
    connection: &Connection,
    state: &ConnectionState,
    refresh_token: &str,
) -> Result<String> {
    let response: CAuthenticationAccessTokenGenerateForAppResponse = service_method::call_authed(
        connection,
        state,
        &ServiceMethod::new(GENERATE_ACCESS_TOKEN_METHOD),
        &CAuthenticationAccessTokenGenerateForAppRequest {
            refresh_token: Some(refresh_token.to_owned()),
            steamid: state.steamid,
            ..Default::default()
        },
    )
    .await?;
    response
        .access_token
        .filter(|token| !token.is_empty())
        .ok_or(Error::InvalidResponse(
            "GenerateAccessTokenForApp returned no access token",
        ))
}

/// Steam returns ids and timestamps sometimes as numbers and sometimes as strings.
fn json_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn unix_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default()
}
