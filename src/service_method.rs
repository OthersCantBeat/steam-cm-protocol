use prost::Message;

use crate::{
    connection::{Connection, ConnectionState},
    emsg::EMsg,
    error::{Error, Result},
    protobuf::CMsgProtoBufHeader,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceMethod {
    pub target_job_name: String,
}

impl ServiceMethod {
    pub fn new(target_job_name: impl Into<String>) -> Self {
        Self {
            target_job_name: target_job_name.into(),
        }
    }
}

pub async fn call<Req, Resp>(
    connection: &Connection,
    method: &ServiceMethod,
    request: &Req,
) -> Result<Resp>
where
    Req: Message,
    Resp: Message + Default,
{
    let packet = connection
        .request(
            EMsg::ServiceMethodCallFromClientNonAuthed,
            CMsgProtoBufHeader {
                target_job_name: Some(method.target_job_name.clone()),
                ..Default::default()
            },
            request,
        )
        .await?;

    packet.decode_body::<Resp>()
}

/// Like `call` but uses `ServiceMethodCallFromClient` with the logged-on session
/// headers (steamid + client_sessionid). Required for user-scoped service methods.
pub async fn call_authed<Req, Resp>(
    connection: &Connection,
    state: &ConnectionState,
    method: &ServiceMethod,
    request: &Req,
) -> Result<Resp>
where
    Req: Message,
    Resp: Message + Default,
{
    let packet = connection
        .request(
            EMsg::ServiceMethodCallFromClient,
            CMsgProtoBufHeader {
                steamid: state.steamid,
                client_sessionid: state.client_session_id,
                target_job_name: Some(method.target_job_name.clone()),
                ..Default::default()
            },
            request,
        )
        .await?;

    packet.decode_body::<Resp>()
}

/// Like `call_authed`, but fails when Steam answers with an EResult other than OK. Methods
/// with an empty response (e.g. invites) otherwise "succeed" silently on errors.
pub async fn call_authed_checked<Req, Resp>(
    connection: &Connection,
    state: &ConnectionState,
    method: &ServiceMethod,
    request: &Req,
) -> Result<Resp>
where
    Req: Message,
    Resp: Message + Default,
{
    let packet = connection
        .request(
            EMsg::ServiceMethodCallFromClient,
            CMsgProtoBufHeader {
                steamid: state.steamid,
                client_sessionid: state.client_session_id,
                target_job_name: Some(method.target_job_name.clone()),
                ..Default::default()
            },
            request,
        )
        .await?;

    let eresult = packet.header.eresult.unwrap_or(1);
    if eresult != 1 {
        let detail = packet
            .header
            .error_message
            .as_deref()
            .filter(|m| !m.is_empty())
            .map(|m| format!(": {m}"))
            .unwrap_or_default();
        return Err(Error::Protocol(format!(
            "{} failed with EResult {eresult}{detail}",
            method.target_job_name
        )));
    }
    packet.decode_body::<Resp>()
}
