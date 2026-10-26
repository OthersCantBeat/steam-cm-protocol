//! Steam community group chats (`ChatRoom.*` unified service): resolving an invite link and
//! inviting a friend into the chat room group.
//!
//! The messages are declared by hand instead of compiling `steammessages_chat.steamclient.proto`,
//! which pulls in a large tree of imports for the two calls needed here. Field numbers follow
//! SteamDatabase/Protobufs (`steam/steammessages_chat.steamclient.proto`).

use crate::{
    connection::{Connection, ConnectionState},
    error::{Error, Result},
    service_method::{ServiceMethod, call_authed_checked},
};

const GET_INVITE_LINK_INFO_METHOD: &str = "ChatRoom.GetInviteLinkInfo#1";
const INVITE_FRIEND_METHOD: &str = "ChatRoom.InviteFriendToChatRoomGroup#1";

#[derive(Clone, PartialEq, prost::Message)]
pub struct CChatRoomGetInviteLinkInfoRequest {
    #[prost(string, optional, tag = "1")]
    pub invite_code: Option<String>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CChatRoomGetInviteLinkInfoResponse {
    #[prost(fixed64, optional, tag = "3")]
    pub steamid_sender: Option<u64>,
    #[prost(uint32, optional, tag = "4")]
    pub time_expires: Option<u32>,
    #[prost(uint64, optional, tag = "6")]
    pub chat_id: Option<u64>,
    #[prost(message, optional, tag = "8")]
    pub group_summary: Option<CChatRoomGroupSummary>,
    #[prost(bool, optional, tag = "11")]
    pub banned: Option<bool>,
}

/// The fields of `CChatRoom_GetChatRoomGroupSummary_Response` used here.
#[derive(Clone, PartialEq, prost::Message)]
pub struct CChatRoomGroupSummary {
    #[prost(uint64, optional, tag = "1")]
    pub chat_group_id: Option<u64>,
    #[prost(string, optional, tag = "2")]
    pub chat_group_name: Option<String>,
    #[prost(uint64, optional, tag = "5")]
    pub default_chat_id: Option<u64>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CChatRoomInviteFriendToChatRoomGroupRequest {
    #[prost(uint64, optional, tag = "1")]
    pub chat_group_id: Option<u64>,
    #[prost(fixed64, optional, tag = "2")]
    pub steamid: Option<u64>,
    #[prost(uint64, optional, tag = "3")]
    pub chat_id: Option<u64>,
    #[prost(bool, optional, tag = "4")]
    pub skip_friendsui_check: Option<bool>,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct CChatRoomInviteFriendToChatRoomGroupResponse {}

/// The chat room group an invite link points to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatInviteLink {
    pub chat_group_id: u64,
    /// Room the link opens (0 = the group's default room).
    pub chat_id: u64,
    pub group_name: String,
    /// Unix seconds; None for links that never expire.
    pub expires_at: Option<u32>,
}

/// Extracts the invite code from `https://s.team/chat/<code>`,
/// `https://steamcommunity.com/chat/invite/<code>` or a bare code.
pub fn parse_invite_code(input: &str) -> Option<String> {
    let trimmed = input.trim().trim_end_matches('/');
    let code = trimmed.rsplit('/').next().unwrap_or(trimmed);
    let code = code.split(['?', '#']).next().unwrap_or(code);
    let valid = !code.is_empty()
        && code.len() <= 32
        && code
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    valid.then(|| code.to_owned())
}

/// Resolve an invite code (see [`parse_invite_code`]) to its chat room group.
pub async fn get_invite_link_info(
    connection: &Connection,
    state: &ConnectionState,
    invite_code: &str,
) -> Result<ChatInviteLink> {
    let response: CChatRoomGetInviteLinkInfoResponse = call_authed_checked(
        connection,
        state,
        &ServiceMethod::new(GET_INVITE_LINK_INFO_METHOD),
        &CChatRoomGetInviteLinkInfoRequest {
            invite_code: Some(invite_code.to_owned()),
        },
    )
    .await?;
    let summary = response.group_summary.unwrap_or_default();
    let chat_group_id =
        summary
            .chat_group_id
            .filter(|id| *id != 0)
            .ok_or(Error::InvalidResponse(
                "invite link is unknown or expired (no chat group)",
            ))?;
    Ok(ChatInviteLink {
        chat_group_id,
        chat_id: response
            .chat_id
            .filter(|id| *id != 0)
            .or(summary.default_chat_id)
            .unwrap_or_default(),
        group_name: summary.chat_group_name.unwrap_or_default(),
        expires_at: response.time_expires.filter(|t| *t != 0),
    })
}

/// Invite a friend of the logged-on account into a chat room group. The account must be a
/// member of the group with the right to invite, and `steamid` must be its friend.
pub async fn invite_friend_to_group(
    connection: &Connection,
    state: &ConnectionState,
    chat_group_id: u64,
    chat_id: Option<u64>,
    steamid: u64,
) -> Result<()> {
    let _: CChatRoomInviteFriendToChatRoomGroupResponse = call_authed_checked(
        connection,
        state,
        &ServiceMethod::new(INVITE_FRIEND_METHOD),
        &CChatRoomInviteFriendToChatRoomGroupRequest {
            chat_group_id: Some(chat_group_id),
            steamid: Some(steamid),
            chat_id: chat_id.filter(|id| *id != 0),
            skip_friendsui_check: Some(false),
        },
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn parses_invite_codes() {
        assert_eq!(
            parse_invite_code("https://s.team/chat/AbC1dE2f").as_deref(),
            Some("AbC1dE2f")
        );
        assert_eq!(
            parse_invite_code(" https://steamcommunity.com/chat/invite/AbC1dE2f/ ").as_deref(),
            Some("AbC1dE2f")
        );
        assert_eq!(
            parse_invite_code("https://s.team/chat/AbC1dE2f?l=german").as_deref(),
            Some("AbC1dE2f")
        );
        assert_eq!(parse_invite_code("AbC1dE2f").as_deref(), Some("AbC1dE2f"));
        assert_eq!(parse_invite_code(""), None);
        assert_eq!(parse_invite_code("https://s.team/chat/ab cd"), None);
    }

    #[test]
    fn invite_request_wire_format() {
        let bytes = CChatRoomInviteFriendToChatRoomGroupRequest {
            chat_group_id: Some(5),
            steamid: Some(76561197960287930),
            chat_id: None,
            skip_friendsui_check: Some(false),
        }
        .encode_to_vec();
        // tag 1 varint, tag 2 fixed64, tag 4 varint
        assert_eq!(bytes[0], 0x08);
        assert_eq!(bytes[2], 0x11);
        assert_eq!(&bytes[3..11], &76561197960287930u64.to_le_bytes());
        assert_eq!(bytes[11], 0x20);
    }

    #[test]
    fn decodes_invite_link_info() {
        let response = CChatRoomGetInviteLinkInfoResponse {
            chat_id: Some(0),
            group_summary: Some(CChatRoomGroupSummary {
                chat_group_id: Some(42),
                chat_group_name: Some("OCB".into()),
                default_chat_id: Some(7),
            }),
            ..Default::default()
        };
        let decoded =
            CChatRoomGetInviteLinkInfoResponse::decode(response.encode_to_vec().as_slice())
                .unwrap();
        assert_eq!(decoded.group_summary.unwrap().chat_group_id, Some(42));
    }
}
