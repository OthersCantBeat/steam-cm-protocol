use crate::{
    emsg::EMsg,
    message::Packet,
    protobuf::{
        CMsgClientAddFriend, CMsgClientAddFriendResponse, CMsgClientChangeStatus,
        CMsgClientFriendsList, CMsgClientPersonaState, CMsgClientRemoveFriend,
        CMsgClientRequestFriendData, CMsgProtoBufHeader,
    },
};

// EClientPersonaStateFlag: Status=1 | PlayerName=2 | LastSeen=64 | GameExtraInfo=256
const PERSONA_STATE_REQUESTED: u32 = 1 | 2 | 64 | 256;

// EFriendRelationship::RequestRecipient (someone sent us a friend request)
const RELATIONSHIP_REQUEST_RECIPIENT: u32 = 2;
// EFriendRelationship::Friend
const RELATIONSHIP_FRIEND: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonaState {
    Offline,
    Online,
    Busy,
    Away,
    Snooze,
    LookingToTrade,
    LookingToPlay,
    Invisible,
}

impl PersonaState {
    pub fn from_raw(raw: u32) -> Self {
        match raw {
            1 => Self::Online,
            2 => Self::Busy,
            3 => Self::Away,
            4 => Self::Snooze,
            5 => Self::LookingToTrade,
            6 => Self::LookingToPlay,
            7 => Self::Invisible,
            _ => Self::Offline,
        }
    }

    pub fn to_raw(self) -> u32 {
        match self {
            Self::Offline => 0,
            Self::Online => 1,
            Self::Busy => 2,
            Self::Away => 3,
            Self::Snooze => 4,
            Self::LookingToTrade => 5,
            Self::LookingToPlay => 6,
            Self::Invisible => 7,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Friend {
    pub steamid: u64,
    pub relationship: u32,
}

#[derive(Debug, Clone)]
pub struct Persona {
    pub steamid: u64,
    pub name: String,
    pub state: PersonaState,
    pub game_app_id: Option<u32>,
    pub game_name: Option<String>,
    pub avatar_hash: Option<Vec<u8>>,
    /// True when this update explicitly included game fields (even if they were cleared).
    /// False when game fields were absent from the protobuf — existing values should be kept.
    pub game_fields_present: bool,
}

/// One entry from an app's PICS `config/launch` block — how Steam itself would start the game.
/// Used by the direct (no-Steam) launch path to resolve the executable without the Steam client.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LaunchEntry {
    /// Executable path, relative to the game's install directory (e.g. `bin/game.exe`).
    pub executable: String,
    /// Launch arguments, if any.
    pub arguments: Option<String>,
    /// Working directory, relative to the install directory; `None` means the install root.
    pub workingdir: Option<String>,
    /// `config/launch[i].type` — "default", "none", "config", … `None`/empty when unset.
    pub launch_type: Option<String>,
    /// `config/launch[i].config.oslist` — comma list ("windows", "linux", "macos").
    pub oslist: Option<String>,
    /// `config/launch[i].config.osarch` — "32" / "64".
    pub osarch: Option<String>,
    /// `config/launch[i].config.betakey` — present only for beta-gated launch options.
    pub betakey: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProtocolGame {
    pub appid: u32,
    pub name: String,
    pub playtime_forever: i32,
    pub rtime_last_played: u32,
    pub img_icon_url: Option<String>,
    /// Steam appinfo `common.type` (lowercased): "game", "application", "tool", … `None` when
    /// the appinfo could not be resolved. Used by the TUI to filter the library by type.
    pub app_type: Option<String>,
    /// Appinfo `config.installdir` — the game's folder name under `steamapps/common/`. `None`
    /// when unresolved or for the recently-played / Web-API fallback paths.
    pub installdir: Option<String>,
    /// Appinfo `config/launch` entries (how Steam would launch it). Empty when unresolved.
    pub launch: Vec<LaunchEntry>,
}

#[derive(Debug, Clone)]
pub struct ProtocolAchievement {
    pub apiname: String,
    pub achieved: bool,
    pub unlocktime: u64,
    pub name: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug)]
pub enum FriendsEvent {
    FriendsList(Vec<Friend>),
    PersonaStates(Vec<Persona>),
    RecentlyPlayedGames(Vec<ProtocolGame>),
    OwnedGames(Vec<ProtocolGame>),
    PlayerAchievements {
        appid: u32,
        achievements: Vec<ProtocolAchievement>,
    },
    /// A 1-on-1 friend message arrived (or a cross-session echo of our own send).
    IncomingMessage(crate::chat::ChatMessage),
    /// One of our own outgoing messages was confirmed by Steam, stamped with the authoritative
    /// `server_timestamp` + `ordinal` (so the UI can append/dedupe it like any other message).
    MessageSent(crate::chat::ChatMessage),
    /// A friend started typing in the 1-on-1 conversation.
    TypingNotification {
        steamid: u64,
    },
    /// Result of a `GetRecentMessages` history fetch for one conversation (oldest first).
    RecentMessages {
        steamid: u64,
        messages: Vec<crate::chat::ChatMessage>,
    },
    /// Steam's response to a `AddFriend` request. A successful result means the request was
    /// accepted by Steam; the other user may still need to accept the friend request.
    FriendRequestResponse {
        result: i32,
        steamid: Option<u64>,
        persona_name: Option<String>,
    },
    /// Users who sent us a friend request. Accept one with `RunCommand::AddFriend`.
    FriendRequestsReceived(Vec<u64>),
    /// Raw friends list entries with every relationship (`EFriendRelationship`: 0 none/removed,
    /// 2 request recipient, 3 friend, 4 request initiator, ...). `incremental: false` is the full
    /// list sent at logon; incremental updates only carry the changed entries.
    FriendRelationships {
        incremental: bool,
        entries: Vec<(u64, u32)>,
    },
}

/// Decode a packet into a FriendsEvent, returning None for unrelated packets.
pub fn decode(packet: &Packet) -> Option<FriendsEvent> {
    if packet.emsg == EMsg::ClientAddFriendResponse.raw() {
        let msg = packet.decode_body::<CMsgClientAddFriendResponse>().ok()?;
        return Some(FriendsEvent::FriendRequestResponse {
            result: msg.eresult(),
            steamid: msg.steam_id_added.filter(|&id| id != 0),
            persona_name: msg.persona_name_added.filter(|name| !name.is_empty()),
        });
    }

    if packet.emsg == EMsg::ClientFriendsList.raw() {
        let msg = packet.decode_body::<CMsgClientFriendsList>().ok()?;
        let friends = msg
            .friends
            .into_iter()
            .filter(|f| f.efriendrelationship() == RELATIONSHIP_FRIEND)
            .map(|f| Friend {
                steamid: f.ulfriendid(),
                relationship: f.efriendrelationship(),
            })
            .collect();
        return Some(FriendsEvent::FriendsList(friends));
    }

    if packet.emsg == EMsg::ClientPersonaState.raw() {
        let msg = packet.decode_body::<CMsgClientPersonaState>().ok()?;
        let personas = msg
            .friends
            .into_iter()
            .map(|f| {
                let game_fields_present = f.game_played_app_id.is_some() || f.game_name.is_some();
                Persona {
                    steamid: f.friendid(),
                    state: PersonaState::from_raw(f.persona_state()),
                    name: f.player_name.unwrap_or_default(),
                    game_app_id: f.game_played_app_id.filter(|&id| id != 0),
                    game_name: f.game_name.filter(|s| !s.is_empty()),
                    avatar_hash: f.avatar_hash.filter(|h| !h.is_empty()),
                    game_fields_present,
                }
            })
            .collect();
        return Some(FriendsEvent::PersonaStates(personas));
    }

    None
}

/// Decode pending incoming friend requests from a friends list packet. This is separate from
/// `decode` because the same packet also yields `FriendsEvent::FriendsList`.
pub fn decode_friend_requests(packet: &Packet) -> Option<FriendsEvent> {
    if packet.emsg != EMsg::ClientFriendsList.raw() {
        return None;
    }
    let msg = packet.decode_body::<CMsgClientFriendsList>().ok()?;
    let requests: Vec<u64> = msg
        .friends
        .into_iter()
        .filter(|f| f.efriendrelationship() == RELATIONSHIP_REQUEST_RECIPIENT)
        .map(|f| f.ulfriendid())
        .collect();
    (!requests.is_empty()).then_some(FriendsEvent::FriendRequestsReceived(requests))
}

/// Decode every entry of a friends list packet, see `FriendsEvent::FriendRelationships`.
pub fn decode_friend_relationships(packet: &Packet) -> Option<FriendsEvent> {
    if packet.emsg != EMsg::ClientFriendsList.raw() {
        return None;
    }
    let msg = packet.decode_body::<CMsgClientFriendsList>().ok()?;
    Some(FriendsEvent::FriendRelationships {
        incremental: msg.bincremental.unwrap_or(false),
        entries: msg
            .friends
            .into_iter()
            .map(|f| (f.ulfriendid(), f.efriendrelationship()))
            .collect(),
    })
}

/// Individual SteamID64s are this offset plus the 32-bit account id.
const STEAMID64_INDIVIDUAL_BASE: u64 = 76561197960265728;

/// Steam levels of the given users (friends or the logged-on user), keyed by SteamID64.
pub async fn get_steam_levels(
    connection: &crate::connection::Connection,
    connection_state: &crate::connection::ConnectionState,
    steamids: &[u64],
) -> crate::error::Result<std::collections::HashMap<u64, u32>> {
    let header = CMsgProtoBufHeader {
        steamid: connection_state.steamid,
        client_sessionid: connection_state.client_session_id,
        ..Default::default()
    };
    let body = crate::protobuf::CMsgClientFsGetFriendsSteamLevels {
        accountids: steamids
            .iter()
            .map(|id| id.wrapping_sub(STEAMID64_INDIVIDUAL_BASE) as u32)
            .collect(),
    };
    let packet = connection
        .request(EMsg::ClientFSGetFriendsSteamLevels, header, &body)
        .await?;
    let response =
        packet.decode_body::<crate::protobuf::CMsgClientFsGetFriendsSteamLevelsResponse>()?;
    Ok(response
        .friends
        .into_iter()
        .filter_map(|f| {
            Some((
                STEAMID64_INDIVIDUAL_BASE + u64::from(f.accountid?),
                f.level?,
            ))
        })
        .collect())
}

pub fn build_remove_friend(
    connection_state: &crate::connection::ConnectionState,
    steamid: u64,
) -> (CMsgProtoBufHeader, CMsgClientRemoveFriend) {
    let header = CMsgProtoBufHeader {
        steamid: connection_state.steamid,
        client_sessionid: connection_state.client_session_id,
        ..Default::default()
    };
    let body = CMsgClientRemoveFriend {
        friendid: Some(steamid),
    };
    (header, body)
}

pub fn build_add_friend(
    connection_state: &crate::connection::ConnectionState,
    steamid: u64,
) -> (CMsgProtoBufHeader, CMsgClientAddFriend) {
    let header = CMsgProtoBufHeader {
        steamid: connection_state.steamid,
        client_sessionid: connection_state.client_session_id,
        ..Default::default()
    };
    let body = CMsgClientAddFriend {
        steamid_to_add: Some(steamid),
        ..Default::default()
    };
    (header, body)
}

pub fn build_request_friend_data(
    connection_state: &crate::connection::ConnectionState,
    steamids: Vec<u64>,
) -> (CMsgProtoBufHeader, CMsgClientRequestFriendData) {
    let header = CMsgProtoBufHeader {
        steamid: connection_state.steamid,
        client_sessionid: connection_state.client_session_id,
        ..Default::default()
    };
    let body = CMsgClientRequestFriendData {
        persona_state_requested: Some(PERSONA_STATE_REQUESTED),
        friends: steamids,
    };
    (header, body)
}

/// Like `build_change_status`, but also sets the profile name shown to friends.
pub fn build_change_status_with_name(
    connection_state: &crate::connection::ConnectionState,
    state: PersonaState,
    player_name: String,
) -> (CMsgProtoBufHeader, CMsgClientChangeStatus) {
    let (header, mut body) = build_change_status(connection_state, state);
    body.player_name = Some(player_name);
    (header, body)
}

pub fn build_change_status(
    connection_state: &crate::connection::ConnectionState,
    state: PersonaState,
) -> (CMsgProtoBufHeader, CMsgClientChangeStatus) {
    let header = CMsgProtoBufHeader {
        steamid: connection_state.steamid,
        client_sessionid: connection_state.client_session_id,
        ..Default::default()
    };
    let body = CMsgClientChangeStatus {
        persona_state: Some(state.to_raw()),
        persona_set_by_user: Some(true),
        ..Default::default()
    };
    (header, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        emsg::EMsg,
        message::{decode_frame, encode_message},
        protobuf::{CMsgClientPersonaState, CMsgProtoBufHeader},
    };

    #[test]
    fn decodes_persona_state_into_friends_event() {
        let mut friend = CMsgClientPersonaState::default();
        let f = crate::protobuf::c_msg_client_persona_state::Friend {
            friendid: Some(76561198000000001),
            player_name: Some("Alice".to_owned()),
            persona_state: Some(1), // Online
            game_name: Some("Half-Life 2".to_owned()),
            game_played_app_id: Some(220),
            ..Default::default()
        };
        friend.friends.push(f);

        let header = CMsgProtoBufHeader::default();
        let encoded = encode_message(EMsg::ClientPersonaState, &header, &friend).unwrap();
        let packets = decode_frame(&encoded).unwrap();
        assert_eq!(packets.len(), 1);

        let event = decode(&packets[0]).expect("should decode as FriendsEvent");
        match event {
            FriendsEvent::PersonaStates(personas) => {
                assert_eq!(personas.len(), 1);
                assert_eq!(personas[0].steamid, 76561198000000001);
                assert_eq!(personas[0].name, "Alice");
                assert_eq!(personas[0].state, PersonaState::Online);
                assert_eq!(personas[0].game_app_id, Some(220));
                assert_eq!(personas[0].game_name.as_deref(), Some("Half-Life 2"));
                assert!(personas[0].game_fields_present);
            }
            _ => panic!("expected PersonaStates"),
        }
    }

    #[test]
    fn filters_non_friend_relationships() {
        let mut msg = crate::protobuf::CMsgClientFriendsList::default();
        // relationship 3 = Friend, 1 = Blocked
        let friend_entry = crate::protobuf::c_msg_client_friends_list::Friend {
            ulfriendid: Some(76561198000000002),
            efriendrelationship: Some(3),
        };
        msg.friends.push(friend_entry);
        let blocked_entry = crate::protobuf::c_msg_client_friends_list::Friend {
            ulfriendid: Some(76561198000000099),
            efriendrelationship: Some(1),
        };
        msg.friends.push(blocked_entry);

        let header = CMsgProtoBufHeader::default();
        let encoded = encode_message(EMsg::ClientFriendsList, &header, &msg).unwrap();
        let packets = decode_frame(&encoded).unwrap();

        let event = decode(&packets[0]).expect("should decode");
        match event {
            FriendsEvent::FriendsList(friends) => {
                assert_eq!(friends.len(), 1);
                assert_eq!(friends[0].steamid, 76561198000000002);
            }
            _ => panic!("expected FriendsList"),
        }
    }

    #[test]
    fn decodes_incoming_friend_requests() {
        let mut msg = crate::protobuf::CMsgClientFriendsList::default();
        // relationship 2 = RequestRecipient, 3 = Friend
        msg.friends
            .push(crate::protobuf::c_msg_client_friends_list::Friend {
                ulfriendid: Some(76561198000000003),
                efriendrelationship: Some(2),
            });
        msg.friends
            .push(crate::protobuf::c_msg_client_friends_list::Friend {
                ulfriendid: Some(76561198000000002),
                efriendrelationship: Some(3),
            });

        let header = CMsgProtoBufHeader::default();
        let encoded = encode_message(EMsg::ClientFriendsList, &header, &msg).unwrap();
        let packets = decode_frame(&encoded).unwrap();

        match decode_friend_requests(&packets[0]) {
            Some(FriendsEvent::FriendRequestsReceived(ids)) => {
                assert_eq!(ids, vec![76561198000000003]);
            }
            _ => panic!("expected FriendRequestsReceived"),
        }
    }

    #[test]
    fn decodes_all_friend_relationships() {
        let mut msg = crate::protobuf::CMsgClientFriendsList {
            bincremental: Some(true),
            ..Default::default()
        };
        msg.friends
            .push(crate::protobuf::c_msg_client_friends_list::Friend {
                ulfriendid: Some(76561198000000004),
                efriendrelationship: Some(0),
            });

        let header = CMsgProtoBufHeader::default();
        let encoded = encode_message(EMsg::ClientFriendsList, &header, &msg).unwrap();
        let packets = decode_frame(&encoded).unwrap();

        match decode_friend_relationships(&packets[0]) {
            Some(FriendsEvent::FriendRelationships {
                incremental,
                entries,
            }) => {
                assert!(incremental);
                assert_eq!(entries, vec![(76561198000000004, 0)]);
            }
            _ => panic!("expected FriendRelationships"),
        }
    }

    #[test]
    fn decodes_add_friend_response() {
        let response = CMsgClientAddFriendResponse {
            eresult: Some(1),
            steam_id_added: Some(76561198000000001),
            persona_name_added: Some("Alice".to_owned()),
        };
        let encoded = encode_message(
            EMsg::ClientAddFriendResponse,
            &CMsgProtoBufHeader::default(),
            &response,
        )
        .unwrap();
        let packets = decode_frame(&encoded).unwrap();

        assert!(matches!(
            decode(&packets[0]),
            Some(FriendsEvent::FriendRequestResponse {
                result: 1,
                steamid: Some(76561198000000001),
                persona_name: Some(ref name),
            }) if name == "Alice"
        ));
    }
}
