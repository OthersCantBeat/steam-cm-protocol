//! CS2 Game Coordinator: resolves a match (from its share code) to the demo URL.
//!
//! The GC only talks to clients that play the game, so the first request starts
//! "playing" CS2 and says hello until the GC welcomes us. After [`IDLE_TIMEOUT`]
//! without requests the session closes again and the account stops showing as in-game.

use std::sync::Arc;

use prost::Message;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    time::{Duration, Instant, timeout, timeout_at},
};

use crate::{
    connection::Connection,
    emsg::{EMsg, PROTO_MASK},
    error::{Error, Result},
    message::Packet,
    protobuf::{
        CDataGccStrike15V2MatchInfo, CMsgClientGamesPlayed, CMsgGcClient, CMsgGcClientHello,
        CMsgGcConnectionStatus, CMsgGccStrike15V2MatchList,
        CMsgGccStrike15V2MatchListRequestFullGameInfo, CMsgProtoBufHeader,
        c_msg_client_games_played::GamePlayed,
    },
};

pub const CS2_APP_ID: u32 = 730;

const MSG_CLIENT_WELCOME: u32 = 4004;
const MSG_CLIENT_HELLO: u32 = 4006;
const MSG_CONNECTION_STATUS: u32 = 4009;
const MSG_MATCH_LIST: u32 = 9139;
const MSG_MATCH_LIST_REQUEST_FULL_GAME_INFO: u32 = 9147;

/// GC client version in the hello; the GC doesn't enforce it for match requests.
const HELLO_VERSION: u32 = 2000244;
const HELLO_INTERVAL: Duration = Duration::from_secs(5);
const WELCOME_TIMEOUT: Duration = Duration::from_secs(30);
const MATCH_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

/// The parts of a match share code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cs2MatchRequest {
    pub match_id: u64,
    pub outcome_id: u64,
    pub token: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cs2Match {
    pub match_id: u64,
    /// Unix seconds.
    pub match_time: Option<u32>,
    /// None while Valve has no demo: not ready yet or already deleted.
    pub demo_url: Option<String>,
}

#[derive(Debug)]
pub(crate) struct Job {
    pub request: Cs2MatchRequest,
    pub reply: oneshot::Sender<Result<Cs2Match>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GcMessage {
    msg_type: u32,
    body: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum GcOutgoing {
    Playing(bool),
    Send(GcMessage),
}

/// GC payloads with a protobuf header start like CM frames: type with the proto
/// bit, header length, header. An empty header is enough for requests.
fn encode_payload(msg_type: u32, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(8 + body.len());
    payload.extend_from_slice(&(msg_type | PROTO_MASK).to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(body);
    payload
}

/// Protobuf GC messages of CS2; everything else is none of our business.
pub(crate) fn decode_from_gc(packet: &Packet) -> Option<GcMessage> {
    if packet.emsg != EMsg::ClientFromGC.raw() {
        return None;
    }
    let message = packet.decode_body::<CMsgGcClient>().ok()?;
    if message.appid != Some(CS2_APP_ID) {
        return None;
    }
    let msg_type = message.msgtype?;
    if msg_type & PROTO_MASK == 0 {
        return None;
    }
    let payload = message.payload?;
    let header_len = u32::from_le_bytes(payload.get(4..8)?.try_into().ok()?) as usize;
    let body = payload.get(8usize.checked_add(header_len)?..)?.to_vec();
    Some(GcMessage {
        msg_type: msg_type & !PROTO_MASK,
        body,
    })
}

/// The URL is in the last round that has one; the earlier rounds hold no URL.
fn demo_url(info: &CDataGccStrike15V2MatchInfo) -> Option<String> {
    info.roundstatsall
        .iter()
        .rev()
        .chain(info.roundstats_legacy.iter())
        .filter_map(|round| round.map.as_deref())
        .find(|map| map.starts_with("http"))
        .map(str::to_owned)
}

fn match_from_list(request: &Cs2MatchRequest, body: &[u8]) -> Option<Cs2Match> {
    let list = CMsgGccStrike15V2MatchList::decode(body).ok()?;
    if list.matches.is_empty() {
        // The GC's answer for a match it no longer has.
        return Some(Cs2Match {
            match_id: request.match_id,
            match_time: None,
            demo_url: None,
        });
    }
    let info = list
        .matches
        .iter()
        .find(|info| info.matchid == Some(request.match_id))?;
    Some(Cs2Match {
        match_id: request.match_id,
        match_time: info.matchtime,
        demo_url: demo_url(info),
    })
}

fn has_session(message: &GcMessage) -> bool {
    CMsgGcConnectionStatus::decode(message.body.as_slice())
        .is_ok_and(|status| status.status.unwrap_or_default() == 0)
}

struct Worker {
    incoming: mpsc::UnboundedReceiver<GcMessage>,
    out: mpsc::UnboundedSender<GcOutgoing>,
    playing: bool,
    welcomed: bool,
}

impl Worker {
    fn send(&self, msg_type: u32, body: impl Message) {
        let _ = self.out.send(GcOutgoing::Send(GcMessage {
            msg_type,
            body: body.encode_to_vec(),
        }));
    }

    fn set_playing(&mut self, playing: bool) {
        if self.playing != playing {
            self.playing = playing;
            let _ = self.out.send(GcOutgoing::Playing(playing));
        }
        if !playing {
            self.welcomed = false;
        }
    }

    /// Messages arrived while no request was running: a late welcome or a lost session.
    fn drain(&mut self) {
        while let Ok(message) = self.incoming.try_recv() {
            self.track_session(&message);
        }
    }

    fn track_session(&mut self, message: &GcMessage) {
        match message.msg_type {
            MSG_CLIENT_WELCOME => self.welcomed = true,
            MSG_CONNECTION_STATUS => self.welcomed = has_session(message),
            _ => {}
        }
    }

    async fn ensure_session(&mut self) -> Result<()> {
        self.set_playing(true);
        let deadline = Instant::now() + WELCOME_TIMEOUT;
        while !self.welcomed {
            if Instant::now() >= deadline {
                return Err(Error::Transport(
                    "no welcome from the CS2 Game Coordinator (does the account own CS2?)"
                        .to_owned(),
                ));
            }
            self.send(
                MSG_CLIENT_HELLO,
                CMsgGcClientHello {
                    version: Some(HELLO_VERSION),
                    client_session_need: Some(0),
                    client_launcher: Some(0),
                    steam_launcher: Some(0),
                },
            );
            let next_hello = (Instant::now() + HELLO_INTERVAL).min(deadline);
            while !self.welcomed {
                match timeout_at(next_hello, self.incoming.recv()).await {
                    Ok(Some(message)) => self.track_session(&message),
                    Ok(None) => return Err(session_ended()),
                    Err(_) => break,
                }
            }
        }
        Ok(())
    }

    async fn request_match(&mut self, request: &Cs2MatchRequest) -> Result<Cs2Match> {
        self.drain();
        self.ensure_session().await?;
        self.send(
            MSG_MATCH_LIST_REQUEST_FULL_GAME_INFO,
            CMsgGccStrike15V2MatchListRequestFullGameInfo {
                matchid: Some(request.match_id),
                outcomeid: Some(request.outcome_id),
                token: Some(request.token),
            },
        );
        let deadline = Instant::now() + MATCH_TIMEOUT;
        loop {
            match timeout_at(deadline, self.incoming.recv()).await {
                Ok(Some(message)) => {
                    self.track_session(&message);
                    if message.msg_type == MSG_MATCH_LIST
                        && let Some(found) = match_from_list(request, &message.body)
                    {
                        return Ok(found);
                    }
                    if !self.welcomed {
                        return Err(Error::Transport(
                            "lost the CS2 Game Coordinator session".to_owned(),
                        ));
                    }
                }
                Ok(None) => return Err(session_ended()),
                Err(_) => {
                    // Say hello again next time, in case the GC silently forgot us.
                    self.welcomed = false;
                    return Err(Error::Transport(
                        "no answer from the CS2 Game Coordinator".to_owned(),
                    ));
                }
            }
        }
    }
}

fn session_ended() -> Error {
    Error::Transport("steam session ended".to_owned())
}

/// Handles requests one at a time until the session ends. The GC answers without
/// a request id, so overlapping requests couldn't be told apart reliably.
pub(crate) async fn run_worker(
    mut jobs: mpsc::UnboundedReceiver<Job>,
    incoming: mpsc::UnboundedReceiver<GcMessage>,
    out: mpsc::UnboundedSender<GcOutgoing>,
) {
    let mut worker = Worker {
        incoming,
        out,
        playing: false,
        welcomed: false,
    };
    loop {
        let job = if worker.playing {
            match timeout(IDLE_TIMEOUT, jobs.recv()).await {
                Ok(job) => job,
                Err(_) => {
                    worker.set_playing(false);
                    continue;
                }
            }
        } else {
            jobs.recv().await
        };
        let Some(job) = job else { return };
        let result = worker.request_match(&job.request).await;
        let _ = job.reply.send(result);
    }
}

/// Puts the worker's messages on the wire.
pub(crate) async fn forward_outgoing(
    connection: Arc<Mutex<Connection>>,
    mut out: mpsc::UnboundedReceiver<GcOutgoing>,
) {
    while let Some(message) = out.recv().await {
        let conn = connection.lock().await;
        let state = conn.state_snapshot().await;
        let mut header = CMsgProtoBufHeader {
            steamid: state.steamid,
            client_sessionid: state.client_session_id,
            ..Default::default()
        };
        let result = match message {
            GcOutgoing::Playing(playing) => {
                let games_played = if playing {
                    vec![GamePlayed {
                        game_id: Some(CS2_APP_ID.into()),
                        ..Default::default()
                    }]
                } else {
                    vec![]
                };
                conn.send_message(
                    EMsg::ClientGamesPlayed,
                    &header,
                    &CMsgClientGamesPlayed {
                        games_played,
                        ..Default::default()
                    },
                )
                .await
            }
            GcOutgoing::Send(message) => {
                header.routing_appid = Some(CS2_APP_ID);
                conn.send_message(
                    EMsg::ClientToGC,
                    &header,
                    &CMsgGcClient {
                        appid: Some(CS2_APP_ID),
                        msgtype: Some(message.msg_type | PROTO_MASK),
                        payload: Some(encode_payload(message.msg_type, &message.body)),
                        ..Default::default()
                    },
                )
                .await
            }
        };
        if let Err(e) = result {
            tracing::warn!("sending to the CS2 Game Coordinator failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::CMsgGccStrike15V2MatchmakingServerRoundStats as RoundStats;

    const REQUEST: Cs2MatchRequest = Cs2MatchRequest {
        match_id: 3230642215713767580,
        outcome_id: 3230647599455273103,
        token: 55788,
    };

    struct Harness {
        jobs: mpsc::UnboundedSender<Job>,
        gc: mpsc::UnboundedSender<GcMessage>,
        out: mpsc::UnboundedReceiver<GcOutgoing>,
    }

    fn start() -> Harness {
        let (jobs, jobs_rx) = mpsc::unbounded_channel();
        let (gc, gc_rx) = mpsc::unbounded_channel();
        let (out_tx, out) = mpsc::unbounded_channel();
        tokio::spawn(run_worker(jobs_rx, gc_rx, out_tx));
        Harness { jobs, gc, out }
    }

    impl Harness {
        fn request(&self) -> oneshot::Receiver<Result<Cs2Match>> {
            let (reply, rx) = oneshot::channel();
            self.jobs
                .send(Job {
                    request: REQUEST,
                    reply,
                })
                .unwrap();
            rx
        }

        fn gc(&self, msg_type: u32, body: impl Message) {
            self.gc
                .send(GcMessage {
                    msg_type,
                    body: body.encode_to_vec(),
                })
                .unwrap();
        }

        fn welcome(&self) {
            self.gc(MSG_CLIENT_WELCOME, CMsgGcClientHello::default());
        }

        async fn next_sent(&mut self) -> GcMessage {
            match self.out.recv().await.unwrap() {
                GcOutgoing::Send(message) => message,
                other => panic!("expected a GC message, got {other:?}"),
            }
        }

        async fn open_session(&mut self) {
            assert_eq!(self.out.recv().await.unwrap(), GcOutgoing::Playing(true));
            assert_eq!(self.next_sent().await.msg_type, MSG_CLIENT_HELLO);
            self.welcome();
        }
    }

    fn match_list(url: Option<&str>) -> CMsgGccStrike15V2MatchList {
        CMsgGccStrike15V2MatchList {
            matches: vec![CDataGccStrike15V2MatchInfo {
                matchid: Some(REQUEST.match_id),
                matchtime: Some(1_790_000_000),
                roundstatsall: vec![
                    RoundStats {
                        round: Some(1),
                        ..Default::default()
                    },
                    RoundStats {
                        round: Some(24),
                        map: url.map(str::to_owned),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn resolves_the_demo_url_after_the_welcome() {
        let mut h = start();
        let answer = h.request();
        h.open_session().await;

        let sent = h.next_sent().await;
        assert_eq!(sent.msg_type, MSG_MATCH_LIST_REQUEST_FULL_GAME_INFO);
        let body =
            CMsgGccStrike15V2MatchListRequestFullGameInfo::decode(sent.body.as_slice()).unwrap();
        assert_eq!(body.matchid, Some(REQUEST.match_id));
        assert_eq!(body.outcomeid, Some(REQUEST.outcome_id));
        assert_eq!(body.token, Some(REQUEST.token));

        h.gc(
            MSG_MATCH_LIST,
            match_list(Some("http://replay1.valve.net/730/x.dem.bz2")),
        );
        let found = answer.await.unwrap().unwrap();
        assert_eq!(
            found.demo_url.as_deref(),
            Some("http://replay1.valve.net/730/x.dem.bz2")
        );
        assert_eq!(found.match_time, Some(1_790_000_000));
    }

    #[tokio::test(start_paused = true)]
    async fn says_hello_again_until_welcomed_and_gives_up() {
        let mut h = start();
        let answer = h.request();
        assert_eq!(h.out.recv().await.unwrap(), GcOutgoing::Playing(true));
        for _ in 0..6 {
            assert_eq!(h.next_sent().await.msg_type, MSG_CLIENT_HELLO);
        }
        let err = answer.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("own CS2"), "{err}");
    }

    #[tokio::test(start_paused = true)]
    async fn empty_list_means_no_demo_and_no_url_stays_none() {
        let mut h = start();
        let answer = h.request();
        h.open_session().await;
        h.next_sent().await;
        h.gc(MSG_MATCH_LIST, CMsgGccStrike15V2MatchList::default());
        assert_eq!(answer.await.unwrap().unwrap().demo_url, None);

        let answer = h.request();
        h.next_sent().await;
        h.gc(MSG_MATCH_LIST, match_list(Some("de_inferno")));
        assert_eq!(answer.await.unwrap().unwrap().demo_url, None);
    }

    #[tokio::test(start_paused = true)]
    async fn ignores_lists_of_other_matches_and_times_out() {
        let mut h = start();
        let answer = h.request();
        h.open_session().await;
        h.next_sent().await;
        let mut other = match_list(Some("http://replay1.valve.net/730/other.dem.bz2"));
        other.matches[0].matchid = Some(1);
        h.gc(MSG_MATCH_LIST, other);
        let err = answer.await.unwrap().unwrap_err();
        assert!(err.to_string().contains("no answer"), "{err}");

        // Timed out: the next request says hello first.
        let _answer = h.request();
        assert_eq!(h.next_sent().await.msg_type, MSG_CLIENT_HELLO);
    }

    #[tokio::test(start_paused = true)]
    async fn stops_playing_when_idle_and_reopens() {
        let mut h = start();
        let answer = h.request();
        h.open_session().await;
        h.next_sent().await;
        h.gc(MSG_MATCH_LIST, match_list(None));
        answer.await.unwrap().unwrap();

        assert_eq!(h.out.recv().await.unwrap(), GcOutgoing::Playing(false));
        let _answer = h.request();
        assert_eq!(h.out.recv().await.unwrap(), GcOutgoing::Playing(true));
        assert_eq!(h.next_sent().await.msg_type, MSG_CLIENT_HELLO);
    }

    #[tokio::test(start_paused = true)]
    async fn lost_session_fails_the_request_and_needs_a_new_hello() {
        let mut h = start();
        let answer = h.request();
        h.open_session().await;
        h.next_sent().await;
        h.gc(
            MSG_CONNECTION_STATUS,
            CMsgGcConnectionStatus { status: Some(2) },
        );
        assert!(answer.await.unwrap().is_err());
        let _answer = h.request();
        assert_eq!(h.next_sent().await.msg_type, MSG_CLIENT_HELLO);
    }

    #[test]
    fn payload_round_trips_through_cm_framing() {
        let payload = encode_payload(MSG_MATCH_LIST, &[1, 2, 3]);
        assert_eq!(
            &payload[..8],
            &[0xB3, 0x23, 0, 0x80, 0, 0, 0, 0],
            "9139 with proto bit, empty header"
        );
        let packet = Packet {
            emsg: EMsg::ClientFromGC.raw(),
            header: CMsgProtoBufHeader::default(),
            body: CMsgGcClient {
                appid: Some(CS2_APP_ID),
                msgtype: Some(MSG_MATCH_LIST | PROTO_MASK),
                payload: Some(payload),
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        };
        assert_eq!(
            decode_from_gc(&packet),
            Some(GcMessage {
                msg_type: MSG_MATCH_LIST,
                body: vec![1, 2, 3]
            })
        );
    }

    #[test]
    fn decode_skips_other_apps_and_broken_payloads() {
        let packet = |appid, payload: Vec<u8>| Packet {
            emsg: EMsg::ClientFromGC.raw(),
            header: CMsgProtoBufHeader::default(),
            body: CMsgGcClient {
                appid: Some(appid),
                msgtype: Some(MSG_MATCH_LIST | PROTO_MASK),
                payload: Some(payload),
                ..Default::default()
            }
            .encode_to_vec()
            .into(),
        };
        assert_eq!(decode_from_gc(&packet(440, encode_payload(1, &[]))), None);
        assert_eq!(decode_from_gc(&packet(CS2_APP_ID, vec![1, 2])), None);
        let too_long_header = [0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(
            decode_from_gc(&packet(CS2_APP_ID, too_long_header.to_vec())),
            None
        );
    }
}
