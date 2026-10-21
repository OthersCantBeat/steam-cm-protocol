use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::{self, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};
use steam_cm_protocol::{
    AuthEvent, AuthMethod, FriendsEvent, GuardKind, PersonaState, RunCommand, SteamClient,
    web::WebSession,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::{mpsc, oneshot},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file_config = read_env_file(Path::new(".env"));
    let mut client = SteamClient::new();
    if let Some(account_name) = config_value(&file_config, "STEAM_ACCOUNT_NAME")
        && !account_name.trim().is_empty()
    {
        client.set_account_name_hint(account_name);
    }

    let account_name =
        config_value(&file_config, "STEAM_ACCOUNT_NAME").filter(|v| !v.trim().is_empty());
    let password = config_value(&file_config, "STEAM_PASSWORD").filter(|v| !v.trim().is_empty());
    let auth_method = match (
        config_value(&file_config, "STEAM_REFRESH_TOKEN"),
        account_name,
        password,
    ) {
        (Some(token), _, _) if !token.trim().is_empty() => AuthMethod::RefreshToken(token),
        (_, Some(account), Some(password)) => AuthMethod::Credentials { account, password },
        _ => AuthMethod::Qr,
    };
    println!(
        "Auth method: {}",
        match &auth_method {
            AuthMethod::Qr => "QR",
            AuthMethod::Credentials { .. } => "account name + password",
            AuthMethod::RefreshToken(_) => "refresh token",
        }
    );
    let mut auth = client.begin_auth(auth_method).await?;
    let mut stdin_lines = BufReader::new(tokio::io::stdin()).lines();
    let session = loop {
        match auth.recv().await {
            Some(AuthEvent::QrChallenge(url)) => {
                println!("Steam QR login challenge URL (open on a device with Steam):\n{url}");
            }
            Some(AuthEvent::GuardRequired(GuardKind::DeviceConfirmation)) => {
                println!("Steam Guard action required: approve the login in Steam Mobile.");
            }
            Some(AuthEvent::GuardRequired(kind)) => {
                print!("Steam Guard code required ({kind:?}). Enter code: ");
                let _ = io::stdout().flush();
                let code = stdin_lines.next_line().await?.unwrap_or_default();
                client.submit_guard_code(code.trim())?;
            }
            Some(AuthEvent::Success(session)) => break session,
            Some(AuthEvent::Failure(error)) => return Err(error.into()),
            None => return Err("Steam authentication ended without a result".into()),
        }
    };

    save_session(
        Path::new(".env"),
        &session.account_name,
        &session.refresh_token,
    )?;
    println!(
        "Logged in as {} (SteamID64 {}). Session saved to .env.",
        session.account_name, session.steamid
    );

    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();

    if let Some(value) = config_value(&file_config, "STEAM_TEST_FRIEND_STEAMID")
        && let Ok(steamid) = value.trim().parse::<u64>()
    {
        println!("Sending friend request to {steamid}…");
        let _ = command_tx.send(RunCommand::AddFriend(steamid));
    }

    // Pings sent from the terminal, waiting for the recipient's first reply.
    let pending_pings: Arc<Mutex<HashMap<u64, Instant>>> = Arc::default();
    let event_pings = pending_pings.clone();
    let reply_tx = command_tx.clone();
    let welcome_message = config_value(&file_config, "STEAM_WELCOME_MESSAGE")
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "Hi! Ich habe deine Freundschaftsanfrage angenommen. Schreib !ping, um den Chat zu testen.".to_owned());
    let image_path =
        config_value(&file_config, "STEAM_IMAGE_PATH").filter(|v| !v.trim().is_empty());
    let (refresh_token, bot_steamid) = (session.refresh_token.clone(), session.steamid);
    let (event_image_path, event_refresh_token) = (image_path.clone(), refresh_token.clone());
    tokio::spawn(async move {
        // Incoming requests we accepted; the welcome message goes out once Steam confirms.
        let mut accepting: HashSet<u64> = HashSet::new();
        let send_welcome = |steamid: u64| {
            println!("[friend request] {steamid} is now a friend, sending welcome message");
            let _ = reply_tx.send(RunCommand::SendMessage {
                steamid,
                message: welcome_message.clone(),
            });
        };
        while let Some(event) = event_rx.recv().await {
            match event {
                FriendsEvent::FriendRequestsReceived(steamids) => {
                    for steamid in steamids {
                        if accepting.insert(steamid) {
                            println!("\n[friend request] from {steamid}, accepting…");
                            let _ = reply_tx.send(RunCommand::AddFriend(steamid));
                        }
                    }
                    prompt();
                }
                FriendsEvent::IncomingMessage(message) => {
                    println!("\n[message from {}] {}", message.steamid, message.message);
                    if let Some(sent_at) = event_pings.lock().unwrap().remove(&message.steamid) {
                        println!(
                            "[ping] {} answered after {:.1}s, chat works in both directions.",
                            message.steamid,
                            sent_at.elapsed().as_secs_f32()
                        );
                    }
                    if message.message.trim().eq_ignore_ascii_case("!ping") {
                        println!("[ping] replying pong to {}", message.steamid);
                        let _ = reply_tx.send(RunCommand::SendMessage {
                            steamid: message.steamid,
                            message: "pong".to_owned(),
                        });
                    }
                    if message.message.trim().eq_ignore_ascii_case("!bild") {
                        spawn_send_image(
                            event_image_path.clone(),
                            event_refresh_token.clone(),
                            bot_steamid,
                            message.steamid,
                            reply_tx.clone(),
                        );
                    }
                    prompt();
                }
                FriendsEvent::MessageSent(message) => {
                    println!(
                        "\n[message sent to {}] {}",
                        message.steamid, message.message
                    );
                    prompt();
                }
                FriendsEvent::FriendRequestResponse {
                    result,
                    steamid,
                    persona_name,
                } => {
                    println!(
                        "\n[friend request response] result={result}, SteamID64={steamid:?}, name={persona_name:?}"
                    );
                    println!(
                        "Result 1 means Steam accepted the request; the other user may still need to accept it."
                    );
                    if let Some(steamid) = steamid.filter(|id| result == 1 && accepting.remove(id))
                    {
                        send_welcome(steamid);
                    }
                    prompt();
                }
                FriendsEvent::FriendsList(friends) => {
                    println!("\n[friend list updated] {} friends:", friends.len());
                    for friend in friends {
                        println!("  {}", friend.steamid);
                        if accepting.remove(&friend.steamid) {
                            send_welcome(friend.steamid);
                        }
                    }
                    prompt();
                }
                FriendsEvent::RecentMessages { steamid, messages } => {
                    println!(
                        "\n[recent messages with {steamid}] {} messages",
                        messages.len()
                    );
                    for message in messages {
                        println!("  {}: {}", message.steamid, message.message);
                    }
                    print!("> ");
                }
                _ => {}
            }
        }
    });

    println!(
        "Commands: add <SteamID64> | ping <SteamID64> | image <SteamID64> | msg <SteamID64> <text> | history <SteamID64> | status <online|offline|busy|away|snooze|trade|play|invisible> | quit"
    );
    let input_tx = command_tx.clone();
    let input_pings = pending_pings.clone();
    tokio::spawn(async move {
        let mut lines = stdin_lines;
        prompt();
        while let Ok(Some(line)) = lines.next_line().await {
            let mut parts = line.trim().splitn(3, ' ');
            let action = parts.next().unwrap_or_default();
            match action {
                "add" => match parts.next().and_then(|id| id.parse::<u64>().ok()) {
                    Some(steamid) => {
                        let _ = input_tx.send(RunCommand::AddFriend(steamid));
                    }
                    None => println!("Usage: add <SteamID64>"),
                },
                "ping" => match parts.next().and_then(|id| id.parse::<u64>().ok()) {
                    Some(steamid) => {
                        input_pings.lock().unwrap().insert(steamid, Instant::now());
                        let message = "Ping vom Bot: Antworte mit irgendetwas, um den Chat zu testen (oder schreib !ping).".to_owned();
                        let _ = input_tx.send(RunCommand::SendMessage { steamid, message });
                        println!("[ping] sent to {steamid}, waiting for a reply…");
                    }
                    None => println!("Usage: ping <SteamID64>"),
                },
                "image" => match parts.next().and_then(|id| id.parse::<u64>().ok()) {
                    Some(steamid) => spawn_send_image(
                        image_path.clone(),
                        refresh_token.clone(),
                        bot_steamid,
                        steamid,
                        input_tx.clone(),
                    ),
                    None => println!("Usage: image <SteamID64>"),
                },
                "msg" => match (
                    parts.next().and_then(|id| id.parse::<u64>().ok()),
                    parts.next(),
                ) {
                    (Some(steamid), Some(message)) if !message.trim().is_empty() => {
                        let _ = input_tx.send(RunCommand::SendMessage {
                            steamid,
                            message: message.to_owned(),
                        });
                    }
                    _ => println!("Usage: msg <SteamID64> <text>"),
                },
                "history" => match parts.next().and_then(|id| id.parse::<u64>().ok()) {
                    Some(steamid) => {
                        let _ = input_tx.send(RunCommand::GetRecentMessages { steamid });
                    }
                    None => println!("Usage: history <SteamID64>"),
                },
                "status" => match parts.next().unwrap_or_default() {
                    "online" => {
                        let _ = input_tx.send(RunCommand::SetPersonaState(PersonaState::Online));
                    }
                    "offline" => {
                        let _ = input_tx.send(RunCommand::SetPersonaState(PersonaState::Offline));
                    }
                    "busy" => {
                        let _ = input_tx.send(RunCommand::SetPersonaState(PersonaState::Busy));
                    }
                    "away" => {
                        let _ = input_tx.send(RunCommand::SetPersonaState(PersonaState::Away));
                    }
                    "snooze" => {
                        let _ = input_tx.send(RunCommand::SetPersonaState(PersonaState::Snooze));
                    }
                    "trade" => {
                        let _ = input_tx
                            .send(RunCommand::SetPersonaState(PersonaState::LookingToTrade));
                    }
                    "play" => {
                        let _ =
                            input_tx.send(RunCommand::SetPersonaState(PersonaState::LookingToPlay));
                    }
                    "invisible" => {
                        let _ = input_tx.send(RunCommand::SetPersonaState(PersonaState::Invisible));
                    }
                    _ => println!(
                        "Usage: status <online|offline|busy|away|snooze|trade|play|invisible>"
                    ),
                },
                "quit" => {
                    let _ = input_tx.send(RunCommand::Quit);
                    break;
                }
                "" => {}
                _ => println!("Unknown command. Try: add, ping, image, msg, history, status, quit"),
            }
            prompt();
        }
    });

    client.run(command_rx, event_tx).await?;
    Ok(())
}

fn spawn_send_image(
    image_path: Option<String>,
    refresh_token: String,
    bot_steamid: u64,
    steamid: u64,
    commands: mpsc::UnboundedSender<RunCommand>,
) {
    tokio::spawn(async move {
        println!("[image] sending image to {steamid}…");
        match send_image(
            image_path.as_deref(),
            refresh_token,
            bot_steamid,
            steamid,
            &commands,
        )
        .await
        {
            Ok(url) => println!("\n[image] sent to {steamid}: {url}"),
            Err(error) => {
                println!("\n[image] failed for {steamid}: {error}");
                let message = "Das Bild konnte leider nicht gesendet werden.".to_owned();
                let _ = commands.send(RunCommand::SendMessage { steamid, message });
            }
        }
        prompt();
    });
}

async fn send_image(
    image_path: Option<&str>,
    refresh_token: String,
    bot_steamid: u64,
    steamid: u64,
    commands: &mpsc::UnboundedSender<RunCommand>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let image_path = image_path.ok_or("STEAM_IMAGE_PATH is not set")?;
    let image = fs::read(image_path)?;
    let (reply, access_token) = oneshot::channel();
    commands.send(RunCommand::GetWebAccessToken {
        refresh_token,
        reply,
    })?;
    let access_token = access_token.await??;
    let web = WebSession::new(bot_steamid, &access_token)?;
    Ok(web.send_image_to_user(steamid, image, false).await?)
}

fn prompt() {
    print!("> ");
    let _ = io::stdout().flush();
}

fn read_env_file(path: &Path) -> HashMap<String, String> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            Some((
                key.trim().to_owned(),
                value.trim().trim_matches('"').to_owned(),
            ))
        })
        .collect()
}

fn config_value(file_config: &HashMap<String, String>, key: &str) -> Option<String> {
    env::var(key).ok().or_else(|| file_config.get(key).cloned())
}

fn save_session(path: &Path, account_name: &str, refresh_token: &str) -> io::Result<()> {
    let old = fs::read_to_string(path).unwrap_or_default();
    let mut lines: Vec<String> = old
        .lines()
        .filter(|line| {
            let line = line.trim_start();
            !line.starts_with("STEAM_ACCOUNT_NAME=") && !line.starts_with("STEAM_REFRESH_TOKEN=")
        })
        .map(str::to_owned)
        .collect();
    lines.push(format!("STEAM_ACCOUNT_NAME={account_name:?}"));
    lines.push(format!("STEAM_REFRESH_TOKEN={refresh_token:?}"));
    fs::write(path, lines.join("\n") + "\n")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}
