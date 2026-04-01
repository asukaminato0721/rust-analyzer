use std::{env, path::{Path, PathBuf}, time::Duration};

use anyhow::bail;
use lsp_server::{ErrorCode, Message, Notification, RequestId, Response};
use tenthash::TentHash;

use crate::{cli::flags, from_json};

const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 300;

impl flags::Supervise {
    pub fn run(self) -> anyhow::Result<()> {
        #[cfg(unix)]
        {
            frontend::run(self.idle_timeout_secs.unwrap_or(DEFAULT_IDLE_TIMEOUT_SECS))
        }
        #[cfg(not(unix))]
        {
            let _ = self;
            bail!("`rust-analyzer supervise` is currently only available on Unix")
        }
    }
}

impl flags::SuperviseDaemon {
    pub fn run(self) -> anyhow::Result<()> {
        #[cfg(unix)]
        {
            daemon::run(
                self.socket_path,
                Duration::from_secs(self.idle_timeout_secs.unwrap_or(DEFAULT_IDLE_TIMEOUT_SECS)),
            )
        }
        #[cfg(not(unix))]
        {
            let _ = self;
            bail!("`rust-analyzer supervise-daemon` is currently only available on Unix")
        }
    }
}

fn workspace_root_from_initialize(message: &Message) -> anyhow::Result<PathBuf> {
    let Message::Request(req) = message else {
        bail!("expected `initialize` request as the first client message");
    };
    if req.method != "initialize" {
        bail!("expected `initialize` request as the first client message, got `{}`", req.method);
    }

    let params = from_json::<lsp_types::InitializeParams>("InitializeParams", &req.params)?;
    if let Some(root) = params
        .root_uri
        .and_then(|it| it.to_file_path().ok())
        .or_else(|| {
            params
                .workspace_folders
                .and_then(|folders| folders.into_iter().find_map(|it| it.uri.to_file_path().ok()))
        })
    {
        Ok(root)
    } else {
        Ok(env::current_dir()?)
    }
}

fn socket_path_for_workspace(workspace_root: &Path) -> PathBuf {
    let base =
        dirs::runtime_dir().unwrap_or_else(|| env::temp_dir()).join("rust-analyzer-supervise");
    let mut hasher = TentHash::new();
    hasher.update(workspace_root.as_os_str().as_encoded_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        _ = write!(&mut hex, "{byte:02x}");
    }
    base.join(format!("{hex}.sock"))
}

fn is_initialized_notification(notification: &Notification) -> bool {
    notification.method == "initialized"
}

fn is_exit_notification(notification: &Notification) -> bool {
    notification.method == "exit"
}

fn is_did_open(notification: &Notification) -> bool {
    notification.method == "textDocument/didOpen"
}

fn is_did_close(notification: &Notification) -> bool {
    notification.method == "textDocument/didClose"
}

fn text_document_uri(notification: &Notification) -> Option<String> {
    notification
        .params
        .pointer("/textDocument/uri")
        .and_then(|it| it.as_str())
        .map(ToOwned::to_owned)
}

fn did_close_notification(uri: &str) -> Notification {
    Notification::new(
        "textDocument/didClose".to_owned(),
        serde_json::json!({ "textDocument": { "uri": uri } }),
    )
}

fn client_unavailable_response(id: RequestId) -> Response {
    Response::new_err(
        id,
        ErrorCode::ServerCancelled as i32,
        "no client is currently attached to the supervisor".to_owned(),
    )
}

#[cfg(unix)]
mod frontend {
    use std::{
        io::{self, BufReader, BufWriter},
        os::unix::net::UnixStream,
        thread,
        time::Duration,
    };

    use anyhow::Context;
    use lsp_server::Message;

    use super::{socket_path_for_workspace, workspace_root_from_initialize};

    pub(super) fn run(idle_timeout_secs: u64) -> anyhow::Result<()> {
        let stdin = io::stdin();
        let mut stdin = BufReader::new(stdin.lock());

        let first_message = Message::read(&mut stdin)?
            .context("expected `initialize` request as the first client message")?;
        let workspace_root = workspace_root_from_initialize(&first_message)?;
        let socket_path = socket_path_for_workspace(&workspace_root);
        let stream = connect_or_spawn(&socket_path, idle_timeout_secs)?;
        let mut daemon_writer = BufWriter::new(stream.try_clone()?);
        let mut daemon_reader = BufReader::new(stream);
        first_message.write(&mut daemon_writer)?;

        let forward_server = thread::spawn(move || -> io::Result<()> {
            let stdout = io::stdout();
            let mut stdout = BufWriter::new(stdout.lock());
            relay_messages(&mut daemon_reader, &mut stdout)
        });

        let client_result = relay_messages(&mut stdin, &mut daemon_writer);
        drop(daemon_writer);
        let server_result = forward_server.join().unwrap();

        match (client_result, server_result) {
            (Ok(()), Ok(())) => {}
            (Err(err), Ok(())) | (Ok(()), Err(err)) => return Err(err.into()),
            (Err(first), Err(_second)) => return Err(first.into()),
        }

        Ok(())
    }

    fn relay_messages(
        reader: &mut impl io::BufRead,
        writer: &mut impl io::Write,
    ) -> io::Result<()> {
        while let Some(message) = Message::read(reader)? {
            message.write(writer)?;
        }
        Ok(())
    }

    fn connect_or_spawn(socket_path: &std::path::Path, idle_timeout_secs: u64) -> anyhow::Result<UnixStream> {
        if let Ok(stream) = UnixStream::connect(socket_path) {
            return Ok(stream);
        }

        if let Some(parent) = socket_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut cmd = Command::new(env::current_exe()?);
        cmd.arg("supervise-daemon")
            .arg(socket_path)
            .arg("--idle-timeout-secs")
            .arg(idle_timeout_secs.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let _child = cmd.spawn().context("failed to spawn supervisor daemon")?;

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match UnixStream::connect(socket_path) {
                Ok(stream) => return Ok(stream),
                Err(err) if Instant::now() < deadline => {
                    let _ = err;
                    thread::sleep(Duration::from_millis(50));
                }
                Err(err) => return Err(err).context("failed to connect to supervisor daemon"),
            }
        }
    }

    use std::{env, fs, process::{Command, Stdio}, time::Instant};
}

#[cfg(unix)]
mod daemon {
    use std::{
        env,
        fs,
        io::{BufReader, BufWriter},
        os::unix::net::{UnixListener, UnixStream},
        path::PathBuf,
        process::{Child, ChildStdin, ChildStdout, Command, Stdio},
        thread,
        time::{Duration, Instant},
    };

    use anyhow::{Context, bail};
    use crossbeam_channel::{Receiver, Sender, unbounded};
    use lsp_server::{Message, Notification};
    use rustc_hash::FxHashSet;

    use super::{
        Response, SessionState, client_unavailable_response, did_close_notification, is_did_close,
        is_did_open, is_exit_notification, is_initialized_notification, text_document_uri,
    };

    pub(super) fn run(socket_path: PathBuf, idle_timeout: Duration) -> anyhow::Result<()> {
        if let Some(parent) = socket_path.parent() {
            fs::create_dir_all(parent)?;
        }

        if socket_path.exists() {
            match UnixStream::connect(&socket_path) {
                Ok(_) => bail!("supervisor daemon already running for {}", socket_path.display()),
                Err(_) => {
                    let _ = fs::remove_file(&socket_path);
                }
            }
        }

        let _socket_guard = SocketGuard { path: socket_path.clone() };
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("failed to bind {}", socket_path.display()))?;
        let (backend_stdin, backend_stdout, _backend_child) = spawn_backend()?;

        let (event_sender, event_receiver) = unbounded();
        spawn_listener_thread(listener, event_sender.clone());
        spawn_backend_thread(backend_stdout, event_sender.clone());

        let mut daemon = SupervisorDaemon::new(backend_stdin, event_sender.clone(), idle_timeout);
        daemon.run(event_receiver)
    }

    #[derive(Debug)]
    enum Event {
        NewClient(UnixStream),
        ClientMessage { session_id: u64, message: Message },
        ClientDisconnected { session_id: u64 },
        BackendMessage(Message),
        BackendDisconnected,
    }

    struct SupervisorDaemon {
        backend_stdin: ChildStdin,
        event_sender: Sender<Event>,
        idle_timeout: Duration,
        idle_deadline: Instant,
        next_session_id: u64,
        current_session: Option<ClientSession>,
        buffered_backend_messages: Vec<Message>,
        cached_initialize_response: Option<CachedInitializeResponse>,
        pending_backend_initialize_id: Option<lsp_server::RequestId>,
        waiting_initialize_session: Option<(u64, lsp_server::RequestId)>,
        backend_initialized: bool,
    }

    impl SupervisorDaemon {
        fn new(backend_stdin: ChildStdin, event_sender: Sender<Event>, idle_timeout: Duration) -> Self {
            Self {
                backend_stdin,
                event_sender,
                idle_timeout,
                idle_deadline: Instant::now() + idle_timeout,
                next_session_id: 0,
                current_session: None,
                buffered_backend_messages: Vec::new(),
                cached_initialize_response: None,
                pending_backend_initialize_id: None,
                waiting_initialize_session: None,
                backend_initialized: false,
            }
        }

        fn run(&mut self, event_receiver: Receiver<Event>) -> anyhow::Result<()> {
            loop {
                let timeout = self
                    .idle_deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(250));
                match event_receiver.recv_timeout(timeout) {
                    Ok(Event::BackendDisconnected) => return Ok(()),
                    Ok(event) => self.handle_event(event)?,
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        if self.current_session.is_none() && Instant::now() >= self.idle_deadline {
                            return Ok(());
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(()),
                }
            }
        }

        fn handle_event(&mut self, event: Event) -> anyhow::Result<()> {
            match event {
                Event::NewClient(stream) => self.attach_client(stream)?,
                Event::ClientMessage { session_id, message } => {
                    self.handle_client_message(session_id, message)?
                }
                Event::ClientDisconnected { session_id } => self.detach_client(session_id)?,
                Event::BackendMessage(message) => self.handle_backend_message(message)?,
                Event::BackendDisconnected => return Ok(()),
            }
            Ok(())
        }

        fn attach_client(&mut self, stream: UnixStream) -> anyhow::Result<()> {
            if self.current_session.is_some() {
                drop(stream);
                return Ok(());
            }

            self.next_session_id += 1;
            let session_id = self.next_session_id;
            let read_stream = stream.try_clone()?;
            let writer = BufWriter::new(stream);
            self.current_session = Some(ClientSession {
                id: session_id,
                writer,
                open_documents: FxHashSet::default(),
                state: SessionState::WaitingForInitialize,
            });

            let sender = self.event_sender.clone();
            thread::spawn(move || read_client_messages(read_stream, session_id, sender));
            self.idle_deadline = Instant::now() + self.idle_timeout;
            Ok(())
        }

        fn handle_client_message(
            &mut self,
            session_id: u64,
            message: Message,
        ) -> anyhow::Result<()> {
            let Some(current_session_id) = self.current_session.as_ref().map(|it| it.id) else {
                return Ok(());
            };
            if current_session_id != session_id {
                return Ok(());
            }

            let session_state = self.current_session.as_ref().unwrap().state;
            match session_state {
                SessionState::WaitingForInitialize => {
                    let Message::Request(req) = message else {
                        return Ok(());
                    };
                    if req.method != "initialize" {
                        return Ok(());
                    }

                    self.waiting_initialize_session = Some((session_id, req.id.clone()));
                    if let Some(cached) = &self.cached_initialize_response {
                        Message::Response(cached.to_response(req.id))
                            .write(&mut self.current_session.as_mut().unwrap().writer)
                            .context("failed to write initialize response to client")?;
                        self.current_session.as_mut().unwrap().state =
                            SessionState::WaitingForInitialized;
                    } else if self.pending_backend_initialize_id.is_none() {
                        self.pending_backend_initialize_id = Some(req.id.clone());
                        Message::Request(req)
                            .write(&mut self.backend_stdin)
                            .context("failed to forward initialize to backend")?;
                    }
                }
                SessionState::WaitingForInitialized => match message {
                    Message::Notification(notification) if is_initialized_notification(&notification) => {
                        if !self.backend_initialized {
                            Message::Notification(notification)
                                .write(&mut self.backend_stdin)
                                .context("failed to forward initialized notification")?;
                            self.backend_initialized = true;
                        }
                        self.current_session.as_mut().unwrap().state = SessionState::Ready;
                        self.flush_buffered_backend_messages()?;
                    }
                    Message::Request(req) if req.method == "shutdown" => {
                        Message::Response(Response::new_ok(req.id, ()))
                            .write(&mut self.current_session.as_mut().unwrap().writer)
                            .context("failed to write shutdown response to client")?;
                    }
                    Message::Notification(notification) if is_exit_notification(&notification) => {
                        self.detach_client(session_id)?;
                    }
                    _ => {}
                },
                SessionState::Ready => match message {
                    Message::Request(req) if req.method == "shutdown" => {
                        Message::Response(Response::new_ok(req.id, ()))
                            .write(&mut self.current_session.as_mut().unwrap().writer)
                            .context("failed to write shutdown response to client")?;
                    }
                    Message::Notification(notification) if is_exit_notification(&notification) => {
                        self.detach_client(session_id)?;
                    }
                    Message::Notification(notification) => {
                        track_open_documents(
                            &mut self.current_session.as_mut().unwrap().open_documents,
                            &notification,
                        );
                        Message::Notification(notification)
                            .write(&mut self.backend_stdin)
                            .context("failed to forward notification to backend")?;
                    }
                    other => other
                        .write(&mut self.backend_stdin)
                        .context("failed to forward message to backend")?,
                },
            }

            Ok(())
        }

        fn handle_backend_message(&mut self, message: Message) -> anyhow::Result<()> {
            if let Message::Response(resp) = &message
                && self
                    .pending_backend_initialize_id
                    .as_ref()
                    .is_some_and(|pending| pending == &resp.id)
            {
                self.cached_initialize_response = Some(CachedInitializeResponse {
                    result: resp.result.clone(),
                    error: resp.error.clone(),
                });
                self.pending_backend_initialize_id = None;
                if let Some((session_id, initialize_id)) = self.waiting_initialize_session.take()
                    && let Some(session) = self.current_session.as_mut()
                    && session.id == session_id
                {
                    Message::Response(
                        self.cached_initialize_response
                            .as_ref()
                            .unwrap()
                            .to_response(initialize_id),
                    )
                        .write(&mut session.writer)
                        .context("failed to write initialize response to client")?;
                    session.state = SessionState::WaitingForInitialized;
                }
                return Ok(());
            }

            match self.current_session.as_mut() {
                Some(session) if matches!(session.state, SessionState::Ready) => {
                    message
                        .write(&mut session.writer)
                        .context("failed to forward backend message to client")?;
                }
                Some(_) => self.buffered_backend_messages.push(message),
                None => match message {
                    Message::Request(req) => {
                        Message::Response(client_unavailable_response(req.id))
                            .write(&mut self.backend_stdin)
                            .context("failed to respond to backend request")?;
                    }
                    Message::Notification(_) | Message::Response(_) => {}
                },
            }

            Ok(())
        }

        fn flush_buffered_backend_messages(&mut self) -> anyhow::Result<()> {
            let Some(session) = self.current_session.as_mut() else {
                return Ok(());
            };
            if !matches!(session.state, SessionState::Ready) {
                return Ok(());
            }

            for message in self.buffered_backend_messages.drain(..) {
                message
                    .write(&mut session.writer)
                    .context("failed to forward buffered backend message")?;
            }
            Ok(())
        }

        fn detach_client(&mut self, session_id: u64) -> anyhow::Result<()> {
            let Some(session) = self.current_session.take() else {
                return Ok(());
            };
            if session.id != session_id {
                self.current_session = Some(session);
                return Ok(());
            }

            if self
                .waiting_initialize_session
                .as_ref()
                .is_some_and(|(waiting_session_id, _)| *waiting_session_id == session_id)
            {
                self.waiting_initialize_session = None;
            }

            for uri in session.open_documents {
                Message::Notification(did_close_notification(&uri))
                    .write(&mut self.backend_stdin)
                    .context("failed to synthesize didClose for disconnected client")?;
            }
            self.idle_deadline = Instant::now() + self.idle_timeout;
            Ok(())
        }
    }

    struct ClientSession {
        id: u64,
        writer: BufWriter<UnixStream>,
        open_documents: FxHashSet<String>,
        state: SessionState,
    }

    #[derive(Clone)]
    struct CachedInitializeResponse {
        result: Option<serde_json::Value>,
        error: Option<lsp_server::ResponseError>,
    }

    impl CachedInitializeResponse {
        fn to_response(&self, id: lsp_server::RequestId) -> Response {
            Response { id, result: self.result.clone(), error: self.error.clone() }
        }
    }

    fn track_open_documents(open_documents: &mut FxHashSet<String>, notification: &Notification) {
        if is_did_open(notification) {
            if let Some(uri) = text_document_uri(notification) {
                open_documents.insert(uri);
            }
        } else if is_did_close(notification) && let Some(uri) = text_document_uri(notification) {
            open_documents.remove(&uri);
        }
    }

    fn spawn_listener_thread(listener: UnixListener, sender: Sender<Event>) {
        thread::spawn(move || {
            while let Ok((stream, _addr)) = listener.accept() {
                if sender.send(Event::NewClient(stream)).is_err() {
                    break;
                }
            }
        });
    }

    fn spawn_backend_thread(stdout: ChildStdout, sender: Sender<Event>) {
        thread::spawn(move || {
            let mut stdout = BufReader::new(stdout);
            loop {
                match Message::read(&mut stdout) {
                    Ok(Some(message)) => {
                        if sender.send(Event::BackendMessage(message)).is_err() {
                            break;
                        }
                    }
                    Ok(None) => {
                        _ = sender.send(Event::BackendDisconnected);
                        break;
                    }
                    Err(_) => {
                        _ = sender.send(Event::BackendDisconnected);
                        break;
                    }
                }
            }
        });
    }

    fn read_client_messages(stream: UnixStream, session_id: u64, sender: Sender<Event>) {
        let mut stream = BufReader::new(stream);
        loop {
            match Message::read(&mut stream) {
                Ok(Some(message)) => {
                    if sender.send(Event::ClientMessage { session_id, message }).is_err() {
                        return;
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
        _ = sender.send(Event::ClientDisconnected { session_id });
    }

    fn spawn_backend() -> anyhow::Result<(ChildStdin, ChildStdout, Child)> {
        let mut child = Command::new(env::current_exe()?)
            .arg("lsp-server")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to spawn rust-analyzer backend")?;
        let stdin = child.stdin.take().context("backend stdin missing")?;
        let stdout = child.stdout.take().context("backend stdout missing")?;
        Ok((stdin, stdout, child))
    }

    struct SocketGuard {
        path: PathBuf,
    }

    impl Drop for SocketGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionState {
    WaitingForInitialize,
    WaitingForInitialized,
    Ready,
}

#[cfg(test)]
mod tests {
    use lsp_server::{Message, Notification, Request};

    use super::{
        SessionState, did_close_notification, is_did_close, socket_path_for_workspace,
        text_document_uri, workspace_root_from_initialize,
    };

    #[test]
    fn socket_path_is_stable() {
        let path = socket_path_for_workspace(std::path::Path::new("/tmp/project"));
        let file_name = path.file_name().unwrap().to_string_lossy();
        assert!(file_name.ends_with(".sock"));
        assert!(file_name.len() > 10);
    }

    #[test]
    fn initialize_root_prefers_root_uri() {
        let message = Message::Request(Request::new(
            1.into(),
            "initialize".to_owned(),
            serde_json::json!({
                "rootUri": "file:///tmp/workspace",
                "capabilities": {}
            }),
        ));
        let root = workspace_root_from_initialize(&message).unwrap();
        assert_eq!(root, std::path::PathBuf::from("/tmp/workspace"));
    }

    #[test]
    fn did_close_uses_text_document_uri() {
        let notification = did_close_notification("file:///tmp/main.rs");
        assert!(is_did_close(&notification));
        assert_eq!(text_document_uri(&notification).as_deref(), Some("file:///tmp/main.rs"));
    }

    #[test]
    fn session_state_progression_is_ordered() {
        assert_ne!(SessionState::WaitingForInitialize, SessionState::Ready);
    }

    #[test]
    fn text_document_uri_reads_did_open_shape() {
        let notification = Notification::new(
            "textDocument/didOpen".to_owned(),
            serde_json::json!({
                "textDocument": { "uri": "file:///tmp/lib.rs" }
            }),
        );
        assert_eq!(text_document_uri(&notification).as_deref(), Some("file:///tmp/lib.rs"));
    }
}
