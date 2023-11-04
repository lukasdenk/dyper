use std::collections::HashMap;
use std::io::ErrorKind::ConnectionRefused;
use std::mem;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use dashmap::DashMap;
use futures::future::join_all;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, Mutex};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::sleep;

use crate::algos::ToFromBytes;

const BUF_SIZE: usize = 100_000;

pub struct Peer<const LM2: usize, const LS: usize, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>> {
    sender: Mutex<BufWriter<OwnedWriteHalf>>,
    receiver: Mutex<BufReader<OwnedReadHalf>>,
    receive_task: Mutex<Option<JoinHandle<()>>>,
    send_task: Mutex<Option<JoinHandle<()>>>,
    complete_sending: AtomicBool,
    status_queue_sender: UnboundedSender<(u32, S)>,
    status_queue_receiver: Mutex<UnboundedReceiver<(u32, S)>>,
    combine: Option<fn(M2, M2) -> M2>,
    buf: Option<RwLock<DashMap<u32, M2>>>,
    msg_queue_sender: Option<UnboundedSender<(u32, M2)>>,
    msg_queue_receiver: Option<Mutex<UnboundedReceiver<(u32, M2)>>>,
}

impl<const LM2: usize, const LS: usize, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>> Peer<LM2, LS, M2, S> {
    pub fn new(tcp_stream: TcpStream, combine: Option<fn(M2, M2) -> M2>) -> Self {
        println!("Connected peer to {tcp_stream:?}");
        let (read_half, write_half) = tcp_stream.into_split();
        let (status_queue_sender, status_queue_receiver): (UnboundedSender<(u32, S)>, UnboundedReceiver<(u32, S)>) = mpsc::unbounded_channel();
        let (msg_queue_sender, msg_queue_receiver): (Option<UnboundedSender<(u32, M2)>>, Option<Mutex<UnboundedReceiver<(u32, M2)>>>) = if combine.is_none() {
            let (queue_sender, queue_receiver): (UnboundedSender<(u32, M2)>, UnboundedReceiver<(u32, M2)>) = mpsc::unbounded_channel();
            (Some(queue_sender), Some(Mutex::new(queue_receiver)))
        } else {
            (None, None)
        };
        let buf = if combine.is_some() { Some(RwLock::new(DashMap::with_capacity(BUF_SIZE * 4))) } else { None };

        Self {
            sender: Mutex::new(BufWriter::new(write_half)),
            receiver: Mutex::new(BufReader::new(read_half)),
            receive_task: Mutex::new(None),
            send_task: Mutex::new(None),
            status_queue_sender,
            status_queue_receiver: Mutex::new(status_queue_receiver),
            complete_sending: AtomicBool::new(false),
            combine,
            buf,
            msg_queue_sender,
            msg_queue_receiver,
        }
    }

    pub fn send_msg(&self, replica_id: u32, msg: M2) {
        match self.buf.as_ref() {
            Some(buf) => {
                buf.read().unwrap().entry(replica_id)
                    .and_modify(|val| { *val = (self.combine.as_ref().unwrap())(*val, msg); }).or_insert(msg);
            }
            None => { self.msg_queue_sender.as_ref().unwrap().send((replica_id, msg)).unwrap(); }
        };
    }

    pub fn send_status(&self, replica_id: u32, status: S) {
        self.status_queue_sender.send((replica_id, status)).unwrap();
    }

    pub async fn send_bool(&self, bool_: bool) {
        let byte = if bool_ { 1 } else { 0 };
        let mut writer = self.sender.lock().await;
        writer.write_u8(byte).await.unwrap();
        writer.flush().await.unwrap();
    }

    pub async fn rcv_bool(&self) -> bool {
        let byte = self.receiver.lock().await.read_u8().await.unwrap();
        if byte != 0 { true } else { false }
    }

    pub async fn start_phase2(self: &Arc<Self>, replica_rcv: Arc<impl Fn(u32, M2) + Send + Sync + 'static>) {
        self.complete_sending.store(false, Relaxed);
        let this = self.clone();
        let receiving_task = tokio::spawn(async move { this.start_rcv_msg(replica_rcv).await });
        self.receive_task.lock().await.replace(receiving_task);

        let this = self.clone();
        let sending_task = if self.combine.is_some() {
            tokio::spawn(async move { this.start_sending_msgs_with_combine().await })
        } else {
            tokio::spawn(async move { this.start_sending_queue(&this.msg_queue_receiver.as_ref().unwrap()).await })
        };
        self.send_task.lock().await.replace(sending_task);
    }

    pub async fn start_phase3(self: &Arc<Self>, replica_rcv: Arc<impl Fn(u32, S) + Send + Sync + 'static>) {
        self.complete_sending.store(false, Relaxed);
        let this = self.clone();
        let receiving_task = tokio::spawn(async move { this.start_rcv_status(replica_rcv).await });
        self.receive_task.lock().await.replace(receiving_task);

        let this = self.clone();
        let sending_task = tokio::spawn(async move { this.start_sending_queue(&this.status_queue_receiver).await });
        self.send_task.lock().await.replace(sending_task);
    }

    async fn start_sending_queue<const N: usize, T: ToFromBytes<N>>(&self, send_queue_receiver: &Mutex<UnboundedReceiver<(u32, T)>>) {
        let mut writer = self.sender.lock().await;
        let mut queue_receiver = send_queue_receiver.lock().await;
        loop {
            match queue_receiver.try_recv() {
                Ok((receiver_id, msg)) => {
                    writer.write_u32(receiver_id).await.unwrap();
                    writer.write(&msg.to_bytes()).await.unwrap();
                }
                Err(TryRecvError::Empty) => {
                    if self.complete_sending.load(Relaxed) {
                        writer.write_u32(u32::MAX).await.unwrap();
                        writer.flush().await.unwrap();
                        break;
                    } else {
                        time::sleep(Duration::from_millis(1)).await;
                    }
                }
                Err(TryRecvError::Disconnected) => panic!("Sending queue disconnected")
            }
        }
    }

    pub async fn end_phase(&self) {
        self.complete_sending.store(true, Relaxed);
        let sending_task = self.send_task.lock().await.take().unwrap();
        sending_task.await.unwrap();

        let receiving_task = self.receive_task.lock().await.take().unwrap();
        receiving_task.await.unwrap();
    }

    async fn start_sending_msgs_with_combine(&self) {
        //create this here so that allocation happens only once
        let mut buf: DashMap<u32, M2> = DashMap::with_capacity(self.buf.as_ref().unwrap().read().unwrap().capacity());
        let mut writer = self.sender.lock().await;
        loop {
            if self.buf.as_ref().unwrap().read().unwrap().len() > BUF_SIZE {
                self.send_buf(&mut buf, &mut writer).await;
            } else if self.complete_sending.load(Relaxed) {
                self.send_buf(&mut buf, &mut writer).await;
                //signal superstep completed locally
                writer.write_u32(u32::MAX).await.unwrap();
                writer.flush().await.unwrap();
                return;
            } else {
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
        }
    }

    async fn send_buf(&self, mut buf: &mut DashMap<u32, M2>, writer: &mut BufWriter<OwnedWriteHalf>) {
        mem::swap(&mut *self.buf.as_ref().unwrap().write().unwrap(), &mut buf);
        for entry in buf.iter() {
            // Serialize the key
            writer.write_u32(*entry.key()).await.unwrap();

            // Serialize the value
            let val_bytes = entry.value().to_bytes();
            writer.write(&val_bytes).await.unwrap();
        }
        buf.clear();
    }

    async fn start_rcv_msg(&self, update_replica: Arc<impl Fn(u32, M2)>) {
        let mut reader = self.receiver.lock().await;
        loop {
            let replica_id = reader.read_u32().await.unwrap();
            //superstep complete is encoded as replica_id==u32::MAX
            if replica_id == u32::MAX {
                return;
            }
            let mut buffer = [0u8; LM2];
            reader.read_exact(&mut buffer).await.unwrap();
            let val = M2::from_bytes(buffer);
            (*update_replica)(replica_id, val);
        }
    }

    async fn start_rcv_status(&self, update_replica: Arc<impl Fn(u32, S)>) {
        let mut reader = self.receiver.lock().await;
        loop {
            let replica_id = reader.read_u32().await.unwrap();
            //round complete is encoded as replica_id==u32::MAX
            if replica_id == u32::MAX {
                return;
            }
            let mut buffer = [0u8; LS];
            reader.read_exact(&mut buffer).await.unwrap();
            let val = S::from_bytes(buffer);
            (*update_replica)(replica_id, val);
        }
    }
}

pub async fn create_peers<const LM2: usize, const LS: usize, M2: ToFromBytes<LM2>, S: ToFromBytes<LS>>(node_addresses_filepath: &str, node_id: u8, combine: Option<fn(M2, M2) -> M2>) -> HashMap<u8, Arc<Peer<LM2, LS, M2, S>>> {
    let mut node_id_to_address: HashMap<u8, SocketAddr> = std::fs::read_to_string(node_addresses_filepath).unwrap().lines().enumerate()
        .map(|(id, line)| (id as u8, line.parse::<SocketAddr>().unwrap()))
        .collect();
    if node_id_to_address.len() == 1 {
        return HashMap::new();
    }
    let local_addr = node_id_to_address.remove(&node_id).unwrap();
    let port = local_addr.port();
    let total_peers = node_id_to_address.len();

    println!("bind to port {port:?}");
    let listener = TcpListener::bind(format!("0.0.0.0:{port:?}")).await.unwrap();
    let mut peer_futures = Vec::new();
    for peer_id in 0..node_id {
        let peer_addr = node_id_to_address.remove(&peer_id).unwrap();
        peer_futures.push(tokio::spawn(async move {
            let mut tcp_stream_result = TcpStream::connect(peer_addr).await;
            while let Err(err) = tcp_stream_result {
                if err.kind() == ConnectionRefused {
                    sleep(Duration::from_millis(10)).await;
                    tcp_stream_result = TcpStream::connect(peer_addr).await;
                } else {
                    panic!("{}", err);
                }
            }
            let tcp_stream = tcp_stream_result.unwrap();
            (peer_id, Arc::new(Peer::new(tcp_stream, combine)))
        }
        ));
    }
    let mut peers: HashMap<u8, Arc<Peer<LM2, LS, M2, S>>> = join_all(peer_futures).await.into_iter().map(|result| result.unwrap()).collect();


    let incoming_peers = total_peers - peers.len();
    if incoming_peers != 0 {
        println!("Waiting for {} incoming connections...", incoming_peers);
    }
    for _ in 0..(incoming_peers) {
        let (tcp_stream, _) = listener.accept().await.unwrap();
        println!("Connect to {:?}", tcp_stream.peer_addr().unwrap());
        let (id, _) = node_id_to_address.iter().find(|(_, addr)| addr.ip() == tcp_stream.peer_addr().unwrap().ip()).unwrap();
        let peer = Peer::new(tcp_stream, combine);
        peers.insert(*id, Arc::new(peer));
    }
    peers
}
