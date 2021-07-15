use futures::future::try_join_all;
use redis::{aio::MultiplexedConnection, Client, IntoConnectionInfo};
use sha1::{Digest, Sha1};
use std::ops::Deref;
use std::time::{Duration, Instant};
use tokio::runtime;

// const ITERATIONS: usize = 10;
// const DATA_LIMIT: usize = 4 << 30;

/// Untill zdb flush command properly removes ALL data
const ITERATIONS: usize = 1;
const DATA_LIMIT: usize = 1 << 30;

fn main() {
    let data_sizes = [4 << 10, 2 << 20];
    let namespaces = [1, 5, 10];
    let host_threads = [1, 2, 4];

    println!("RESULTS:");
    println!("thread count | concurrent namespaces | data size | longest task | shortest task");
    println!("---|---|---|---|---");
    for thread_count in host_threads {
        for ns_count in namespaces {
            for data_size in data_sizes {
                let mut results = Vec::with_capacity(ITERATIONS);
                for iteration_idx in 0..ITERATIONS {
                    // some garbage data of the right size, leak to make static for now
                    let data = vec![1; data_size];

                    let rt = runtime::Builder::new_multi_thread()
                        .enable_time()
                        .enable_io()
                        .worker_threads(thread_count)
                        .build()
                        .unwrap();

                    let stats: Vec<TaskStats> = rt.block_on(async {
                        // each namespace has a dedicated connection
                        let mut namespaces = Vec::with_capacity(ns_count);
                        for ns_idx in 0..ns_count {
                            namespaces.push(
                                EphemeralNamespace::new(Namespace::new(
                                    Con::new("redis://localhost:9900").await.unwrap(),
                                    format!("blaze-{}-{}", iteration_idx, ns_idx),
                                ))
                                .await
                                .unwrap(),
                            );
                        }
                        let mut handles = Vec::with_capacity(namespaces.len());
                        for ns in namespaces {
                            let data = data.clone();
                            let count = DATA_LIMIT / data_size;
                            handles.push(tokio::task::spawn(async move {
                                let mut stats = TaskStats::new();
                                let start = Instant::now();

                                for _ in 0..(count) {
                                    ns.set(None, &data).await.unwrap();
                                    stats.bytes_written += data.len();
                                    stats.io_operations += 1;
                                }

                                stats.time = Instant::now().duration_since(start);

                                stats
                            }));
                        }
                        try_join_all(handles).await.unwrap().into_iter().collect()
                        // exit reactor
                    });

                    results.push(stats);
                }

                // print results for config
                let longest_run_ms = results
                    .iter()
                    .flatten()
                    .map(|stats: &TaskStats| stats.time.as_millis())
                    .max()
                    .unwrap();
                let shortest_run_ms = results
                    .iter()
                    .flatten()
                    .map(|stats: &TaskStats| stats.time.as_millis())
                    .min()
                    .unwrap();
                println!(
                    "{} | {} | {} | {} ms| {} ms",
                    thread_count, ns_count, data_size, longest_run_ms, shortest_run_ms
                );
            }
        }
    }
}

#[derive(Debug)]
struct TaskStats {
    bytes_written: usize,
    io_operations: usize,
    time: Duration,
}

impl TaskStats {
    pub fn new() -> Self {
        Self {
            bytes_written: 0,
            io_operations: 0,
            time: Duration::default(),
        }
    }
}

impl Default for TaskStats {
    fn default() -> Self {
        Self::new()
    }
}

struct EphemeralNamespace {
    ns: Namespace,
}

impl EphemeralNamespace {
    pub async fn new(ns: Namespace) -> Result<EphemeralNamespace, redis::RedisError> {
        ns.create().await?;
        //println!("create");
        ns.set_password().await?;
        //println!("password");
        ns.set_private().await?;
        //println!("private");
        ns.open_private().await?;
        //println!("open private");
        Ok(EphemeralNamespace { ns })
    }

    async fn set(&self, key: Option<&[u8]>, data: &[u8]) -> Result<Vec<u8>, redis::RedisError> {
        self.ns.set(key, data).await
    }
}

impl Drop for EphemeralNamespace {
    fn drop(&mut self) {
        // eprintln!("dropping namespace {}", self.ns.name);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { self.ns.delete_private().await })
        })
        .unwrap();
    }
}

struct Namespace {
    con: Con,
    name: String,
}

impl Namespace {
    pub fn new(con: Con, name: String) -> Namespace {
        Namespace { con, name }
    }

    pub async fn create(&self) -> Result<(), redis::RedisError> {
        redis::cmd("NSNEW")
            .arg(&self.name)
            .query_async(&mut (*self.con).clone())
            .await
    }

    pub async fn set_password(&self) -> Result<(), redis::RedisError> {
        redis::cmd("NSSET")
            .arg(&self.name)
            .arg("password")
            .arg(&self.name)
            .query_async(&mut (*self.con).clone())
            .await
    }

    pub async fn set_private(&self) -> Result<(), redis::RedisError> {
        redis::cmd("NSSET")
            .arg(&self.name)
            .arg("public")
            .arg("false")
            .query_async(&mut (*self.con).clone())
            .await
    }

    pub async fn open(&self) -> Result<(), redis::RedisError> {
        redis::cmd("SELECT")
            .arg(&self.name)
            .query_async(&mut (*self.con).clone())
            .await
    }

    pub async fn open_private(&self) -> Result<(), redis::RedisError> {
        let challenge: String = redis::cmd("AUTH")
            .arg("SECURE")
            .arg("CHALLENGE")
            .query_async(&mut (*self.con).clone())
            .await?;

        let mut hasher = Sha1::new();
        hasher.update(format!("{}:{}", challenge, self.name).as_bytes());
        let result = hex::encode(hasher.finalize());

        redis::cmd("SELECT")
            .arg(&self.name)
            .arg(&self.name)
            // .arg("SECURE")
            // .arg(result)
            .query_async(&mut (*self.con).clone())
            .await
    }

    pub async fn delete(&self) -> Result<(), redis::RedisError> {
        redis::cmd("SELECT")
            .arg("default")
            .query_async(&mut (*self.con).clone())
            .await?;

        redis::cmd("NSDEL")
            .arg(&self.name)
            .query_async(&mut (*self.con).clone())
            .await
    }

    pub async fn delete_private(&self) -> Result<(), redis::RedisError> {
        redis::cmd("FLUSH")
            .query_async(&mut (*self.con).clone())
            .await?;

        tokio::time::sleep(Duration::from_secs(5)).await;
        redis::cmd("SELECT")
            .arg("default")
            .query_async(&mut (*self.con).clone())
            .await?;

        redis::cmd("NSDEL")
            .arg(&self.name)
            .query_async(&mut (*self.con).clone())
            .await
    }

    pub async fn set(&self, key: Option<&[u8]>, data: &[u8]) -> Result<Vec<u8>, redis::RedisError> {
        redis::cmd("SET")
            .arg(key.unwrap_or(&[]))
            .arg(data)
            .query_async(&mut (*self.con).clone())
            .await
    }
}

#[derive(Clone)]
struct Con {
    con: MultiplexedConnection,
}

impl Deref for Con {
    type Target = MultiplexedConnection;

    fn deref(&self) -> &Self::Target {
        &self.con
    }
}

impl Con {
    /// Create a new async connection to a server.
    pub async fn new<T: IntoConnectionInfo>(ci: T) -> Result<Con, redis::RedisError> {
        let con = Client::open(ci.into_connection_info()?)?
            .get_multiplexed_tokio_connection()
            .await?;
        Ok(Con { con })
    }
}
