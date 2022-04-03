//use rayon;
//use std::sync::mpsc::channel;
//use r2d2_postgres::{PostgresConnectionManager};
//use r2d2::Pool;
use histo::Histogram;
//use std::time::{Duration, Instant};
use std::time::{Instant};
use postgres::{Client, NoTls};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;

pub fn run_test() {
    let nr_loop=10000;
    let urls = [ "host=/tmp sslmode=disable user=postgres password=postgres",
        "host=localhost port=5432 sslmode=disable user=postgres password=postgres",
        "host=localhost port=5432 sslmode=require user=postgres password=postgres",
        "host=10.0.2.15 port=5432 sslmode=disable user=postgres password=postgres",
        "host=10.0.2.15 port=5432 sslmode=require user=postgres password=postgres" ];
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
    builder.set_ca_file("/tmp/ca.cert").expect("unable to load ca.cert");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    for url in urls {
        let mut histogram = Histogram::with_buckets(10);
        for _ in 0..nr_loop {
            let connector = connector.clone();
            let now = Instant::now();
            if url.contains("sslmode=disable") {
                let _connection=create_notls_connection(url);
            } else {
                let _connection=create_tls_connection(url, connector);
            }
            histogram.add(now.elapsed().as_micros().try_into().unwrap());
        }
        println!("Run: {}", url);
        println!("{}", histogram);
    }
}
pub fn create_notls_connection(url: &str) -> Client {
    let connection = Client::connect(url, NoTls).expect("failed to create notls postgres connection");
    connection
}
pub fn create_tls_connection(url: &str, connection: MakeTlsConnector) -> Client {
    let connection = Client::connect(url, connection).expect("failed to create tls postgres connection");
    connection
}

/*
pub fn create_socket_connection() -> Client {
    let url = "host=/tmp sslmode=disable user=postgres password=postgres";
    let connection = Client::connect(url, NoTls).expect("failed to create postgres connection");
    connection
}
pub fn create_localhost_nossl_connection() -> Client {
    let url = "host=localhost port=5432 sslmode=disable user=postgres password=postgres";
    let connection = Client::connect(url, NoTls).expect("failed to create postgres connection");
    connection
}

pub fn create_localhost_ssl_connection() -> Client {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
    builder.set_ca_file("/tmp/ca.cert").expect("unable to load ca.cert");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let url = "host=localhost port=5432 sslmode=require user=postgres password=postgres";
    let connection = Client::connect(url, connector).expect("failed to create postgres connection");
    connection
}

pub fn create_nic_nossl_connection() -> Client {
    let url = "host=10.0.2.15 port=5432 sslmode=disable user=postgres password=postgres";
    let connection = Client::connect(url, NoTls).expect("failed to create postgres connection");
    connection
}

pub fn create_nic_ssl_connection() -> Client {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
    builder.set_ca_file("/tmp/ca.cert").expect("unable to load ca.cert");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let url = "host=10.0.2.15 port=5432 sslmode=require user=postgres password=postgres";
    let connection = Client::connect(url, connector).expect("failed to create postgres connection");
    connection
}
pub fn create_pool() -> Pool<PostgresConnectionManager<NoTls>> {
    let manager = PostgresConnectionManager::new(
        "host=192.168.66.80,192.168.66.81,192.168.66.82 port=5433 user=yugabyte password=yugabyte".parse().unwrap(),
        NoTls,
    );
    r2d2::Pool::builder()
        .max_size(5)
        .connection_timeout(Duration::from_secs(120))
        .build(manager)
        .unwrap()
}

pub fn run_queries(pool: Pool<PostgresConnectionManager<NoTls>>, mut histogram: Histogram) -> Histogram {
    let tp = rayon::ThreadPoolBuilder::new().num_threads(10).build().unwrap();
    let (tx,rx) = channel();
    tp.scope(move |s| {
        for _i in 0..10 {
            let pool = pool.clone();
            let tx = tx.clone();
            s.spawn(move |_| {
                let mut client = pool.get().unwrap();
                //client.execute("select $1", &[&i.to_string()]).unwrap();
                for _ in 0..1000
                {
                    let now = Instant::now();
                    //client.simple_query("select pg_sleep(10)").unwrap();
                    client.simple_query("select 1").unwrap();
                    //histogram.add(now.elapsed().as_micros().try_into().unwrap())
                    tx.send(now.elapsed().as_micros());
                    //thread::sleep(Duration::from_secs(60));
                }
            });
        }
    });
    for sample in rx {
        histogram.add(sample.try_into().unwrap());
    }
    histogram
}

fn run_query(mut connection: Client) {
    connection.simple_query("select 1").unwrap();
}

pub fn run_connect(mut histogram: Histogram) -> Histogram {
    for _ in 0..1000 {
        let now = Instant::now();
        let connection = create_localhost_nossl_connection();
        histogram.add(now.elapsed().as_micros().try_into().unwrap());
        //run_query(connection);
    }
    histogram
}
*/