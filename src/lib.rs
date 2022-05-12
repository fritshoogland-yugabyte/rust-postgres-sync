use rayon;
use std::sync::mpsc::channel;
use r2d2_postgres::{PostgresConnectionManager};
use r2d2::{Pool, PooledConnection};
use histo::Histogram;
use std::time::{Duration, Instant};
use postgres::{Client, NoTls};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use postgres::types::ToSql;
use num_traits::cast::ToPrimitive;
use std::io::Write;
use chrono::{DateTime, Utc};
use plotters::prelude::*;

//const PG_URL: &str = "host=192.168.66.80 port=5434 sslmode=disable user=yugabyte password=yugabyte";

/*
macro_rules! retry {
    ($f:expr) => {{
        let mut retries = 3;
        loop {
            let result = $f();
            if results.is_ok() {
                break result;
            }
            if retries > 0 {
                retries -= 1;
                print!(".");
                thread::sleep(time::Duration::from_secs(5));
                continue;
            }
        }
        break result;
    }};
}
 */

pub fn run(
    cacert_file: &str,
    text_fields_length: i32,
    batch_size: i32,
    rows: i32,
    threads: i32,
    print_histogram: bool,
    nontransactional: bool,
    show_rowsize: bool,
    operations: &str,
    tablets: i32,
    no_prepared: bool,
    url: &str,
    drop: bool,
) {
    let connection_pool = create_pool(url, threads, cacert_file);

    let connection = connection_pool.get().unwrap();
    run_create_table(connection, tablets, drop);
    let connection = connection_pool.get().unwrap();
    run_create_procedure(connection);

    for operation in operations.split(",") {
        match operation {
            "copy" => {
                let connection_pool = connection_pool.clone();
                let connection = connection_pool.get().unwrap();
                run_truncate(connection);
                println!(">> copy");
                let pool = connection_pool.clone();
                let tp = rayon::ThreadPoolBuilder::new().num_threads(10).build().unwrap();
                let (tx_copy, rx_copy) = channel();
                let mut histogram = Histogram::with_buckets(10);
                let mut query_time: u64 = 0;
                let copy_start_time = Instant::now();
                tp.scope(move |s| {
                    for thread_id in 0..threads {
                        let connection = pool.get().unwrap();
                        let tx_copy = tx_copy.clone();
                        s.spawn(move |_| {
                            let latencies_vec = run_copy_from(connection, rows, batch_size, thread_id, nontransactional, text_fields_length);
                            tx_copy.send(latencies_vec).unwrap();
                        });
                    }
                });
                let copy_time = copy_start_time.elapsed().as_micros();
                let mut graph_data: Vec<(DateTime<Utc>,u64,i32)> = Vec::new();
                for latency_vec in rx_copy {
                    for ( utc_time, latency, thread_id ) in latency_vec {
                        graph_data.push((utc_time, latency, thread_id));
                        histogram.add(latency);
                        query_time += latency;
                    }
                }
                if show_rowsize {
                    let connection = connection_pool.get().unwrap();
                    run_show_rowsize(connection);
                }
                println!("wallclock time  : {:12.6} sec", copy_time as f64 / 1000000.0);
                println!("tot db time     : {:12.6} sec {:5.2} %", query_time as f64 / 1000000.0, (query_time as f64/copy_time as f64)*100.0);
                println!("rows per thread : {:12}", rows);
                println!("threads         : {:12}", threads);
                println!("batch           : {:12}", batch_size);
                println!("total rows      : {:12}", rows * threads);
                println!("nontransactional: {:>12}", nontransactional);
                println!("wallclock tm/row: {:12.6} us", copy_time as f64 / (rows * threads).to_f64().unwrap());
                println!("db tm/row       : {:12.6} us", query_time as f64 / (rows * threads).to_f64().unwrap());
                if print_histogram {
                    println!("histogram is per batch ({} rows)", batch_size);
                    println!("{}", histogram);
                }

                draw_plot(graph_data, "copy");
            },
            "insert" => {
                let connection_pool = connection_pool.clone();
                let connection = connection_pool.get().unwrap();
                run_truncate(connection);

                println!(">> insert");
                let pool = connection_pool.clone();
                let tp = rayon::ThreadPoolBuilder::new().num_threads(10).build().unwrap();
                let (tx_insert, rx_insert) = channel();
                let mut histogram = Histogram::with_buckets(10);
                let mut query_time: u64 = 0;
                let insert_start_time = Instant::now();
                tp.scope(move |s| {
                    for thread_id in 0..threads {
                        let connection = pool.get().unwrap();
                        let tx_insert = tx_insert.clone();
                        s.spawn(move |_| {
                            let latencies = run_insert(connection, rows, batch_size, thread_id, nontransactional, text_fields_length, no_prepared);
                            tx_insert.send(latencies).unwrap();
                        });
                    }
                });
                let insert_time = insert_start_time.elapsed().as_micros();
                let mut graph_data: Vec<(DateTime<Utc>,u64,i32)> = Vec::new();
                for latency_vec in rx_insert {
                    for ( utc_time, latency, thread_id) in latency_vec {
                        graph_data.push((utc_time, latency, thread_id));
                        histogram.add(latency);
                        query_time += latency;
                    }
                }
                if show_rowsize {
                    let connection = connection_pool.get().unwrap();
                    run_show_rowsize(connection);
                }
                println!("wallclock time  : {:12.6} sec", insert_time as f64 / 1000000.0);
                println!("tot db time     : {:12.6} sec {:5.2} %", query_time as f64 / 1000000.0, (query_time as f64/insert_time as f64)*100.0);
                println!("rows per thread : {:12}", rows);
                println!("threads         : {:12}", threads);
                println!("batch           : {:12}", batch_size);
                println!("total rows      : {:12}", rows * threads);
                println!("nontransactional: {:>12}", nontransactional);
                println!("no_prepared     : {:>12}", no_prepared);
                println!("wallclock tm/row: {:12.6} us", insert_time as f64 / (rows * threads).to_f64().unwrap());
                println!("db tm/row       : {:12.6} us", query_time as f64 / (rows * threads).to_f64().unwrap());
                if print_histogram {
                    println!("histogram is per batch ({} rows)", batch_size);
                    println!("{}", histogram);
                }

                draw_plot(graph_data, "insert");
            },
            "procedure" => {
                let connection_pool = connection_pool.clone();
                let connection = connection_pool.get().unwrap();
                run_truncate(connection);

                println!(">> procedure");
                let pool = connection_pool.clone();
                let tp = rayon::ThreadPoolBuilder::new().num_threads(10).build().unwrap();
                let (tx_proc, rx_proc) = channel();
                let mut histogram = Histogram::with_buckets(10);
                let mut query_time: u64 = 0;
                let proc_start_time = Instant::now();
                tp.scope(move |s| {
                    for thread_id in 0..threads {
                        let connection = pool.get().unwrap();
                        let tx_proc = tx_proc.clone();
                        s.spawn(move |_| {
                            let latencies = run_procedure(connection, rows, batch_size, thread_id + 1, nontransactional, text_fields_length);
                            tx_proc.send(latencies).unwrap();
                        });
                    }
                });
                let proc_time = proc_start_time.elapsed().as_micros();
                let mut graph_data: Vec<(DateTime<Utc>,u64,i32)> = Vec::new();
                for latency_vec in rx_proc {
                    for ( utc_time, latency, thread_id) in latency_vec {
                        graph_data.push((utc_time, latency, thread_id));
                        histogram.add(latency);
                        query_time += latency;
                    }
                }
                if show_rowsize {
                    let connection = connection_pool.get().unwrap();
                    run_show_rowsize(connection);
                }
                println!("wallclock time  : {:12.6} sec", proc_time as f64 / 1000000.0);
                println!("tot db time     : {:12.6} sec {:5.2} %", query_time as f64 / 1000000.0, (query_time as f64/proc_time as f64)*100.0);
                println!("rows per thread : {:12}", rows);
                println!("threads         : {:12}", threads);
                println!("batch           : {:12}", batch_size);
                println!("total rows      : {:12}", rows * threads);
                println!("nontransactional: {:>12}", nontransactional);
                println!("wallclock tm/row: {:12.6} us", proc_time as f64 / (rows * threads).to_f64().unwrap());
                println!("db tm/row       : {:12.6} us", query_time as f64 / (rows * threads).to_f64().unwrap());

                draw_plot(graph_data, "procedure");
            },
            &_ => println!("unknown operation: {}", operation),
        }
    }
}

fn run_create_table(mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>, tablets: i32, drop: bool) {
    if drop {
        let sql_statement = format!("drop table if exists test_table");
        connection.simple_query(&sql_statement).expect("error during drop table if exists test_table");
    };
    let sql_statement = format!("create table if not exists test_table( id int primary key, f1 text, f2 text, f3 text, f4 text) split into {} tablets", tablets);
    connection.simple_query(&sql_statement).expect("error during create table if not exists test_table");
}

fn run_truncate(mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>) {
    let sql_statement = "truncate table test_table";
    connection.simple_query(sql_statement).expect("error during truncate");
    println!(">> truncate table");
}

fn run_show_rowsize(mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>) {
    let sql_statement = "select case when exists (select * from test_table limit 1) then pg_column_size(test_table.*) else 0 end from test_table limit 1";
    let row = connection.query_one(sql_statement, &[]).expect("error during select for table size");
    let val: i64 = row.get(0);
    println!("row size        : {:12} bytes", val);

}

fn run_create_procedure(mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>> ) {
    let sql_statement = "select count(*) from pg_proc where proname = 'load_test' and prolang = (select oid from pg_language where lanname = 'plpgsql') and prokind = 'p'";
    let row = connection.query_one(sql_statement, &[]).expect("error during select for validating procedure load_test existence via pg_proc");
    let val: i64 = row.get(0);
    if val == 0 {
        println!(">> create load_test procedure");
        let sql_statement = "
create or replace procedure load_test ( rows int, field_length int, commit_batch int, run_thread_nr int default 1)
language plpgsql as $$
declare
  v_start_id int := rows * run_thread_nr;
  v_end_id int := v_start_id + rows - 1;
  a_id int[];
  a_f1 text[];
  a_f2 text[];
  a_f3 text[];
  a_f4 text[];
begin
  for i in v_start_id..v_end_id loop
    a_id[i] := i;
    a_f1[i] := dbms_random.string('a', field_length);
    a_f2[i] := dbms_random.string('a', field_length);
    a_f3[i] := dbms_random.string('a', field_length);
    a_f4[i] := dbms_random.string('a', field_length);
    if mod(commit_batch, i) = 0 then
      insert into test_table (id, f1, f2, f3, f4)
      select unnest(a_id), unnest(a_f1), unnest(a_f2), unnest(a_f3), unnest(a_f4);
      a_id := '{}';
      a_f1 := '{}';
      a_f2 := '{}';
      a_f3 := '{}';
      a_f4 := '{}';
      commit;
    end if;
  end loop;
  if array_length(a_id,1) > 0 then
    insert into test_table (id, f1, f2, f3, f4)
    select unnest(a_id), unnest(a_f1), unnest(a_f2), unnest(a_f3), unnest(a_f4);
    commit;
  end if;
end $$;
";
        connection.simple_query(sql_statement).unwrap();
    }
}

pub fn run_insert(
    mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>,
    rows: i32,
    values_batch: i32,
    thread_id: i32,
    nontransactional: bool,
    text_fields_length: i32,
    no_prepared: bool,
) -> Vec<(DateTime<Utc>,u64,i32)> {
    let mut query_latencies: Vec<(DateTime<Utc>,u64,i32)> = Vec::new();
    let start_id = rows * thread_id;
    let end_id = start_id + rows - 1;

    if nontransactional {
        connection.simple_query("set yb_disable_transactional_writes=on").expect("error in setting yb_disable_transactional_writes to on");
    } else {
        connection.simple_query("set yb_disable_transactional_writes=off").expect("error in setting yb_disable_transactional_writes to off");
    }

    let base_insert = "insert into test_table (id,f1,f2,f3,f4) values";
    let mut fields = String::from("");
    for fields_nr in 0..values_batch {
        fields.push_str(format!("(${},${},${},${},${}),", (fields_nr*5)+1,(fields_nr*5)+2,(fields_nr*5)+3,(fields_nr*5)+4,(fields_nr*5)+5).as_str());
    }
    fields.pop();
    let statement = format!("{} {}", &base_insert, fields);
    let prepared_statement = connection.prepare(statement.as_str()).unwrap();
    for nr in (start_id..end_id).step_by(values_batch.try_into().unwrap()) {
        let mut row_values_i32: Vec<i32>= Vec::new();
        let mut row_values_string: Vec<String>= Vec::new();
        let mut values: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for value_nr in 0..values_batch {
            // build a vector with field values
            if nr + value_nr <= end_id {
                row_values_i32.push(nr + value_nr);
                row_values_string.push(random_characters(text_fields_length));
                row_values_string.push(random_characters(text_fields_length));
                row_values_string.push(random_characters(text_fields_length));
                row_values_string.push(random_characters(text_fields_length));
            }
        }
        for value_nr in 0..values_batch {
            // build a ToSql vector with references to the field values
            if nr + value_nr <= end_id {
                values.push(&row_values_i32[value_nr.to_usize().unwrap()]);
                values.push(&row_values_string[((value_nr * 4) + 0).to_usize().unwrap()]);
                values.push(&row_values_string[((value_nr * 4) + 1).to_usize().unwrap()]);
                values.push(&row_values_string[((value_nr * 4) + 2).to_usize().unwrap()]);
                values.push(&row_values_string[((value_nr * 4) + 3).to_usize().unwrap()]);
            }
        }

        let query_start_time = Instant::now();
        // if the batch length makes the last batch to insert shorter, a custom non-prepared statement is created
        // values batch * 5 fields
        if values.len() < (values_batch*5).try_into().unwrap() {
            // custom execution for the last batch with a lower number of values
            let mut fields = String::from("");
            for fields_nr in 0..(values.len()/5) {
                fields.push_str(format!("(${},${},${},${},${}),", (fields_nr*5)+1, (fields_nr*5)+2, (fields_nr*5)+3, (fields_nr*5)+4, (fields_nr*5)+5 ).as_str());
            }
            fields.pop();
            let statement = format!("{} {}", base_insert, fields);
            connection.query(&statement, &values).expect("error in performing of execution of last batch");
        } else {
            if no_prepared {
                connection.query(&statement, &values).expect("error in performing execution of dynamically created insert nonprepared");
            } else {
                // this is the regular execution of the prepared statement
                connection.query(&prepared_statement, &values[..]).expect("error in performing execution of dynamically created insert prepared");
            }
        }
        query_latencies.push((Utc::now(), query_start_time.elapsed().as_micros().to_u64().unwrap(), thread_id));
        connection.simple_query("commit").expect("error executing commit");

    }
    query_latencies
}

pub fn run_copy_from(
    mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>,
    rows: i32,
    values_batch: i32,
    thread_id: i32,
    nontransactional: bool,
    text_fields_length: i32,
) -> Vec<(DateTime<Utc>,u64, i32)> {
    let mut query_latencies: Vec<(DateTime<Utc>,u64,i32)> = Vec::new();
    let start_id = rows * thread_id;
    let end_id = start_id + rows - 1;

    if nontransactional {
        connection.simple_query("set yb_disable_transactional_writes=on").expect("error in setting yb_disable_transactional_writes to on");
    } else {
        connection.simple_query("set yb_disable_transactional_writes=off").expect("error in setting yb_disable_transactional_writes to off");
    }

    let mut writer = connection.copy_in("copy test_table from stdin").expect("error in performing copy_in");
    for nr in (start_id..end_id).step_by(values_batch.try_into().unwrap()) {
        let mut row = String::from("");
        for value_nr in 0..values_batch {
            if nr + value_nr <= end_id {
                row.push_str(format!("{}\t{}\t{}\t{}\t{}\n", nr + value_nr, random_characters(text_fields_length), random_characters(text_fields_length), random_characters(text_fields_length), random_characters(text_fields_length)).as_str());
            }
        }
        let query_start_time = Instant::now();
        writer.write_all(row.as_bytes()).unwrap();
        query_latencies.push((Utc::now(), query_start_time.elapsed().as_micros().to_u64().unwrap(), thread_id));
    }
    writer.finish().unwrap();

    query_latencies
}

pub fn run_procedure(
    mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>,
    rows: i32,
    values_batch: i32,
    thread_id: i32,
    nontransactional: bool,
    text_fields_length: i32,
) -> Vec<(DateTime<Utc>,u64,i32)> {
    if nontransactional {
        connection.simple_query("set yb_disable_transactional_writes=on").expect("error in setting yb_disable_transactional_writes to on");
    } else {
        connection.simple_query("set yb_disable_transactional_writes=off").expect("error in setting yb_disable_transactional_writes to off");
    }
    let mut query_latencies: Vec<(DateTime<Utc>,u64,i32)> = Vec::new();
    let sql_statement = format!("call load_test({}, {}, {}, {});", rows, text_fields_length, values_batch, thread_id);
    let query_start_time = Instant::now();
    connection.simple_query(&sql_statement).expect("error in executing simple_query call to procedure");
    query_latencies.push((Utc::now(), query_start_time.elapsed().as_micros().to_u64().unwrap(), thread_id));
    query_latencies
}

/*
    let base_insert = "insert into test_table (id, f1, f2, f3, f4) values";
    let mut fields = String::from("");
    for fields_nr in 0..values_batch {
        fields.push_str(format!("(${}, ${}, ${}, ${}, ${}),", (fields_nr*5)+1, (fields_nr*5)+2, (fields_nr*5)+3, (fields_nr*5)+4, (fields_nr*5)+5 ).as_str());
    }
    fields.pop();
    let statement = format!("{} {}", base_insert, fields);
    let statement = connection.prepare(statement.as_str()).unwrap();
    for nr in (0..rows).step_by(values_batch.try_into().unwrap()) {
        let mut row_values_i32: Vec<i32>= Vec::new();
        let mut row_values_string: Vec<String>= Vec::new();
        let mut values: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for value_nr in 0..values_batch {
            // build a vector with field values
            row_values_i32.push(nr + value_nr + 1);
            row_values_string.push(random_characters(FIELD_LENGTH));
            row_values_string.push(random_characters(FIELD_LENGTH));
            row_values_string.push(random_characters(FIELD_LENGTH));
            row_values_string.push(random_characters(FIELD_LENGTH));
        }
        for value_nr in 0..values_batch {
            // build a ToSql vector with references to the field values
            values.push(&row_values_i32[value_nr.to_usize().unwrap()] );
            values.push(&row_values_string[((value_nr*4)+0).to_usize().unwrap()] );
            values.push(&row_values_string[((value_nr*4)+1).to_usize().unwrap()] );
            values.push(&row_values_string[((value_nr*4)+2).to_usize().unwrap()] );
            values.push(&row_values_string[((value_nr*4)+3).to_usize().unwrap()] );
        }

        let query_start_time = Instant::now();
        connection.query( &statement, &values[..]).unwrap();
        histogram.add(query_start_time.elapsed().as_micros().try_into().unwrap());
        overall_query_time += query_start_time.elapsed().as_micros()
    }
    let overall_total_time = overall_start_time.elapsed().as_micros();
    println!("total rows     : {:6}", rows);
    println!("values batch   : {:6}", values_batch);
    println!("total time (s) : {:13.6}", overall_total_time as f64/1000000.0);
    println!("total query (s): {:13.6} SQL time: {:5.2}%", overall_query_time as f64/1000000.0, overall_query_time as f64/overall_total_time as f64*100.0);
    //println!("{}", histogram);
}

 */

#[allow(dead_code)]
fn create_connection(url: &str, cacert_file: &str) -> Client {
    let connection = if url.contains("sslmode=require") {
        let tls = make_tls(cacert_file);
        Client::connect(url, tls).expect("failed to create tls postgres connection.")
    } else {
        Client::connect(url, NoTls).expect("failed to create notls postgres connection.")
    };
    connection
}

#[allow(dead_code)]
fn make_tls(cacert_file: &str) -> MakeTlsConnector {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
    builder.set_ca_file(cacert_file).expect("unable to load ca.cert");
    builder.set_verify(SslVerifyMode::NONE);
    MakeTlsConnector::new(builder.build())
}

//pub fn create_pool<T>(url: &str, pool_size: i32, cacert_file: &str) -> T {
pub fn create_pool(url: &str, pool_size: i32, cacert_file: &str) -> Pool<PostgresConnectionManager<MakeTlsConnector>> {



       let mut builder = SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");

       if url.contains("sslmode=require") {
           builder.set_ca_file(cacert_file).expect("unable to load ca.cert");
           builder.set_verify(SslVerifyMode::NONE);
           println!(">> creating ssl pool");
       } else {
           println!(">> creating nossl pool");
       }

       let connector = MakeTlsConnector::new(builder.build());
       let manager = PostgresConnectionManager::new( url.parse().unwrap(), connector);

       r2d2::Pool::builder()
           .max_size(pool_size as u32)
           .connection_timeout(Duration::from_secs(120))
           .build(manager)
           .unwrap()

           /*
    if url.contains("sslmode=require") {
   } else {


       //let manager = PostgresConnectionManager::new( url.parse().unwrap(), NoTls);
       let connector = MakeTlsConnector::new(builder.build());
       let manager = PostgresConnectionManager::new( url.parse().unwrap(), connector);

       r2d2::Pool::builder()
           .max_size(pool_size as u32)
           .connection_timeout(Duration::from_secs(120))
           .build(manager)
           .unwrap()
   }
            */

}

pub fn create_pool_nossl<T>(url: &str, pool_size: i32) -> Pool<PostgresConnectionManager<NoTls>> {
    println!(">> creating nossl pool");
    let manager = PostgresConnectionManager::new( url.parse().unwrap(), NoTls);

    r2d2::Pool::builder()
        .max_size(pool_size as u32)
        .connection_timeout(Duration::from_secs(120))
        .build(manager)
        .unwrap()
}

pub fn create_pool_ssl(url: &str, pool_size: i32, cacert_file: &str) -> Pool<PostgresConnectionManager<MakeTlsConnector>> {
    let mut builder = SslConnector::builder(SslMethod::tls()).expect("unable to create sslconnector builder");
    builder.set_ca_file(cacert_file).expect("unable to load ca.cert");
    builder.set_verify(SslVerifyMode::NONE);
    let connector = MakeTlsConnector::new(builder.build());
    let manager = PostgresConnectionManager::new( url.parse().unwrap(), connector);

    r2d2::Pool::builder()
        .max_size(pool_size as u32)
        .connection_timeout(Duration::from_secs(120))
        .build(manager)
        .unwrap()
}

fn random_characters(length: i32) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length.try_into().unwrap())
        .map(char::from)
        .collect()
}

/*
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

 */

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

fn draw_plot(latency_vec: Vec<(DateTime<Utc>,u64,i32)>, heading: &str) {
    let start_time = latency_vec.iter().map(|(date, _val, _thread_id)| date).min().unwrap();
    let end_time = latency_vec.iter().map(|(date, _val, _thread_id)| date).max().unwrap();
    let low_value: u64 = 0;
    let high_value = latency_vec.iter().map(|(_date, val, _thread_id)| val).max().unwrap();
    let root = BitMapBackend::new("plot.png", (600,400))
        .into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut context = ChartBuilder::on(&root)
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 50)
        .caption(heading, ("sans-serif", 20))
        .build_cartesian_2d(*start_time..*end_time, low_value..*high_value)
        .unwrap();
    context.configure_mesh()
        .x_labels(4)
        .x_label_formatter(&|x| x.naive_local().to_string())
        .x_desc("Time")
        .y_desc("Latency in microseconds per batch")
        .draw()
        .unwrap();
    context.draw_series(
        latency_vec.iter().map(|(timestamp, latency, thread_id)| Circle::new((*timestamp, *latency), 2, &Palette99::pick(*thread_id as usize)))
    ).unwrap();
    //.label().map(|(timestamp, latency, thread_id)| thread_id)
    //.legend(move |(x,y)| Rectangle::new(vec![(x,y), (x+20,y)], color.filled() ))
}