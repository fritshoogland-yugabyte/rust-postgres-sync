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
use rand::distributions::{Alphanumeric, Uniform, Distribution};
use postgres::types::ToSql;
use num_traits::cast::ToPrimitive;
use std::io::Write;
use chrono::{DateTime, Utc, Local};
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
    graph: bool,
    runtime_select: i64,
) {
    let connection_pool = create_pool(url, threads, cacert_file);

    let connection = connection_pool.get().unwrap();
    run_create_table(connection, tablets, drop);
    let connection = connection_pool.get().unwrap();
    run_create_procedure(connection);

    for operation in operations.split(",") {
        match operation {
            "copy_mem" => {
                let connection_pool = connection_pool.clone();
                let connection = connection_pool.get().unwrap();
                run_truncate(connection);
                println!(">> copy_mem");
                let pool = connection_pool.clone();
                let tp = rayon::ThreadPoolBuilder::new().num_threads(threads.try_into().unwrap()).build().unwrap();
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
                if graph {
                    draw_plot(graph_data, "copy_mem");
                }
            },
            "copy_file" => {
                let connection_pool = connection_pool.clone();
                let connection = connection_pool.get().unwrap();
                run_truncate(connection);
                println!(">> copy_file");
                let pool = connection_pool.clone();
                let tp = rayon::ThreadPoolBuilder::new().num_threads(threads.try_into().unwrap()).build().unwrap();
                let (tx_copy, rx_copy) = channel();
                let mut histogram = Histogram::with_buckets(10);
                let mut query_time: u64 = 0;
                let copy_start_time = Instant::now();
                tp.scope(move |s| {
                    for thread_id in 0..threads {
                        let connection = pool.get().unwrap();
                        let tx_copy = tx_copy.clone();
                        s.spawn(move |_| {
                            let latencies_vec = run_copy_file(connection, rows, batch_size, thread_id, nontransactional, text_fields_length, threads);
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
                println!("rows per thread : {:12}", rows/threads);
                println!("threads         : {:12}", threads);
                println!("batch           : {:12}", "-");
                println!("total rows      : {:12}", rows);
                println!("nontransactional: {:>12}", nontransactional);
                println!("wallclock tm/row: {:12.6} us", copy_time as f64 / rows.to_f64().unwrap());
                println!("db tm/row       : {:12.6} us", query_time as f64 / rows.to_f64().unwrap());
                if print_histogram {
                    println!("histogram is per batch ({} rows)", batch_size);
                    println!("{}", histogram);
                }
                if graph {
                    draw_plot(graph_data, "copy_file");
                }
            },
            "insert" => {
                let connection_pool = connection_pool.clone();
                let connection = connection_pool.get().unwrap();
                run_truncate(connection);

                println!(">> insert");
                let pool = connection_pool.clone();
                let tp = rayon::ThreadPoolBuilder::new().num_threads(threads.try_into().unwrap()).build().unwrap();
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
                if graph {
                    draw_plot(graph_data, "insert");
                }
            },
            "select" => {
                let connection_pool = connection_pool.clone();

                println!(">> select");
                let pool = connection_pool.clone();
                let tp = rayon::ThreadPoolBuilder::new().num_threads(threads.try_into().unwrap()).build().unwrap();
                let (tx_select, rx_select) = channel();
                let mut histogram = Histogram::with_buckets(10);
                let mut query_time: u64 = 0;
                let select_start_time = Instant::now();
                tp.scope(move |s| {
                    for thread_id in 0..threads {
                        let connection = pool.get().unwrap();
                        let tx_select = tx_select.clone();
                        s.spawn(move |_| {
                            let latencies = run_select(connection, rows, runtime_select, thread_id, no_prepared);
                            tx_select.send(latencies).unwrap();
                        });
                    }
                });
                let select_time = select_start_time.elapsed().as_micros();
                let mut graph_data: Vec<(DateTime<Utc>,u64,i32)> = Vec::new();
                for latency_vec in rx_select {
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
                println!("wallclock time  : {:12.6} sec", select_time as f64 / 1000000.0);
                println!("tot db time     : {:12.6} sec {:5.2} %", query_time as f64 / 1000000.0, (query_time as f64/select_time as f64)*100.0);
                println!("threads         : {:12}", threads);
                println!("nr queries      : {:12}", graph_data.iter().count());
                println!("nr queries/thr. : {:12.0}", graph_data.iter().count() as f64/threads.to_f64().unwrap());
                println!("avg.wallclock/q : {:12.6} us", select_time as f64 / graph_data.iter().count() as f64);
                println!("avg.time/query  : {:12.6} us", graph_data.iter().map(|(_x, y, _z)| y.to_f64().unwrap()).sum::<f64>() / graph_data.iter().count() as f64);
                //println!("batch           : {:12}", batch_size);
                //println!("total rows      : {:12}", rows * threads);
                //println!("nontransactional: {:>12}", nontransactional);
                println!("no_prepared     : {:>12}", no_prepared);
                //println!("wallclock tm/row: {:12.6} us", select_time as f64 / (rows * threads).to_f64().unwrap());
                //println!("db tm/row       : {:12.6} us", query_time as f64 / (rows * threads).to_f64().unwrap());
                if print_histogram {
                    println!("{}", histogram);
                }
                if graph {
                    draw_plot(graph_data, "select");
                }
            },
            "update" => {
                let connection_pool = connection_pool.clone();

                println!(">> update");
                let pool = connection_pool.clone();
                let tp = rayon::ThreadPoolBuilder::new().num_threads(threads.try_into().unwrap()).build().unwrap();
                let (tx_update, rx_update) = channel();
                let mut histogram = Histogram::with_buckets(10);
                let mut query_time: u64 = 0;
                let update_start_time = Instant::now();
                tp.scope(move |s| {
                    for thread_id in 0..threads {
                        let connection = pool.get().unwrap();
                        let tx_update = tx_update.clone();
                        s.spawn(move |_| {
                            let latencies = run_update(connection, rows, runtime_select, thread_id, no_prepared, text_fields_length);
                            tx_update.send(latencies).unwrap();
                        });
                    }
                });
                let update_time = update_start_time.elapsed().as_micros();
                let mut graph_data: Vec<(DateTime<Utc>,u64,i32)> = Vec::new();
                for latency_vec in rx_update {
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
                println!("wallclock time  : {:12.6} sec", update_time as f64 / 1000000.0);
                println!("tot db time     : {:12.6} sec {:5.2} %", query_time as f64 / 1000000.0, (query_time as f64/update_time as f64)*100.0);
                println!("threads         : {:12}", threads);
                println!("nr queries      : {:12}", graph_data.iter().count());
                println!("nr queries/thr. : {:12.0}", graph_data.iter().count() as f64/threads.to_f64().unwrap());
                println!("avg.wallclock/q : {:12.6} us", update_time as f64 / graph_data.iter().count() as f64);
                println!("avg.time/query  : {:12.6} us", graph_data.iter().map(|(_x, y, _z)| y.to_f64().unwrap()).sum::<f64>() / graph_data.iter().count() as f64);
                //println!("batch           : {:12}", batch_size);
                //println!("total rows      : {:12}", rows * threads);
                //println!("nontransactional: {:>12}", nontransactional);
                println!("no_prepared     : {:>12}", no_prepared);
                //println!("wallclock tm/row: {:12.6} us", select_time as f64 / (rows * threads).to_f64().unwrap());
                //println!("db tm/row       : {:12.6} us", query_time as f64 / (rows * threads).to_f64().unwrap());
                if print_histogram {
                    println!("{}", histogram);
                }
                if graph {
                    draw_plot(graph_data, "update");
                }
            },
            "procedure" => {
                let connection_pool = connection_pool.clone();
                let connection = connection_pool.get().unwrap();
                run_truncate(connection);

                println!(">> procedure");
                let pool = connection_pool.clone();
                let tp = rayon::ThreadPoolBuilder::new().num_threads(threads.try_into().unwrap()).build().unwrap();
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

                if graph {
                    draw_plot(graph_data, "procedure");
                }
            },
            &_ => println!("unknown operation: {}", operation),
        }
    }
}

fn run_create_table(mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>, tablets: i32, drop: bool) {
    if drop {
        println!(">> drop table");
        let sql_statement = format!("drop table if exists test_table");
        connection.simple_query(&sql_statement).expect("error during drop table if exists test_table");
    };
    println!(">> create table if not exists");
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

pub fn run_select(
    mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>,
    rows: i32,
    runtime_select: i64,
    thread_id: i32,
    no_prepared: bool,
) -> Vec<(DateTime<Utc>,u64,i32)> {
    let mut query_latencies: Vec<(DateTime<Utc>, u64, i32)> = Vec::new();
    //let start_id = rows * thread_id;
    //let end_id = start_id + rows - 1;

    let select_statement = "select f1, f2, f3, f4 from test_table where id = $1";
    let prepared_select_statement = connection.prepare(select_statement).unwrap();
    let mut rng = rand::thread_rng();
    let range = Uniform::from(0..rows);
    let begin_time = Local::now();

    loop {
        if Local::now() >= begin_time.checked_add_signed(chrono::Duration::minutes(runtime_select)).unwrap() { break };
        let query_start_time = Instant::now();
        //dbg!(rows);
        let number = range.sample(&mut rng);
        let result = if no_prepared {
            connection.query_one(select_statement, &[&number]).unwrap()
        } else {
            connection.query_one(&prepared_select_statement, &[&number]).unwrap()
        };
        let _f1: &str = result.get("f1");
        let _f2: &str = result.get("f2");
        let _f3: &str = result.get("f3");
        let _f4: &str = result.get("f4");
        query_latencies.push((Utc::now(), query_start_time.elapsed().as_micros().to_u64().unwrap(), thread_id));
    }

    query_latencies
}

pub fn run_update(
    mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>,
    rows: i32,
    runtime_update: i64,
    thread_id: i32,
    no_prepared: bool,
    text_fields_length: i32,
) -> Vec<(DateTime<Utc>,u64,i32)> {
    let mut query_latencies: Vec<(DateTime<Utc>, u64, i32)> = Vec::new();

    let update_statement = "update test_table set f1=$2, f2=$3, f3=$4, f4=$5 where id = $1";
    let prepared_update_statement = connection.prepare(update_statement).unwrap();
    let mut rng = rand::thread_rng();
    let range = Uniform::from(0..rows);
    let begin_time = Local::now();

    loop {
        if Local::now() >= begin_time.checked_add_signed(chrono::Duration::minutes(runtime_update)).unwrap() { break };
        let query_start_time = Instant::now();
        let number = range.sample(&mut rng);
        let f1 = random_characters(text_fields_length);
        let f2 = random_characters(text_fields_length);
        let f3 = random_characters(text_fields_length);
        let f4 = random_characters(text_fields_length);
        let _result = if no_prepared {
            connection.execute(update_statement, &[&number, &f1, &f2, &f3, &f4]).unwrap()
        } else {
            connection.execute(&prepared_update_statement, &[&number, &f1, &f2, &f3, &f4]).unwrap()
        };
        query_latencies.push((Utc::now(), query_start_time.elapsed().as_micros().to_u64().unwrap(), thread_id));
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

pub fn run_copy_file(
    mut connection: PooledConnection<PostgresConnectionManager<MakeTlsConnector>>,
    rows: i32,
    _values_batch: i32,
    thread_id: i32,
    nontransactional: bool,
    text_fields_length: i32,
    threads: i32,
) -> Vec<(DateTime<Utc>,u64, i32)> {
    let mut query_latencies: Vec<(DateTime<Utc>,u64,i32)> = Vec::new();

    if nontransactional {
        connection.simple_query("set yb_disable_transactional_writes=on").expect("error in setting yb_disable_transactional_writes to on");
    } else {
        connection.simple_query("set yb_disable_transactional_writes=off").expect("error in setting yb_disable_transactional_writes to off");
    }

    let sql_statement = format!("copy test_table from '/tmp/ysql_bench-t{}-r{}-f{}--nr{}.csv' with (format csv)",threads,rows,text_fields_length,thread_id+1);

    let query_start_time = Instant::now();
    connection.simple_query(&sql_statement).expect("error during copy from file");
    query_latencies.push((Utc::now(), query_start_time.elapsed().as_micros().to_u64().unwrap(), thread_id));


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


#[allow(dead_code)]
fn create_connection(
    url: &str,
    cacert_file: &str,
) -> Client {
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

fn draw_plot(latency_vec: Vec<(DateTime<Utc>,u64,i32)>, heading: &str) {
    let start_time = latency_vec.iter().map(|(date, _val, _thread_id)| date).min().unwrap();
    let end_time = latency_vec.iter().map(|(date, _val, _thread_id)| date).max().unwrap();
    let low_value: u64 = 0;
    let high_value = latency_vec.iter().map(|(_date, val, _thread_id)| val).max().unwrap();
    let root = BitMapBackend::new("plot.png", (1024,768))
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
    context.draw_series( latency_vec.iter()
            .map(|(timestamp, latency, thread_id)| Circle::new((*timestamp, *latency), 2, Palette99::pick(*thread_id as usize).filled()))
           ).unwrap();
}