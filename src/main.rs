use ysql_bench::run;
use structopt::StructOpt;
use dotenv::dotenv;
use std::collections::HashMap;
use std::{env, process, fs};
use std::io::Write;

const URL: &str = "host=192.168.66.80 port=5433 sslmode=disable user=yugabyte password=yugabyte";
const TABLE_NAME: &str = "test_table";

#[derive(Debug, StructOpt)]
struct Opts {
    /// ca certificate
    #[structopt(short, long, default_value = "/tmp/ca.cert")]
    cacert_file: String,
    /// f1..f4 text field length
    #[structopt(short, long, default_value = "24")]
    fields_length: i32,
    /// batch size
    #[structopt(short, long, default_value = "200")]
    batch_size: i32,
    /// rows
    #[structopt(short, long, default_value = "100000")]
    rows: i32,
    /// threads
    #[structopt(short, long, default_value = "1")]
    threads: i32,
    /// print_histogram
    #[structopt(short, long)]
    print_histogram: bool,
    /// nontransactional
    #[structopt(short, long)]
    nontransactional: bool,
    /// show the size of a row
    #[structopt(long)]
    show_rowsize: bool,
    /// operations (copy_mem,copy_file,insert,procedure,select,update)
    #[structopt(short, long, default_value = "")]
    operations: String,
    /// number of tablets
    #[structopt(long, default_value = "3")]
    tablets: i32,
    /// do NOT use prepared statements
    #[structopt(long)]
    no_prepared: bool,
    /// YSQL connect url
    #[structopt(short, long, default_value = URL)]
    url: String,
    /// drop table
    #[structopt(long)]
    drop: bool,
    /// create graph of the measured statistics
    #[structopt(long)]
    graph: bool,
    /// runtime for select in minutes
    #[structopt(long, default_value = "30")]
    runtime_select: i64,
    /// table name to use
    #[structopt(long, default_value = TABLE_NAME)]
    table_name: String,
    /// number of buckets for histogram
    #[structopt(long, default_value = "10")]
    histogram_buckets: u64,
}

fn main() {
    let options = Opts::from_args();

    let mut changed_options = HashMap::new();
    dotenv().ok();

    let url_string = if options.url == URL {
        match env::var("YSQLBENCH_URL") {
            Ok(var) => {
                changed_options.insert("YSQLBENCH_URL", format!(r#""{}""#, var.to_owned()));
                var
            },
            Err(_e)        => URL.to_string(),
        }
    } else {
        changed_options.insert("YSQLBENCH_URL", format!(r#""{}""#, options.url.to_owned()));
        options.url
    };
    let url = url_string.as_str();

    let cacert_file = &options.cacert_file as &str;
    let text_fields_length = options.fields_length as i32;
    let batch_size = options.batch_size as i32;
    let runtime_select = options.runtime_select as i64;
    let rows = options.rows as i32;
    let threads = options.threads as i32;
    let print_histogram = options.print_histogram as bool;
    let nontransactional = options.nontransactional as bool;
    let show_rowsize = options.show_rowsize as bool;
    let operations = &options.operations as &str;
    let tablets = options.tablets as i32;
    let histogram_buckets = options.histogram_buckets as u64;
    let no_prepared = options.no_prepared as bool;
    let drop = options.drop as bool;
    let graph = options.graph as bool;
    let table_name = &options.table_name as &str;
    run(
        cacert_file,
        text_fields_length,
        batch_size,
        rows,
        threads,
        print_histogram,
        nontransactional,
        show_rowsize,
        operations,
        tablets,
        no_prepared,
        url,
        drop,
        graph,
        runtime_select,
        table_name,
        histogram_buckets,
    );

    if changed_options.len() > 0 {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(".env")
            .unwrap_or_else(| e | {
                eprintln!("error writing .env file into current working directory: {}", e);
                process::exit(1);
            });
        for (key, value) in changed_options {
            file.write(format!("{}={}\n", key, value).as_bytes()).unwrap();
        }
        file.flush().unwrap();
    }
}
