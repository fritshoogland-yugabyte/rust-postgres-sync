use structopt::StructOpt;
use std::{process, fs};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use std::io::Write;

#[derive(Debug, StructOpt)]
struct Opts {
    /// rows
    #[structopt(short, long, default_value = "1000000")]
    rows: i32,
    /// threads
    #[structopt(short, long, default_value = "1")]
    threads: i32,
    /// f1..f4 text field length
    #[structopt(short, long, default_value = "24")]
    fields_length: i32,
}

fn main() {
    let options = Opts::from_args();

    let rows = options.rows as i32;
    let threads = options.threads as i32;
    let fields_length = options.fields_length as i32;

    for file_nr in 1..=threads {
        let nr_batch = rows/threads;
        let begin_nr = (nr_batch*file_nr)-nr_batch;
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(format!("ysql_bench-t{}-r{}-f{}--nr{}.csv", threads, rows, fields_length, file_nr))
            .unwrap_or_else(| e | {
                eprintln!("error writing file into current working directory: {}", e);
                process::exit(1);
            });
        for nr in begin_nr..begin_nr+nr_batch {
            file.write(format!("{},{},{},{},{}\n", nr, random_characters(fields_length), random_characters(fields_length), random_characters(fields_length), random_characters(fields_length)).as_bytes()).unwrap();
        }
        file.flush().unwrap();
    }
}

fn random_characters(length: i32) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length.try_into().unwrap())
        .map(char::from)
        .collect()
}