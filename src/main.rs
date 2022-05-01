use ysql_bench::run;
use structopt::StructOpt;

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
    /// operations (copy,insert,procedure)
    #[structopt(short, long, default_value = "copy,insert,procedure")]
    operations: String,
    /// number of tablets
    #[structopt(long, default_value = "3")]
    tablets: i32,
    /// do NOT use prepared statements
    #[structopt(long)]
    no_prepared: bool,
}

fn main() {
    let options = Opts::from_args();
    let cacert_file = &options.cacert_file as &str;
    let text_fields_length = options.fields_length as i32;
    let batch_size = options.batch_size as i32;
    let rows = options.rows as i32;
    let threads = options.threads as i32;
    let print_histogram = options.print_histogram as bool;
    let nontransactional = options.nontransactional as bool;
    let show_rowsize = options.show_rowsize as bool;
    let operations = &options.operations as &str;
    let tablets = options.tablets as i32;
    let no_prepared = options.no_prepared as bool;
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
    );
}
