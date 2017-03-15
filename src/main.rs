use std::env;
use std::io;
use std::io::prelude::*;

extern crate chrono;
use chrono::{DateTime, FixedOffset};
extern crate postgres;
use postgres::{Connection, TlsMode};

#[derive(Debug)]
struct Sample {
    sampled:  DateTime<FixedOffset>,
    meter_id: i32,
    value:    i32
}

const WATER: &'static str = "water";
const POWER: &'static str = "power";

fn main() {
    let args: Vec<String> = env::args().collect();
    // default to power, but allow override to water
    let mut table = POWER;
    let mut meter_idx: usize = 3;
    let mut value_idx: usize = 7;
    if args.len() > 1 {
        if args[1] == WATER {
            table = WATER;
            meter_idx = 6;
            value_idx = 7;
        }
    }

    // connect to the db
    let mut conn = db_connect( "pwp" );

    let last = last_sample(table, &conn);
    println!("{:?}", last);
    let insert = [
        "insert into ",
        table,
        " (sampled, meter_id, value)",
        " values ($1, $2, $3)"
    ].concat();
    let separators : &[char] = &[','];
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let input = line.unwrap();
        let tokens: Vec<&str> = input.split(separators).collect();
        let sample = Sample {
            sampled:  DateTime::parse_from_rfc3339(tokens[0]).unwrap(),
            meter_id: tokens[meter_idx].parse::<i32>().unwrap(),
            value:    tokens[value_idx].parse::<i32>().unwrap()
        };
        // skip anything before the most recent sample
        // TODO: sample might have more precision, yielding one error at start
        if sample.sampled >= last.sampled {
            println!("{:?}", sample);
            let _res = match conn.execute(&insert, &[&sample.sampled, &sample.meter_id, &sample.value]) {
                Ok(res)  => res,
                Err(why) => {
                    println!("{}", why);
                    conn = db_connect( "pwp" );
                    let _foo = conn.execute(&insert, &[&sample.sampled, &sample.meter_id, &sample.value]);
                    continue;
                },
            };
        }
    }
}

fn db_connect ( db: &str ) -> postgres::Connection {
    println!("connecting to {}", db);
    let user  = env::var("USER").unwrap();
    let db    = "pwp";
    let dburi = ["postgres://", &user, "@%2Frun%2Fpostgresql", "/", &db].concat();
    let conn = Connection::connect(dburi, TlsMode::None).unwrap();
    return conn;
}

fn last_sample (table: &str, conn: &postgres::Connection) -> Sample {
    // find the most recent sample
    let query = [
        "select sampled, meter_id, value from ",
        table,
        " order by sampled desc limit 1"
    ].concat();
    let rs = conn.query(&query, &[]).unwrap();
    let row =  rs.iter().next().unwrap();
    let sample = Sample {
        sampled: row.get(0),
        meter_id: row.get(1),
        value: row.get(2),
    };
    return sample;
}

