use std::env;
use std::io;
use std::io::prelude::*;

extern crate postgres;
use postgres::{Connection, TlsMode};
extern crate chrono;
use chrono::{DateTime, FixedOffset};

#[derive(Debug)]
struct Sample {
    sampled:  DateTime<FixedOffset>,
    meter_id: i32,
    value:    i32
}

fn main() {
    // connect to the db
    let user  = env::var("USER").unwrap();
    let db    = "pwp";
    let table = "power";
    let dburi = ["postgres://", &user, "@%2Frun%2Fpostgresql", "/", &db].concat();
    let conn = Connection::connect(dburi, TlsMode::None).unwrap();

    let last = last_sample(table, &conn);

    let insert = [
        "insert into ",
        &table,
        " (sampled, meter_id, value)",
        " values ($1, $2, $3)"
    ].concat();
    let stmt = conn.prepare(&insert).unwrap();
    let separators : &[char] = &[','];
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let input = line.unwrap();
        let tokens: Vec<&str> = input.split(separators).collect();
        let sample = Sample {
            sampled:  DateTime::parse_from_rfc3339(tokens[0]).unwrap(),
            meter_id: tokens[3].parse::<i32>().unwrap(),
            value:    tokens[7].parse::<i32>().unwrap()
        };
        // skip anything before the most recent sample
        // TODO: sample might have more precision, yielding one error at start
        if sample.sampled >= last.sampled {
            println!("{:?}", sample);
            let _res = stmt.execute(&[&sample.sampled, &sample.meter_id, &sample.value]);
        }
    }
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

