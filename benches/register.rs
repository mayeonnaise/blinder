use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::{criterion_group, criterion_main};

use reqwest::blocking::{Client, ClientBuilder};
use serde::Deserialize;
use serde::Serialize;

static BLINKER_REGISTER_QUERY_URL: &str = "http://localhost:8000/register_query";
static LUCENE_REGISTER_QUERY_URL: &str = "http://localhost:8080/register_query";
static BLINKER_MATCH_QUERY_URL: &str = "http://localhost:8000/match_document";
static LUCENE_MATCH_QUERY_URL: &str = "http://localhost:8080/match_document";

static QUERY1: &str = "body:barack OR body:biden";
static QUERY2: &str = "body:barack OR body:clinton";
static QUERY3: &str = "body:barack OR body:roosevelt";
static QUERY4: &str = "body:barack OR body:clinton OR body:biden";
static QUERY5: &str = "body:barack OR body:bloomberg OR body:biden";
static QUERY6: &str = "body:barack OR body:trump";

#[derive(Serialize)]
pub struct SimpleMonitorQuery {
    pub id: u64,
    pub query: String,
}

#[derive(Serialize)]
pub struct SimpleDocument {
    pub body: String
}

#[derive(Debug, Deserialize)]
pub struct QueryMatches {
    pub matches: Vec<String>
}

fn register_query(client: &Client, url: &'static str, query: &'static str) {
    let query = SimpleMonitorQuery {
        id: 0,
        query: query.to_string(),
    };
    let _ = client.post(url).json(&query).send();
}

fn match_document(client: &Client, url: &'static str) {
    let document = SimpleDocument {
        body: "Quite so! You have not observed. And yet you have seen. That is just
        my point. Now, I know that there are seventeen steps, because I have
        both seen and observed. By-the-way, since you are interested in these
        little problems, and since you are good enough to chronicle one or
        two of my trifling experiences, you may be interested in this. He
        threw over a sheet of thick, pink-tinted note-paper which had been
        lying open upon the table. Donald Trump.".to_string()
    };
    let response = client.post(url).json(&document).send().unwrap().text().unwrap();
    // dbg!(response);
}

fn register_benchmark(c: &mut Criterion) {
    let client = ClientBuilder::new().no_proxy().build().unwrap();
    let mut group = c.benchmark_group("register");

    group.bench_with_input(
        BenchmarkId::new("blinker", ""),
        &client,
        |b, client| {
            b.iter(|| register_query(&client, BLINKER_REGISTER_QUERY_URL, QUERY1))
        },
    );

    group.bench_with_input(
        BenchmarkId::new("lucene", ""),
        &client,
        |b, client| {
            b.iter(|| register_query(&client, LUCENE_REGISTER_QUERY_URL, QUERY2))
        },
    );
}

fn match_benchmark(c: &mut Criterion) {
    let client = ClientBuilder::new().no_proxy().build().unwrap();
    let mut group = c.benchmark_group("match");

    register_query(&client, BLINKER_REGISTER_QUERY_URL, QUERY1);
    register_query(&client, BLINKER_REGISTER_QUERY_URL, QUERY2);
    register_query(&client, BLINKER_REGISTER_QUERY_URL, QUERY3);
    register_query(&client, BLINKER_REGISTER_QUERY_URL, QUERY4);
    register_query(&client, BLINKER_REGISTER_QUERY_URL, QUERY5);
    register_query(&client, BLINKER_REGISTER_QUERY_URL, QUERY6);

    register_query(&client, LUCENE_REGISTER_QUERY_URL, QUERY1);
    register_query(&client, LUCENE_REGISTER_QUERY_URL, QUERY2);
    register_query(&client, LUCENE_REGISTER_QUERY_URL, QUERY3);
    register_query(&client, LUCENE_REGISTER_QUERY_URL, QUERY4);
    register_query(&client, LUCENE_REGISTER_QUERY_URL, QUERY5);
    register_query(&client, LUCENE_REGISTER_QUERY_URL, QUERY6);

    group.bench_with_input(
        BenchmarkId::new("blinker", ""),
        &client,
        |b, client| {
            b.iter(|| match_document(&client, BLINKER_MATCH_QUERY_URL))
        },
    );

    group.bench_with_input(
        BenchmarkId::new("lucene", ""),
        &client,
        |b, client| {
            b.iter(|| match_document(&client, LUCENE_MATCH_QUERY_URL))
        },
    );
}

// criterion_group!(benches, register_benchmark, match_benchmark);
criterion_group!(benches, match_benchmark);
criterion_main!(benches);
