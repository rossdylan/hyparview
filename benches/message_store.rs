use criterion::{black_box, criterion_group, criterion_main, Criterion};

use hyparview::message_store::MessageStore;
use hyparview::{Message, MessageData, Node};

pub fn benchmark_contains(c: &mut Criterion) {
    let m = Message::new(
        Node {
            host: "::1".into(),
            port: 4200,
        },
        MessageData::ForwardJoin {
            node: Node {
                host: "::1".into(),
                port: 6900,
            },
            ttl: 1,
        },
    );
    let ms = MessageStore::new(10_000);
    c.bench_function("contains", |b| b.iter(|| ms.contains(black_box(&m))));
}

fn new_msg() -> Message {
    Message::new(
        Node {
            host: "::1".into(),
            port: 4200,
        },
        MessageData::ForwardJoin {
            node: Node {
                host: "::1".into(),
                port: 6900,
            },
            ttl: 1,
        },
    )
}

pub fn benchmark_insert(c: &mut Criterion) {
    let mut ms = MessageStore::new(10_000);
    c.bench_function("insert", |b| {
        b.iter_batched(
            new_msg,
            |data| ms.insert(black_box(&data)),
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, benchmark_contains, benchmark_insert);
criterion_main!(benches);
