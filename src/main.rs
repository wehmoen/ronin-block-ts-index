use mongodb::bson::{doc, DateTime};
use mongodb::options::{FindOneOptions, IndexOptions, InsertManyOptions};
use mongodb::IndexModel;
use serde::{Deserialize, Serialize};
use web3::types::BlockId;

#[derive(Serialize, Deserialize)]
struct BlockTimestamp {
    number: i64,
    timestamp: DateTime,
}

fn s_to_ms(s: u64) -> i64 {
    (s * 1000) as i64
}

#[tokio::main]
async fn main() {
    let provider = web3::transports::Http::new("http://localhost:8545").unwrap();
    let web3 = web3::Web3::new(provider);

    let client = mongodb::Client::with_uri_str("mongodb://127.0.0.1:27017")
        .await
        .expect("Failed to connect to database!");
    let db = client.database("roninchain");
    let collection = db.collection::<BlockTimestamp>("blocktimes");

    collection
        .create_index(
            IndexModel::builder()
                .keys(doc! {
                    "number": 1
                })
                .options(IndexOptions::builder().unique(true).build())
                .build(),
            None,
        )
        .await
        .ok();

    let mut block_number: i64 = match collection
        .find_one(
            doc! {},
            FindOneOptions::builder().sort(doc! {"number": -1}).build(),
        )
        .await
        .expect("Failed to load data from database")
    {
        Some(res) => res.number + 1,
        None => 1,
    };

    let chain_height = web3
        .eth()
        .block_number()
        .await
        .expect("Failed to parse chain height")
        .as_u64() as i64;

    let mut cache: Vec<BlockTimestamp> = vec![];

    println!("Start: {}\t\tEnd: {}", block_number, chain_height);

    if block_number >= chain_height {
        println!("Exiting. Nothing to do!");
        return;
    }

    loop {
        let block = web3
            .eth()
            .block(BlockId::from(web3::types::U64::from(block_number.clone())))
            .await
            .expect("Failed to load block from chain!")
            .expect("Block not found!");

        let data = BlockTimestamp {
            number: block_number,
            timestamp: DateTime::from_millis(s_to_ms(block.timestamp.as_u64())),
        };

        cache.push(data);

        if cache.len() > 10000 {
            collection
                .insert_many(&cache, InsertManyOptions::builder().ordered(false).build())
                .await
                .expect("Failed to insert blocks into database!");
            cache.clear();
        }

        block_number += 1;

        if block_number > chain_height {
            collection
                .insert_many(&cache, InsertManyOptions::builder().ordered(false).build())
                .await
                .expect("Failed to insert blocks into database!");
            cache.clear();
            break;
        }
    }
}
