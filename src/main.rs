use bitcoin_explorer::{BitcoinDB, FBlock, SBlock, Block, Transaction, FTransaction, STransaction, SConnectedBlock, FConnectedTransaction,Txid, FromHex, Address, ConnectedTx, SConnectedTransaction, STxOut};
use bson::{to_document, Bson};
use core::panic;
use std::path::Path;
use mongodb::{bson::doc, options::{ClientOptions, ServerApi, ServerApiVersion, ResolverConfig}, Collection, Client, Database};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, NaiveDateTime, Utc};
use tokio;

#[derive(Clone, Serialize)]
struct TransactionMG {
    send_ad: Vec<String>,
    rec_ad: Vec<String>,
    amount: String,
    time: String,
    txid: String, 
    block: Bson
}

#[derive(Clone, Serialize)]
struct BlockMG {
    time: String,
    block: Bson
}


#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    // Load the MongoDB connection string from an environment variable:
    // Replace the placeholder with your Atlas connection string
    let uri = "mongodb://localhost:27017";
    let mut client_options =
    ClientOptions::parse(uri)
    .await?;
    let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
    client_options.server_api = Some(server_api);
    let client = Client::with_options(client_options)?;
    let mongo_db = client.database("btc");
    client
    .database("admin")
    .run_command(doc! {"ping": 1}, None)
    .await?;
    println!("Pinged your deployment. You successfully connected to MongoDB!");
    //
    let path = Path::new("J:/bitcoin");
    let db = BitcoinDB::new(path, true).unwrap();
    
    let blocks_collection: Collection<BlockMG> = client.database("btc").collection("blocks");
    let flux_collection: Collection<TransactionMG> = client.database("btc").collection("new_flux");
    let start = 700000;
    let end = 729700;
    
    for (i, block) in db.iter_block::<SBlock>(start, end).enumerate() {
        let actual_index = start + i; // Adiciona o offset
        println!("Processing block num {}", actual_index);
        println!(" block  {:?}", block.header);

        let mut manydocs: Vec<TransactionMG> = Vec::new();
        let timez = DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(block.header.time as i64, 0), Utc).to_rfc3339_opts(chrono::SecondsFormat::Millis, true);                  

        match blocks_collection.insert_one(BlockMG{
            time:timez.clone(),
            block:  Bson::from(actual_index as i64)
        }, None).await {
            Ok(result) => {
                // println!("Documento inserido com sucesso: {:?}", result);
            },
            Err(e) => {
                eprintln!("Erro ao inserir documento: {:?}", e);
                continue;
            },
        }
        for tx in block.txdata {
            let cloned_timez = timez.clone();

            let output = tx.txid;
            let txid = output;
            let output_total: u64 = tx.output.iter().map(|o| o.value).sum();
            if output_total > 10000000 {
                let full_transaction: SConnectedTransaction = db.get_connected_transaction(&txid).unwrap();
                let sender_message: Vec<STxOut> = full_transaction.input;
                let receiver_message : Vec<STxOut> = full_transaction.output;
                let mut send_addresses: Vec<String> = Vec::new();
                let mut rec_addresses: Vec<String> = Vec::new();
                for tx_out in sender_message.iter() {
                    for address in &*tx_out.addresses {
                        // println!("Endereço de envio: {}", address);
                        send_addresses.push(address.to_string());
                    }
                }
                
                for tx_out in receiver_message.iter() {
                    for address in &*tx_out.addresses {
                        // println!("Endereço de recebimento: {}", address);
                        rec_addresses.push(address.to_string());
                    }
                }
                

                manydocs.push(TransactionMG { 
                    send_ad: send_addresses, 
                    rec_ad: rec_addresses, 
                    amount: output_total.to_string(), 
                    time: cloned_timez, 
                    txid: txid.to_string(),
                    block:  Bson::from(actual_index as i64)
                });
            }
        }
        
 
        if !manydocs.is_empty() {
            match flux_collection.insert_many(manydocs, None).await {
                Ok(result) => {
                },
                Err(e) => {
                    eprintln!("Erro ao inserir documento: {:?}", e);
                    continue;
                },
            }
        }
    }
    Ok(())
    
}

