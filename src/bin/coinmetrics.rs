// this use statement gets you access to the lib file
use slurper::*;

use std::time::Duration;

use log::{debug,info};
use std::error::Error;
use self::models::{CoinMetrics,KafkaCryptoTrade,CryptoMarket,CryptoTrade,Trades};
use chrono::{DateTime,Utc};

use chrono::{SecondsFormat};

use futures::future::join_all;

// use mongodb::{Collection};
use mongodb::{Client};
use mongodb::{bson::doc};
// use mongodb::options::{FindOptions};

// use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

use std::collections::HashMap;

extern crate itertools;
use itertools::Itertools;


#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;

use std::str;


use serde::Deserialize;

const MARKETS_URL: &str = "https://community-api.coinmetrics.io/v4/catalog/markets";
const TS_URL: &str = "https://community-api.coinmetrics.io/v4/timeseries/market-trades";




#[derive(Deserialize, Debug)]
struct CMDataMarket {
    #[serde(rename = "market", default)]
    market: String,  
    #[serde(rename = "symbol", default)]
    symbol: String
}

#[derive(Deserialize, Debug)]
struct CMDataWrapperMarkets {
    #[serde(rename = "data", default)]
    cmds: Vec<CMDataMarket>
}



#[derive(Deserialize, Debug, Clone)]
struct CMDataTSMarketTrades {
    time: String,
    market: String,
    coin_metrics_id: String,
    amount: String,
    price: String,
    database_time: String,
    side: Option<String>
}




#[derive(Deserialize, Debug, Clone)]
struct CMDataWrapperTSMarketData {
    #[serde(rename = "data", default)]
    cmds: Vec<CMDataTSMarketTrades>,
    #[serde(rename = "next_page_token", default)]
    next_page_token: String,
    #[serde(rename = "next_page_url", default)]
    next_page_url: String,
}



async fn process_market<'a>(m: String) -> Result<(),Box<dyn Error>> {

    let market = CryptoMarket {market: &m};

    let mut max_market_date_hm = HashMap::new();

    let client = Client::with_uri_str(LOCAL_MONGO).await?;
    let database = client.database(THE_DATABASE);
    let collection = database.collection::<CoinMetrics>(THE_COINMETRICS_COLLECTION);

    let broker = "localhost:9092";
    let topic = "coinmetrics-markets";

    let mut producer = Producer::from_hosts(vec![broker.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    println!("{}", market);
    let big_bang = market.get_last_updated_trade(&collection).await?;
    let gtedate = big_bang.to_rfc3339_opts(SecondsFormat::Secs, true);
    let mut ts_url1 = format!("{}?start_time={}&paging_from=start&markets={}&page_size=1000", TS_URL,gtedate,market.market);

    while ts_url1 != "" {
        let payload_ts = get_ts_market_trades(&ts_url1).await?;
        for cmditem in payload_ts.cmds {
            let trade_date = DateTime::parse_from_rfc3339(&cmditem.time).unwrap();
            if trade_date > big_bang { // this is redundant I think to the above gtedate

                let kafka_trade_date = DateTime::parse_from_rfc3339(&cmditem.time).unwrap();
                
                let new_kct = KafkaCryptoTrade {
                    trade_date: &kafka_trade_date.with_timezone(&Utc).to_rfc3339_opts(SecondsFormat::Secs, true),
                    coin_metrics_id: &cmditem.coin_metrics_id,
                    price: cmditem.price.parse::<f64>().unwrap(),
                    quantity: cmditem.amount.parse::<f64>().unwrap(),
                    market: &cmditem.market,
                    tx_type: &cmditem.side.unwrap_or("SHIT NOT PROVIDED".to_string()),
                };

                println!("{}", new_kct);
                 let data = serde_json::to_string(&new_kct).expect("json serialization failed");
                 let data_as_bytes = data.as_bytes();

                producer.send(&Record {
                    topic,
                    partition: -1,
                    key: (),
                    value: data_as_bytes,
                })?;
                max_market_date_hm.entry(cmditem.market.clone()).or_insert(cmditem.time.clone());


            }
        }
        debug!("npt {:?}", payload_ts.next_page_token);
        debug!("npu {:?}", payload_ts.next_page_url);
        ts_url1 = payload_ts.next_page_url;
    }


    for (k,v) in max_market_date_hm {
        let trade_date = DateTime::parse_from_rfc3339(&v).unwrap();
        let new_cm = CoinMetrics {
            last_known_trade_date: trade_date.with_timezone(&Utc),
            market: k,
        };
        println!("{}", new_cm);
        let _result = collection.insert_one(new_cm, None).await?;                                    
    }



    Ok(())

}


async fn get_ts_market_trades<'a>(whole_url: &'a str) -> Result<CMDataWrapperTSMarketData, Box<dyn Error>> {

    let request_url_ts = format!("{}", whole_url);
    let response_ts = reqwest::get(&request_url_ts).await?;
    let payload_ts: CMDataWrapperTSMarketData = response_ts.json().await?;

    Ok(payload_ts)

}



async fn get_all_coinmetrics_markets() -> Result<Vec<String>, Box<dyn Error>> {

    let mut rvec = Vec::new();

    let request_url = format!("{}", MARKETS_URL);
    let response = reqwest::get(&request_url).await?;
    let payload: CMDataWrapperMarkets = response.json().await?;

    for cmditem in payload.cmds {
        let market = cmditem.market.clone();
        rvec.push(market);
    }

    Ok(rvec)

}


async fn get_hotlist() -> Result<Vec<String>, Box<dyn Error>> {

    let mut hotlist = Vec::new();

    let request_url = format!("{}", MARKETS_URL);
    let response = reqwest::get(&request_url).await?;
    let payload: CMDataWrapperMarkets = response.json().await?;

    for cmditem in payload.cmds {
        let market = cmditem.market.clone();
        let this_market = CryptoMarket {market: &market};

        if this_market.is_hotlist() {
            hotlist.push(market);        
        }
    }

    Ok(hotlist)

}

async fn get_kraken() -> Result<Vec<String>, Box<dyn Error>> {

    let mut hotlist = Vec::new();

    let request_url = format!("{}", MARKETS_URL);
    let response = reqwest::get(&request_url).await?;
    let payload: CMDataWrapperMarkets = response.json().await?;

    for cmditem in payload.cmds {
        let market = cmditem.market.clone();
        let this_market = CryptoMarket {market: &market};

        if this_market.is_kraken() {
            hotlist.push(market);        
        }
    }

    Ok(hotlist)

}




#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 


    let yaml = load_yaml!("../coinmetrics.yml");
    let matches = App::from_yaml(yaml).get_matches();
    info!("processing on directive input: {}", matches.value_of("INPUT").unwrap());

    match matches.value_of("INPUT").unwrap() {


        "hotlist" => {

            let client = Client::with_uri_str(LOCAL_MONGO).await?;
            let database = client.database(THE_DATABASE);
            let collection = database.collection::<CoinMetrics>(THE_COINMETRICS_COLLECTION);

            let all_markets = get_hotlist().await?;
            for m in all_markets {
                let this_market = CryptoMarket {market: &m};
                let last_updated = this_market.get_last_updated_trade(&collection).await?;
                println!("{} {}", this_market, last_updated);

            }

        },

        "markets" => {

            let all_markets = get_all_coinmetrics_markets().await?;
            for m in all_markets {
                println!("{}", m);
            }

        },

        "consumer-for-mongo" => {

            let client = Client::with_uri_str(LOCAL_MONGO).await?;
            let database = client.database(THE_DATABASE);
            let collection = database.collection::<CryptoTrade>(THE_CRYPTO_TRADES_COLLECTION);

            let broker = "localhost:9092";
            let brokers = vec![broker.to_owned()];
            let topic = "coinmetrics-markets";


            let mut con = Consumer::from_hosts(brokers)
                .with_topic(topic.to_string())
//                .with_group(group)
                .with_fallback_offset(FetchOffset::Earliest)
                .with_offset_storage(GroupOffsetStorage::Kafka)
                .create()?;


            loop {

                let mss = con.poll()?;
                if mss.is_empty() {
                    println!("No messages available right now.");
                    return Ok(());
                }

                for ms in mss.iter() {
                    for m in ms.messages() {
                        let mut buf = Vec::with_capacity(1024);
                        //println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, m.value);
                        buf.extend_from_slice(m.value);
                        // buf.push(b'\n');                        
                        let cb =  str::from_utf8(&buf).unwrap();
                        let des_kct: KafkaCryptoTrade = serde_json::from_str(cb).unwrap();
                        println!("{}", des_kct);
                        let mongo_crypto_trade = des_kct.get_crypto_trade_for_mongo().unwrap();
                        println!("{}", mongo_crypto_trade);                        
                        let _result = collection.insert_one(mongo_crypto_trade, None).await?;                                    
                    }
                    let _ = con.consume_messageset(ms);
                }
                con.commit_consumed()?;
            }   

        },




        "ts-append-hotlist" => {

            let all_markets = get_hotlist().await?;

            for chunk in &all_markets.into_iter().chunks(10) {
                let subset_of_markets = chunk.collect::<Vec<_>>();
                debug!("Processing the following subset of coinmetrics markets: {:?}", &subset_of_markets);
                let dcol: Vec<_> = subset_of_markets.into_iter().map(|n| process_market(n)).collect();
                let _rdvec = join_all(dcol).await;
            }


        },

        "ts-append-kraken" => {

            let all_markets = get_kraken().await?;

            for chunk in &all_markets.into_iter().chunks(10) {
                let subset_of_markets = chunk.collect::<Vec<_>>();
                debug!("Processing the following subset of coinmetrics markets: {:?}", &subset_of_markets);
                let dcol: Vec<_> = subset_of_markets.into_iter().map(|n| process_market(n)).collect();
                let _rdvec = join_all(dcol).await;
            }


        },
        _ => {

            debug!("Unrecognized input parm.");
        }


    }




















    Ok(())

}







#[cfg(test)]
mod tests {
    use std::{fs};
//    use std::fs::File;

    #[test]
    fn it_works_test_file_exists() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn it_works_can_open_close_file() {
        assert_eq!(3 + 3, 6);
    }


}












