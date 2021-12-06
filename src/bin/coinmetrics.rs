// this use statement gets you access to the lib file
use slurper::*;

use std::time::Duration;

use log::{debug,info,warn,error};
use std::error::Error;
use self::models::{SourceThingLastUpdate,KafkaCryptoTrade,CryptoMarket,CryptoTrade,TimeRange, MarketSummary, RangeBoundMarketSummary};
use chrono::{DateTime,Utc};

use chrono::{SecondsFormat};

use futures::future::join_all;

use mongodb::{Collection};
use mongodb::{Client};
use mongodb::{bson::doc};
use futures::stream::{StreamExt}; // this gets you next in aggregation cursor

// use futures::stream::TryStreamExt;
// use mongodb::options::{FindOptions};



// use kafka::error::Error as KafkaError;
use kafka::producer::{Producer, Record, RequiredAcks};
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};

//use std::iter::FromIterator;
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
    let collection = database.collection::<SourceThingLastUpdate>(THE_SOURCE_THING_LAST_UPDATE_COLLECTION);

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
        let new_cm = SourceThingLastUpdate {
            last_known_trade_date: trade_date.with_timezone(&Utc),
            source: "coinmetrics".to_string(),
            thing: k,
            thing_description: "market".to_string()
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


async fn agg_rbms<'a, T>(description_surname: &'a str, tr: &TimeRange, collection: &Collection<T>) -> Result<Vec<RangeBoundMarketSummary>, Box<dyn Error>> {

    let description = format!("{} {}", (tr.ltdate - tr.gtedate).num_minutes(), description_surname);

    let mut rvec = Vec::new();    
    let filter = doc! {"$match": {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}}};
    let stage_group_market = doc! {"$group": {"_id": "$market", 
    "cnt": { "$sum": 1 }, "qty": { "$sum": "$quantity" }, 
    "std": { "$stdDevPop": "$price" }, 
    "na": { "$sum": {"$multiply": ["$price","$quantity"]}}, }};
    let pipeline = vec![filter, stage_group_market];

    let mut results = collection.aggregate(pipeline, None).await?;
    while let Some(result) = results.next().await {
       let doc: MarketSummary = bson::from_document(result?)?;
       let rbdoc = RangeBoundMarketSummary {
        gtedate: tr.gtedate,
        ltdate: tr.ltdate,
        description: description.clone(),
        market_summary: doc
       };
       rvec.push(rbdoc);
    }

    Ok(rvec)

}


// async fn agg_rbes<'a>(tr: &TimeRange, collection: &Collection<CryptoTrade>) -> Result<Vec<RangeBoundExchangeSummary>, Box<dyn Error>> {

//     let description = format!("{} {}", (tr.ltdate - tr.gtedate).num_minutes(), "Trade Count");

//     let mut rvec = Vec::new();    
//     let filter = doc! {"$match": {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}}};
//     let stage_group_market = doc! {"$group": {"_id": {"trade_llama_exchange": "$trade_llama_exchange", "trade_llama_instrument_type":"$trade_llama_instrument_type", "tx_type":"$tx_type"}, 
//     "cnt": { "$sum": 1 }, "na": { "$sum": {"$multiply": ["$price","$quantity"]}}, }};
//     let pipeline = vec![filter, stage_group_market];

//     let mut results = collection.aggregate(pipeline, None).await?;
//     while let Some(result) = results.next().await {
//        let doc: ExchangeSummary = bson::from_document(result?)?;
//        let rbdoc = RangeBoundExchangeSummary {
//         gtedate: tr.gtedate,
//         ltdate: tr.ltdate,
//         description: description.clone(),
//         exchange_summary: doc
//        };
//        rvec.push(rbdoc);
//     }

//     Ok(rvec)

// }





#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 


    let yaml = load_yaml!("../cmds.yml");
    let matches = App::from_yaml(yaml).get_matches();
    info!("processing on directive input: {}", matches.value_of("INPUT").unwrap());

    match matches.value_of("INPUT").unwrap() {


        "hotlist" => {

            let client = Client::with_uri_str(LOCAL_MONGO).await?;
            let database = client.database(THE_DATABASE);
            let collection = database.collection::<SourceThingLastUpdate>(THE_SOURCE_THING_LAST_UPDATE_COLLECTION);

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



        "cluster-crypto-trades" => {

            let client = Client::with_uri_str(LOCAL_MONGO).await?;
            let database = client.database(THE_DATABASE);
            let collection = database.collection::<CryptoTrade>(THE_CRYPTO_TRADES_COLLECTION);

            let time_ranges = utils::get_time_ranges("2021-10-27 00:00:00","2021-10-28 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap(); 

            for otr in &time_ranges{
                let hourlies = otr.get_hourlies().unwrap();
                assert_eq!(hourlies.len(),24);

                // warn!("this delete concurrency run does not specify description, so both trade and liquidation summs are deleted");
                // let dcol: Vec<_> = (0..24).map(|n| hourlies[n].delete_exact_range(&rbmscollection)).collect();
                // let _rdvec = join_all(dcol).await;

                let hcol: Vec<_> = (0..24).map(|n| agg_rbms("Trade Count", &hourlies[n],&collection)).collect();
                let rvec = join_all(hcol).await;

                debug!("first trades");
                for (idx, r) in rvec.iter().enumerate() {
                    match r {
                        Ok(aggsv) => {
                            for aggs in aggsv {
                                println!("{}", aggs);
//                                let _result = rbmscollection.insert_one(aggs, None).await?;                                                                
                            }
                        },
                        Err(error) => {
                            error!("Hour {:?} Error: {:?}", idx+1,  error);
                            panic!("We choose to no longer live.")                            
                        }
                    }
                }
            }

        },


        "consumer-for-mongo" => {

            let client = Client::with_uri_str(LOCAL_MONGO).await?;
            let database = client.database(THE_DATABASE);
            let collection = database.collection::<CryptoTrade>(THE_CRYPTO_TRADES_COLLECTION);

            let broker = "localhost:9092";
            let brokers = vec![broker.to_owned()];
            let topic = "coinmetrics-markets";


            // this is a hack, for prep on a kraken meeting
            let kraken_markets = get_kraken().await?;
            let mut max_market_date_hm = HashMap::new();
            for km in kraken_markets {
                let market = CryptoMarket {
                    market: &km
                };
                let last_updated = market.get_last_migrated_trade(&collection).await?;
                max_market_date_hm.entry(km.clone()).or_insert(last_updated);                
            }

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
                        if mongo_crypto_trade.trade_date > max_market_date_hm[&mongo_crypto_trade.market] {
                            let _result = collection.insert_one(mongo_crypto_trade, None).await?;                                                            
                        } else {
                            warn!("we have seen this trade before - SKIPPING");
                        }
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
//    use std::{fs};
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












