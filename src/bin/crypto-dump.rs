// this use statement gets you access to the lib file
use tlplay::*;

use log::{info,debug,warn,error};
use std::error::Error;
use self::models::{CryptoTrade,CryptoTradeOpt,CryptoLiquidation,CryptoTradez,CryptoCluster,CryptoMarket,ImpactItem,ADItem,CapSuite,Trades,TimeRange};

use linfa::traits::Predict;
use linfa::DatasetBase;
use linfa_clustering::{KMeans};

use ndarray::Array;
use ndarray::{Axis, array};
use ndarray_rand::rand::SeedableRng;
use rand_isaac::Isaac64Rng;


use futures::stream::TryStreamExt;
use mongodb::{Collection};
use mongodb::{Client};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};

use futures::join;

use std::iter::FromIterator;
use std::collections::HashMap;

use std::fmt; // Import `fmt`

#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;
#[macro_use]
extern crate serde_derive;

extern crate csv;
use csv::Writer;

// use serde::Deserialize;
//use reqwest::Error;

#[derive(Debug, Hash, Eq, Ord, PartialEq, PartialOrd, Clone)]
struct DDHM {
    dom: i64,
    dow: i64,
    hid: i64,
    market: String
}


#[derive(Debug, PartialEq, PartialOrd, Clone)]
struct HourlyTradeSummary {
    ddh: DDHM,
    cnt: i64,
    net_amount: f64
}

impl fmt::Display for HourlyTradeSummary {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<35} {:>2} {:>2} {:>2} {:>9} {:>15.4}", self.ddh.market, self.ddh.dom, self.ddh.dow, self.ddh.hid, self.cnt, self.net_amount)
    }
}




#[derive(Debug, Hash, Eq, Ord, PartialEq, PartialOrd, Clone)]
struct MKTX {
    market: String,
    tx_type: String,
}

#[derive(Debug, Hash, Eq, Ord, PartialEq, PartialOrd, Clone)]
struct EXTX {
    exchange: String,
    instrument_type: String,
    tx_type: String,
}


#[derive(Debug, Serialize)]
struct MarketAccumDist {
    #[serde(rename(serialize = "Market"))]
    market: String,
    #[serde(rename(serialize = "TX"))]
    tx_type: String,
    #[serde(rename(serialize = "Net"))]
    net: f64
}



const LOOKBACK: i64 = 1000;
const LOOKAHEAD: i64 = 1000;
// const LOOKBACK: i64 = 3000;
// const LOOKAHEAD: i64 = 3000;
// const LOOKBACK: i64 = 60000;
// const LOOKAHEAD: i64 = 60000;





#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 

    let yaml = load_yaml!("../coinmetrics.yml");
    let matches = App::from_yaml(yaml).get_matches();

    // Calling .unwrap() is safe here because "INPUT" is required (if "INPUT" wasn't
    // required we could have used an 'if let' to conditionally get the value)
    info!("processing on directive input: {}", matches.value_of("INPUT").unwrap());

    let client = Client::with_uri_str(LOCAL_MONGO).await?;
    let database = client.database(THE_DATABASE);
    let collection = database.collection::<CryptoTrade>(THE_CRYPTO_COLLECTION);
    let ocollection = database.collection::<CryptoTradeOpt>(THE_CRYPTO_OPT_COLLECTION);
    let lcollection = database.collection::<CryptoLiquidation>(THE_CRYPTO_LIQUIDATION_COLLECTION);
    let zcollection = database.collection::<CryptoTradez>(THE_CRYPTOZ_COLLECTION);
    let zccollection = database.collection::<CryptoCluster>(THE_CRYPTOCLUSTER_COLLECTION);
    let capcollection = database.collection::<CapSuite>(THE_CRYPTO_CAP_SUITE_COLLECTION);

    // let time_ranges = get_time_ranges("2021-09-13 00:00:00","2021-09-14 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
    let time_ranges = get_time_ranges("2021-09-14 00:00:00","2021-09-15 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();


    match matches.value_of("INPUT").unwrap() {



        "zscores" => {

            let mut wtrimpactn = Writer::from_path("/tmp/netdiffs-spot.csv")?;
            let mut wtrimpactf = Writer::from_path("/tmp/netdiffs-future.csv")?;
            let mut wtrimpactne = Writer::from_path("/tmp/exchange-netdiffs-spot.csv")?;
            let mut wtrimpactfe = Writer::from_path("/tmp/exchange-netdiffs-future.csv")?;

            let mut market_tx_net_difference = HashMap::new();
            let mut exchange_tx_net_difference = HashMap::new();

            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
                let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}, "tx_type": {"$ne": "NOT PROVIDED"} };
                let find_options = FindOptions::builder().build();
                let mut cursor = zcollection.find(filter, find_options).await?;
                while let Some(trade) = cursor.try_next().await? {

                    let market = CryptoMarket {
                        market: trade.market.clone()
                    };

                    let mktx =  MKTX {
                        market: trade.market.clone(),
                        tx_type: trade.tx_type.clone()
                    };

                    let extx = EXTX {
                        exchange: market.just_exchange().unwrap().to_string().clone(),
                        instrument_type: market.drop_all_but_instrument_type().unwrap().to_string().clone(),
                        tx_type: trade.tx_type.clone()
                    };

                    let net_difference = trade.get_difference().unwrap();
                    market_tx_net_difference.entry(mktx.clone()).and_modify(|e| { *e += net_difference}).or_insert(net_difference);
                    exchange_tx_net_difference.entry(extx.clone()).and_modify(|e| { *e += net_difference}).or_insert(net_difference);

                }

                let mut kmtnd = Vec::from_iter(market_tx_net_difference.keys());
                kmtnd.sort();

                let mut ketnd = Vec::from_iter(exchange_tx_net_difference.keys());
                ketnd.sort();


                for k in kmtnd {
                    let new_impact_item = ImpactItem {
                        gtedate: tr.gtedate,
                        ltdate: tr.ltdate,
                        lookback: LOOKBACK,
                        lookahead: LOOKAHEAD,
                        aggregation_thing: k.market.clone(),
                        tx_type: k.tx_type.clone(),
                        net_difference: market_tx_net_difference[k],
                    };
                    println!("{}", new_impact_item);
                    if k.market.contains("spot") {
                        wtrimpactn.serialize(new_impact_item.get_csv().unwrap())?;                                                            
                    } else {
                        wtrimpactf.serialize(new_impact_item.get_csv().unwrap())?;                                                            
                    }
                }


                for k in ketnd {
                    let new_impact_item = ImpactItem {
                        gtedate: tr.gtedate,
                        ltdate: tr.ltdate,
                        lookback: LOOKBACK,
                        lookahead: LOOKAHEAD,
                        aggregation_thing: k.exchange.clone(),
                        tx_type: k.tx_type.clone(),
                        net_difference: exchange_tx_net_difference[k],
                    };
                    println!("{}", new_impact_item);
                    if k.instrument_type == "spot" {
                        wtrimpactne.serialize(new_impact_item.get_csv().unwrap())?;                                                            
                    } else {
                        wtrimpactfe.serialize(new_impact_item.get_csv().unwrap())?;                                                            
                    }
                }

                wtrimpactn.flush()?;
                wtrimpactf.flush()?;
                wtrimpactne.flush()?;
                wtrimpactfe.flush()?;

            }
        },



        "dump-cap-suite" => {

            let mut wtr = Writer::from_path("/tmp/capsuite.csv")?;
            let filter = doc! {"asset": "ETH" };
            let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
            let mut cursor = capcollection.find(filter, find_options).await?;
            while let Some(cs) = cursor.try_next().await? {
                wtr.serialize(cs.get_csv().unwrap())?;                       
                println!("{}",cs);             
            }
            wtr.flush()?;

        }


        "dump-binance-cluster-spots" => {

            let mut wtrcls = Writer::from_path("/tmp/binance-clusters-spot.csv")?;

            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
                let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}, "exchange": "binance", "instrument_type": "spot"};
//                let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
                    let find_options = FindOptions::builder().build();
                let mut cursor = zccollection.find(filter, find_options).await?;
                while let Some(trade) = cursor.try_next().await? {
                    wtrcls.serialize(trade.get_csv().unwrap())?;                                    
                }
                wtrcls.flush()?;
            }
        },


        "dump-ftx-cluster-spots" => {

            let mut wtrcls = Writer::from_path("/tmp/ftx-clusters-spot.csv")?;

            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
                let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}, "exchange": "ftx", "instrument_type": "spot"};
//                let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
                    let find_options = FindOptions::builder().build();
                let mut cursor = zccollection.find(filter, find_options).await?;
                while let Some(trade) = cursor.try_next().await? {
                    wtrcls.serialize(trade.get_csv().unwrap())?;                                    
                }
                wtrcls.flush()?;
            }
        },

        "dump-coinbase-cluster-spots" => {

            let mut wtrcls = Writer::from_path("/tmp/coinbase-clusters-spot.csv")?;

            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
                let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}, "exchange": "coinbase", "instrument_type": "spot"};
//                let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
                    let find_options = FindOptions::builder().build();
                let mut cursor = zccollection.find(filter, find_options).await?;
                while let Some(trade) = cursor.try_next().await? {
                    wtrcls.serialize(trade.get_csv().unwrap())?;                                    
                }
                wtrcls.flush()?;
            }
        },

        "dump-gemini-cluster-spots" => {

            let mut wtrcls = Writer::from_path("/tmp/gemini-clusters-spot.csv")?;

            for tr in time_ranges{
                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
                let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}, "exchange": "gemini", "instrument_type": "spot"};
//                let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
                    let find_options = FindOptions::builder().build();
                let mut cursor = zccollection.find(filter, find_options).await?;
                while let Some(trade) = cursor.try_next().await? {
                    wtrcls.serialize(trade.get_csv().unwrap())?;                                    
                }
                wtrcls.flush()?;
            }
        },


        _ => {
            debug!("i am tbd");
        }
    };

    debug!("i am done");


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












