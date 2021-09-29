// this use statement gets you access to the lib file
use slurper::*;

use log::{info,debug,warn,error};
use std::error::Error;
use self::models::{CryptoTrade,CryptoLiquidation,Trades,TimeRange,RangeBoundLiquidationCluster,RangeBoundMarketSummary,MarketSummary,RangeBoundExchangeSummary,ExchangeSummary};



use futures::stream::TryStreamExt;
use mongodb::{Collection};
use mongodb::{Client};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};


use futures::join;
use futures::future::join_all;
use futures::stream::{self, StreamExt}; // this gets you next in aggregation cursor


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


// const LOOKBACK: i64 = 1000;
// const LOOKAHEAD: i64 = 1000;
// const LOOKBACK: i64 = 3000;
// const LOOKAHEAD: i64 = 3000;
// const LOOKBACK: i64 = 60000;
// const LOOKAHEAD: i64 = 60000;



// pub fn do_duo_kmeans<'a>(v: Vec<f64>, vct: Vec<CryptoTradez>) -> Vec<CryptoCluster> {

//     let rng = Isaac64Rng::seed_from_u64(42);
//     let expected_centroids = array![[1000., 4.], [10000., 3.], [100000., 2.], [1000000., 1.],];
// //    let n = 10000;
//     let zdataset =  Array::from_shape_vec((v.len() / 2, 2), v.to_vec()).unwrap();
//     let dataset = DatasetBase::from(zdataset);
//     let n_clusters = expected_centroids.len_of(Axis(0));
//     let model = KMeans::params_with_rng(n_clusters, rng)
//         .max_n_iterations(200)
//         .tolerance(1e-5)
//         .fit(&dataset)
//         .expect("KMeans fitted");
//     let dataset = model.predict(dataset);
//     let DatasetBase {
//         records, targets, ..
//     } = dataset;

//     let mut rvec = Vec::new();


//     for (idx, _ags) in vct.iter().enumerate() {
//         let k = &targets[idx];

//         let market = CryptoMarket {
//             market: vct[idx].market.clone() 
//         };

//         let new_cc = CryptoCluster {
//             lookback: LOOKBACK,
//             lookahead: LOOKAHEAD,
//             trade_date: vct[idx].trade_date,
//             price: vct[idx].price,
//             wm_price: vct[idx].wm_price,
//             quantity: vct[idx].quantity,
//             market: vct[idx].market.clone(),
//             exchange: market.just_exchange().unwrap().to_string().clone(),
//             instrument_type: market.drop_all_but_instrument_type().unwrap().to_string().clone(),
//             tx_type: vct[idx].tx_type.clone(),
//             z_score: vct[idx].z_score.unwrap_or(0.00),
//             cluster: *k as i32         
//         };
//         rvec.push(new_cc.clone());
//     }

//     rvec

// }




// async fn sum_market_in_rust<'a>(tr: &TimeRange, collection: &Collection<CryptoTrade>) -> Result<(i32,f64), Box<dyn Error>> {


//     let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate} };
// //    let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build(); // cannot sort on such big entries
//     let find_options = FindOptions::builder().build();
//     let mut cursor = collection.find(filter, find_options).await?;

//     let mut trade_count = 0;
//     let mut net_amount = 0.;

//     while let Some(trade) = cursor.try_next().await? {
//         trade_count+=1;
//         net_amount+=trade.get_net().unwrap_or(0.00);
//     }

//     debug!("{:?} {:?} {:?}", tr,trade_count,net_amount);
//     Ok((trade_count,net_amount))

// }



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


async fn agg_rbes<'a>(tr: &TimeRange, collection: &Collection<CryptoTrade>) -> Result<Vec<RangeBoundExchangeSummary>, Box<dyn Error>> {

    let description = format!("{} {}", (tr.ltdate - tr.gtedate).num_minutes(), "Trade Count");

    let mut rvec = Vec::new();    
    let filter = doc! {"$match": {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}}};
    let stage_group_market = doc! {"$group": {"_id": {"trade_llama_exchange": "$trade_llama_exchange", "trade_llama_instrument_type":"$trade_llama_instrument_type", "tx_type":"$tx_type"}, 
    "cnt": { "$sum": 1 }, "na": { "$sum": {"$multiply": ["$price","$quantity"]}}, }};
    let pipeline = vec![filter, stage_group_market];

    let mut results = collection.aggregate(pipeline, None).await?;
    while let Some(result) = results.next().await {
       let doc: ExchangeSummary = bson::from_document(result?)?;
       let rbdoc = RangeBoundExchangeSummary {
        gtedate: tr.gtedate,
        ltdate: tr.ltdate,
        description: description.clone(),
        exchange_summary: doc
       };
       rvec.push(rbdoc);
    }

    Ok(rvec)

}



















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
    let ccollection = database.collection::<RangeBoundLiquidationCluster>(THE_CRYPTOCLUSTER_COLLECTION);
    let lcollection = database.collection::<CryptoLiquidation>(THE_CRYPTO_LIQUIDATION_COLLECTION);
    let rbmscollection = database.collection::<RangeBoundMarketSummary>(THE_CRYPTO_RBMS_COLLECTION);
    let rbescollection = database.collection::<RangeBoundExchangeSummary>(THE_CRYPTO_RBES_COLLECTION);

    // let time_ranges = get_time_ranges("2021-09-21 00:00:00","2021-09-22 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
    // let time_ranges = get_time_ranges("2021-09-22 00:00:00","2021-09-23 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
    // let time_ranges = get_time_ranges("2021-09-23 00:00:00","2021-09-24 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap(); 
    // let time_ranges = get_time_ranges("2021-09-24 00:00:00","2021-09-25 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap(); 
    // let time_ranges = get_time_ranges("2021-09-25 00:00:00","2021-09-26 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap(); 
    let time_ranges = get_time_ranges("2021-09-26 00:00:00","2021-09-27 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap(); 

    let time_ranges_for_all_in_ml = get_time_ranges("2021-09-21 00:00:00","2021-09-27 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();


    match matches.value_of("INPUT").unwrap() {
        "destroy" => {
            warn!("You are deleting the entire crypto DB - fuck, I should double check this.");
            info!("deleting the entire crypto collection, you should configure this for options");
            // collection.delete_many(doc!{}, None).await?;    
        },

        "cluster-liquidations-by-day" => {

            // info!("Processing Liquidation Summaries");
            // for otr in &time_ranges{
            //     let hourlies = otr.get_hourlies().unwrap();
            //     assert_eq!(hourlies.len(),24);                
            //     let dcol: Vec<_> = (0..24).map(|n| hourlies[n].delete_exact_range(&lcollection)).collect();                
            // }

            let mut lvec = Vec::new();
            let txl = vec!["buy","sell"];

            for tx in txl {
    
                for tr in &time_ranges_for_all_in_ml {
                    tr.delete_exact_range(&ccollection);
                    let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}, "tx_type": tx };
                    let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build(); // cannot sort on such big entries
                    let mut cursor = lcollection.find(filter, find_options).await?;
                    while let Some(liquidation) = cursor.try_next().await? {
                        lvec.push(liquidation);
                    }        

                    let liquidations = Trades {
                        vts: lvec.clone()
                    };
                    let (p,q,pq,km) = liquidations.get_pqkm().unwrap();
                    for (idx, l) in liquidations.vts.iter().enumerate() {
                        assert_eq!(l.price, p[idx]);
                        assert_eq!(l.quantity, q[idx]);
                        let new_range_bound_liquidation_cluster = RangeBoundLiquidationCluster {
                            gtedate: tr.gtedate,
                            ltdate: tr.ltdate,
                            tx_type: tx.to_string().clone(),
                            price: p[idx],
                            quantity: q[idx],
                            cluster: km[idx]
                        };
                        let _result = ccollection.insert_one(new_range_bound_liquidation_cluster, None).await?;                                                                
                    }

                }

            }

        },



        "summary-hourlies" => {

            info!("Processing Market Summaries");
            for otr in &time_ranges{
                let hourlies = otr.get_hourlies().unwrap();
                assert_eq!(hourlies.len(),24);

                warn!("this delete concurrency run does not specify description, so both trade and liquidation summs are deleted");
                let dcol: Vec<_> = (0..24).map(|n| hourlies[n].delete_exact_range(&rbmscollection)).collect();
                let _rdvec = join_all(dcol).await;

                let hcol: Vec<_> = (0..24).map(|n| agg_rbms("Trade Count", &hourlies[n],&collection)).collect();
                let rvec = join_all(hcol).await;

                debug!("first trades");
                for (idx, r) in rvec.iter().enumerate() {
                    match r {
                        Ok(aggsv) => {
                            for aggs in aggsv {
                                println!("{}", aggs);
                                let _result = rbmscollection.insert_one(aggs, None).await?;                                                                
                            }
                        },
                        Err(error) => {
                            error!("Hour {:?} Error: {:?}", idx+1,  error);
                            panic!("We choose to no longer live.")                            
                        }
                    }
                }


                debug!("now liquidations");
                let hcollq: Vec<_> = (0..24).map(|n| agg_rbms("Liquidation Count", &hourlies[n],&lcollection)).collect();
                let rveclq = join_all(hcollq).await;

                for (idx, r) in rveclq.iter().enumerate() {
                    match r {
                        Ok(aggsv) => {
                            for aggs in aggsv {
                                println!("{}", aggs);
                                let _result = rbmscollection.insert_one(aggs, None).await?;                                                                
                            }
                        },
                        Err(error) => {
                            error!("Hour {:?} Error: {:?}", idx+1,  error);
                            panic!("We choose to no longer live.")                            
                        }
                    }
                }


            }



            info!("Processing Exchange Summaries");
            for otr in &time_ranges{
                let hourlies = otr.get_hourlies().unwrap();
                assert_eq!(hourlies.len(),24);
                
                let dcol: Vec<_> = (0..24).map(|n| hourlies[n].delete_exact_range(&rbescollection)).collect();
                let _rdvec = join_all(dcol).await;

                let hcol: Vec<_> = (0..24).map(|n| agg_rbes(&hourlies[n],&collection)).collect();
                let rvec = join_all(hcol).await;

                for (idx, r) in rvec.iter().enumerate() {
                    match r {
                        Ok(aggsv) => {
                            for aggs in aggsv {
                                println!("{}", aggs);
                                let _result = rbescollection.insert_one(aggs, None).await?;                                                                
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












