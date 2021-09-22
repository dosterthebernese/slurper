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



pub fn do_duo_kmeans<'a>(v: Vec<f64>, vct: Vec<CryptoTradez>) -> Vec<CryptoCluster> {

    let rng = Isaac64Rng::seed_from_u64(42);
    let expected_centroids = array![[1000., 4.], [10000., 3.], [100000., 2.], [1000000., 1.],];
//    let n = 10000;
    let zdataset =  Array::from_shape_vec((v.len() / 2, 2), v.to_vec()).unwrap();
    let dataset = DatasetBase::from(zdataset);
    let n_clusters = expected_centroids.len_of(Axis(0));
    let model = KMeans::params_with_rng(n_clusters, rng)
        .max_n_iterations(200)
        .tolerance(1e-5)
        .fit(&dataset)
        .expect("KMeans fitted");
    let dataset = model.predict(dataset);
    let DatasetBase {
        records, targets, ..
    } = dataset;

    let mut rvec = Vec::new();


    for (idx, _ags) in vct.iter().enumerate() {
        let k = &targets[idx];

        let market = CryptoMarket {
            market: vct[idx].market.clone() 
        };

        let new_cc = CryptoCluster {
            lookback: LOOKBACK,
            lookahead: LOOKAHEAD,
            trade_date: vct[idx].trade_date,
            price: vct[idx].price,
            wm_price: vct[idx].wm_price,
            quantity: vct[idx].quantity,
            market: vct[idx].market.clone(),
            exchange: market.just_exchange().unwrap().to_string().clone(),
            instrument_type: market.drop_all_but_instrument_type().unwrap().to_string().clone(),
            tx_type: vct[idx].tx_type.clone(),
            z_score: vct[idx].z_score.unwrap_or(0.00),
            cluster: *k as i32         
        };
        rvec.push(new_cc.clone());
    }

    rvec

}




async fn opt_concurrent<'a>(tr: &TimeRange, collection: &Collection<CryptoTrade>, ocollection: &Collection<CryptoTradeOpt>) -> Result<i32, Box<dyn Error>> {

    info!("deleting crypto opt for {:?}", &tr);
    ocollection.delete_many(doc!{"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}}, None).await?;    
    info!("deleted");

    let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate} };
    let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
    let mut cursor = collection.find(filter, find_options).await?;

    let mut trade_count = 0;
    let mut bulk_loader: Vec<CryptoTradeOpt> = Vec::new();

    while let Some(trade) = cursor.try_next().await? {
        trade_count+=1;


        let m1 = CryptoMarket {
            market: trade.market.clone()
        };


        let new_crypto_trade_opt = CryptoTradeOpt {
            trade_date: trade.trade_date,
            price: trade.price,
            quantity: trade.quantity.clone(),
            market: trade.market.clone(),
            tx_type: trade.tx_type.clone(),
            exchange: m1.just_exchange().unwrap().to_string(),
            instrument: m1.drop_exchange().unwrap(),
            instrument_type: m1.drop_all_but_instrument_type().unwrap().to_string()
        };

        println!("{}",new_crypto_trade_opt);
        &bulk_loader.push(new_crypto_trade_opt);
        if &bulk_loader.len() == &10000 {
            let _result = ocollection.insert_many(&bulk_loader, None).await?;
            &bulk_loader.clear();
        }

    }

    debug!("You need to do the last bulk loader");
    if &bulk_loader.len() > &0 {
        let _result = ocollection.insert_many(&bulk_loader, None).await?;
        &bulk_loader.clear();
    }


    debug!("{:?} {:?}", &tr, trade_count);
    Ok(trade_count)

}









async fn comp_concurrent<'a>(tr: &TimeRange, collection: &Collection<CryptoTradeOpt>, zcollection: &Collection<CryptoTradez>) -> Result<i32, Box<dyn Error>> {

    info!("deleting crypto z for {:?}", &tr);
    zcollection.delete_many(doc!{"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate}, "lookback": LOOKBACK, "lookahead": LOOKAHEAD}, None).await?;    
    info!("deleted");

    let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate} };
    let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).batch_size(1000).build();
    let mut cursor = collection.find(filter, find_options).await?;

    let mut trade_count = 0;
    let mut bulk_loader: Vec<CryptoTradez> = Vec::new();

    while let Some(trade) = cursor.try_next().await? {
        trade_count+=1;

        let vectrades = trade.get_comparables(&LOOKBACK, &LOOKAHEAD,&collection).await?;
        let cmptrades = Trades {
            vts: vectrades
        };
        let wmqlb = cmptrades.get_weighted_mean().unwrap();
        let stdlb = cmptrades.get_standard_deviation_of_prices().unwrap();

        let zs = match stdlb {
            Some(std) => {
                let diff = trade.price - wmqlb.mean();
                let z = diff / std;
                if z.is_nan() {
                    None
                } else {
                    Some(z)
                }
            },
            _ => {None}
        };

        let new_crypto_tradez = CryptoTradez {
            lookback: LOOKBACK,
            lookahead: LOOKAHEAD,
            trade_date: trade.trade_date,
            price: trade.price,
            wm_price: wmqlb.mean(),
            quantity: trade.quantity.clone(),
            market: trade.market.clone(),
            tx_type: trade.tx_type.clone(),
            z_score: zs
        };
        println!("{}",new_crypto_tradez);
        &bulk_loader.push(new_crypto_tradez);
        //let _result = zcollection.insert_one(new_crypto_tradez, None).await?;                    
        if &bulk_loader.len() == &1000 {
            let _result = zcollection.insert_many(&bulk_loader, None).await?;
            &bulk_loader.clear();
        }

    }

    debug!("You need to do the last bulk loader");
    if &bulk_loader.len() > &0 {
        let _result = zcollection.insert_many(&bulk_loader, None).await?;
        &bulk_loader.clear();
    }


    debug!("{:?} {:?}", &tr, trade_count);
    Ok(trade_count)

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
    let ocollection = database.collection::<CryptoTradeOpt>(THE_CRYPTO_OPT_COLLECTION);
    let lcollection = database.collection::<CryptoLiquidation>(THE_CRYPTO_LIQUIDATION_COLLECTION);
    let zcollection = database.collection::<CryptoTradez>(THE_CRYPTOZ_COLLECTION);
    let zccollection = database.collection::<CryptoCluster>(THE_CRYPTOCLUSTER_COLLECTION);
    let capcollection = database.collection::<CapSuite>(THE_CRYPTO_CAP_SUITE_COLLECTION);

    // let time_ranges = get_time_ranges("2021-09-13 00:00:00","2021-09-14 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
    // let time_ranges = get_time_ranges("2021-09-14 00:00:00","2021-09-15 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
    // let time_ranges = get_time_ranges("2021-09-15 00:00:00","2021-09-16 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
    // let time_ranges = get_time_ranges("2021-09-16 00:00:00","2021-09-17 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
    let time_ranges = get_time_ranges("2021-09-17 00:00:00","2021-09-18 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();


    match matches.value_of("INPUT").unwrap() {
        "destroy" => {
            warn!("You are deleting the entire crypto DB - fuck, I should double check this.");
            info!("deleting the entire crypto collection, you should configure this for options");
            collection.delete_many(doc!{}, None).await?;    
        },
        "destroy-derivative" => {
            warn!("You are deleting the entire crypto z and cluster DB - fuck, I should double check this.");
            zcollection.delete_many(doc!{}, None).await?;    
            zccollection.delete_many(doc!{}, None).await?;    
        },




        "spawnopt" => {

            for otr in time_ranges{
                let hourlies = otr.get_hourlies().unwrap();
                assert_eq!(hourlies.len(),24);
                let hour1 = &hourlies[0];
                let hour2 = &hourlies[1];
                let hour3 = &hourlies[2];
                let hour4 = &hourlies[3];
                let hour5 = &hourlies[4];
                let hour6 = &hourlies[5];
                let hour7 = &hourlies[6];
                let hour8 = &hourlies[7];
                let hour9 = &hourlies[8];
                let hour10 = &hourlies[9];
                let hour11 = &hourlies[10];
                let hour12 = &hourlies[11];
                let hour13 = &hourlies[12];
                let hour14 = &hourlies[13];
                let hour15 = &hourlies[14];
                let hour16 = &hourlies[15];
                let hour17 = &hourlies[16];
                let hour18 = &hourlies[17];
                let hour19 = &hourlies[18];
                let hour20 = &hourlies[19];
                let hour21 = &hourlies[20];
                let hour22 = &hourlies[21];
                let hour23 = &hourlies[22];
                let hour24 = &hourlies[23];
                let comps_hour1 = opt_concurrent(&hour1,&collection,&ocollection);
                let comps_hour2 = opt_concurrent(&hour2,&collection,&ocollection);
                let comps_hour3 = opt_concurrent(&hour3,&collection,&ocollection);
                let comps_hour4 = opt_concurrent(&hour4,&collection,&ocollection);
                let comps_hour5 = opt_concurrent(&hour5,&collection,&ocollection);
                let comps_hour6 = opt_concurrent(&hour6,&collection,&ocollection);
                let comps_hour7 = opt_concurrent(&hour7,&collection,&ocollection);
                let comps_hour8 = opt_concurrent(&hour8,&collection,&ocollection);
                let comps_hour9 = opt_concurrent(&hour9,&collection,&ocollection);
                let comps_hour10 = opt_concurrent(&hour10,&collection,&ocollection);
                let comps_hour11 = opt_concurrent(&hour11,&collection,&ocollection);
                let comps_hour12 = opt_concurrent(&hour12,&collection,&ocollection);
                let comps_hour13 = opt_concurrent(&hour13,&collection,&ocollection);
                let comps_hour14 = opt_concurrent(&hour14,&collection,&ocollection);
                let comps_hour15 = opt_concurrent(&hour15,&collection,&ocollection);
                let comps_hour16 = opt_concurrent(&hour16,&collection,&ocollection);
                let comps_hour17 = opt_concurrent(&hour17,&collection,&ocollection);
                let comps_hour18 = opt_concurrent(&hour18,&collection,&ocollection);
                let comps_hour19 = opt_concurrent(&hour19,&collection,&ocollection);
                let comps_hour20 = opt_concurrent(&hour20,&collection,&ocollection);
                let comps_hour21 = opt_concurrent(&hour21,&collection,&ocollection);
                let comps_hour22 = opt_concurrent(&hour22,&collection,&ocollection);
                let comps_hour23 = opt_concurrent(&hour23,&collection,&ocollection);
                let comps_hour24 = opt_concurrent(&hour24,&collection,&ocollection);
                let (r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24) = join!(
                    comps_hour1,comps_hour2,comps_hour3,comps_hour4,comps_hour5,comps_hour6,comps_hour7,comps_hour8,
                    comps_hour9,comps_hour10,comps_hour11,comps_hour12,comps_hour13,comps_hour14,comps_hour15,comps_hour16,
                    comps_hour17,comps_hour18,comps_hour19,comps_hour20,comps_hour21,comps_hour22,comps_hour23,comps_hour24);


                let rvec = vec![r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,r11,r12,r13,r14,r15,r16,r17,r18,r19,r20,r21,r22,r23,r24];


                for (idx, r) in rvec.iter().enumerate() {
                    match r {
                        Ok(cnt) => debug!("Processed {:?}", cnt),
                        Err(error) => {
                            error!("Hour {:?} Error: {:?}", idx+1,  error);
                            panic!("We choose to no longer live.")                            
                        }
                    }
                }

            }

        },















        "spawnz" => {

            for otr in time_ranges{
                let hourlies = otr.get_hourlies().unwrap();
                assert_eq!(hourlies.len(),24);
                let hour1 = &hourlies[0];
                let hour2 = &hourlies[1];
                let hour3 = &hourlies[2];
                let hour4 = &hourlies[3];
                let hour5 = &hourlies[4];
                let hour6 = &hourlies[5];
                let hour7 = &hourlies[6];
                let hour8 = &hourlies[7];
                let hour9 = &hourlies[8];
                let hour10 = &hourlies[9];
                let hour11 = &hourlies[10];
                let hour12 = &hourlies[11];
                let hour13 = &hourlies[12];
                let hour14 = &hourlies[13];
                let hour15 = &hourlies[14];
                let hour16 = &hourlies[15];
                let hour17 = &hourlies[16];
                let hour18 = &hourlies[17];
                let hour19 = &hourlies[18];
                let hour20 = &hourlies[19];
                let hour21 = &hourlies[20];
                let hour22 = &hourlies[21];
                let hour23 = &hourlies[22];
                let hour24 = &hourlies[23];
                let comps_hour1 = comp_concurrent(&hour1,&ocollection,&zcollection);
                let comps_hour2 = comp_concurrent(&hour2,&ocollection,&zcollection);
                let comps_hour3 = comp_concurrent(&hour3,&ocollection,&zcollection);
                let comps_hour4 = comp_concurrent(&hour4,&ocollection,&zcollection);
                let comps_hour5 = comp_concurrent(&hour5,&ocollection,&zcollection);
                let comps_hour6 = comp_concurrent(&hour6,&ocollection,&zcollection);
                let comps_hour7 = comp_concurrent(&hour7,&ocollection,&zcollection);
                let comps_hour8 = comp_concurrent(&hour8,&ocollection,&zcollection);
                let comps_hour9 = comp_concurrent(&hour9,&ocollection,&zcollection);
                let comps_hour10 = comp_concurrent(&hour10,&ocollection,&zcollection);
                let comps_hour11 = comp_concurrent(&hour11,&ocollection,&zcollection);
                let comps_hour12 = comp_concurrent(&hour12,&ocollection,&zcollection);
                let comps_hour13 = comp_concurrent(&hour13,&ocollection,&zcollection);
                let comps_hour14 = comp_concurrent(&hour14,&ocollection,&zcollection);
                let comps_hour15 = comp_concurrent(&hour15,&ocollection,&zcollection);
                let comps_hour16 = comp_concurrent(&hour16,&ocollection,&zcollection);
                let comps_hour17 = comp_concurrent(&hour17,&ocollection,&zcollection);
                let comps_hour18 = comp_concurrent(&hour18,&ocollection,&zcollection);
                let comps_hour19 = comp_concurrent(&hour19,&ocollection,&zcollection);
                let comps_hour20 = comp_concurrent(&hour20,&ocollection,&zcollection);
                let comps_hour21 = comp_concurrent(&hour21,&ocollection,&zcollection);
                let comps_hour22 = comp_concurrent(&hour22,&ocollection,&zcollection);
                let comps_hour23 = comp_concurrent(&hour23,&ocollection,&zcollection);
                let comps_hour24 = comp_concurrent(&hour24,&ocollection,&zcollection);


                let (r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18, r19, r20, r21, r22, r23, r24) = join!(
                    comps_hour1,comps_hour2,comps_hour3,comps_hour4,comps_hour5,comps_hour6,comps_hour7,comps_hour8,
                    comps_hour9,comps_hour10,comps_hour11,comps_hour12,comps_hour13,comps_hour14,comps_hour15,comps_hour16,
                    comps_hour17,comps_hour18,comps_hour19,comps_hour20,comps_hour21,comps_hour22,comps_hour23,comps_hour24);

                let rvec = vec![r1,r2,r3,r4,r5,r6,r7,r8,r9,r10,r11,r12,r13,r14,r15,r16,r17,r18,r19,r20,r21,r22,r23,r24];


                for (idx, r) in rvec.iter().enumerate() {
                    match r {
                        Ok(cnt) => debug!("Processed {:?}", cnt),
                        Err(error) => {
                            error!("Hour {:?} Error: {:?}", idx+1,  error);
                            panic!("We choose to no longer live.")                            
                        }
                    }
                }

            }

        },




        "cluster-spots" => {

            for tr in time_ranges{

                let mut vctdb = Vec::new();
                let mut vctds = Vec::new();
                let mut vctb = Vec::new();
                let mut vcts = Vec::new();

                info!("Operating on range: {} {}", &tr.gtedate, &tr.ltdate);
                warn!("NOTE THAT THIS CALL DOES NOT DELETE FOR THE GIVEN LOOKBACK LOOKAHEAD FOR THE DATE RANGE!");
                warn!("this is just going to filter after the query for spot - so, should update make z to push those strings so can be part of query");
                let filter = doc! {"trade_date": {"$gte": tr.gtedate, "$lt": tr.ltdate} };
//                let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
                let find_options = FindOptions::builder().build();
                let mut cursor = zcollection.find(filter, find_options).await?;
                while let Some(trade) = cursor.try_next().await? {
                    let market = CryptoMarket {
                        market: trade.market.clone()
                    };

                    if market.drop_all_but_instrument_type().unwrap().to_string() == "spot" {
                        let zs = trade.z_score.unwrap_or(0.00);
                        match trade.tx_type.as_str() {
                            "buy" => {
                                vctdb.push(trade.get_net().unwrap());
                                vctdb.push(zs);
                                vctb.push(trade);
                            },
                            "sell" => {
                                vctds.push(trade.get_net().unwrap());
                                vctds.push(zs);
                                vcts.push(trade);
                            },
                            _ => {}
                        }                        
                    }

                }


                let vccb = do_duo_kmeans(vctdb,vctb);
                for v in &vccb {
                    debug!("{:?}", v);
                    let _result = zccollection.insert_one(v, None).await?;                            
                }        

                let vccs = do_duo_kmeans(vctds,vcts);
                for v in &vccs {
                    debug!("{:?}", v);
                    let _result = zccollection.insert_one(v, None).await?;                            
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












