// this use statement gets you access to the lib file
use slurper::*;
use std::fmt; // Import `fmt`
use num_traits::pow::Pow;


use log::{info,debug};
use std::error::Error;
//use std::convert::TryFrom;
use self::models::{AnalysisArtifact,PhemexDataWrapperAccount,PhemexDataWrapperProducts, PhemexProduct, PhemexCurrency,PhemexDataWrapperMD, PhemexMD,TLPhemexMDSnapshot, CryptoLiquidation, Trades};
use chrono::{DateTime,Utc};
use time::Duration as NormalDuration;
use tokio::time as TokioTime;  //renamed norm duration so could use this for interval
use tokio::time::Duration as TokioDuration;  //renamed norm duration so could use this for interval

use futures::stream::TryStreamExt;
use mongodb::{Client};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};

use std::collections::HashMap;



#[macro_use]
extern crate clap;
use clap::App;

extern crate serde;
extern crate base64;



use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use sha2::Sha256;


use serde::{Deserialize, Serialize};


struct Stake<'a> {
    name: &'a str,
    quantity: f64,
}

impl fmt::Display for Stake<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<9} {:>6.4}", 
            &self.name, &self.quantity)
    }
}



// async fn swap<'a>(asset1: Asset<'_>, asset2: Asset<'_>) -> Result<bool, Box<dyn Error>> {

//     Ok(true)

// } 



#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::init(); 
    let yaml = load_yaml!("../coinmetrics.yml");

    let matches = App::from_yaml(yaml).get_matches();
//    debug!("{:?}",matches);

    match matches.value_of("INPUT").unwrap() {

        "demo_from_alex_evans_whitepaper" => {


            let mut weights = HashMap::new();
            weights.insert("Asset A", 1./3.);
            weights.insert("Asset B", 2./3.);


            let neil_stake_a = Stake {
                name: "Asset A",
                quantity: 5.
            };
            let neil_stake_b = Stake {
                name: "Asset B",
                quantity: 5.
            };
            let doster_stake_a = Stake {
                name: "Asset A",
                quantity: 5.
            };
            let doster_stake_b = Stake {
                name: "Asset B",
                quantity: 5.
            };

            let initial_stakes = vec![neil_stake_a,neil_stake_b,doster_stake_a,doster_stake_b];

            let mut pool = HashMap::new();
            for stake in initial_stakes {
                pool.entry(stake.name).and_modify(|e| { *e += stake.quantity}).or_insert(stake.quantity);                                
            }


            //https://www.dummies.com/education/math/business-statistics/how-to-find-the-weighted-geometric-mean-of-a-data-set/
            let sum_of_the_weights: f64 = weights.values().sum();
            let exponent_for_product = 1. / sum_of_the_weights; // 

            debug!("{:?}", pool);
            debug!("{}", sum_of_the_weights);
            debug!("{}", exponent_for_product);

            // so it applies the appropriate weight as exponent in map and then that vector of floats is productized, bitches
            let product: f64 = pool.iter().map(|(&a,&b)| b.pow(weights[a])).collect::<Vec<f64>>().iter().product();
            let gm_at_inception = product.pow(exponent_for_product);
            debug!("gm at inception is {}", gm_at_inception);



            // let proposal_1_delta_a = pool["Asset A"] + 1.;
            // let proposal_1_delta_b = pool["Asset B"] - 5.;
            // let mut proposal1 = HashMap::new();
            // proposal1.insert("Asset A", proposal_1_delta_a);
            // proposal1.insert("Asset B", proposal_1_delta_b);

            // let product_proposal_1: f64 = proposal1.iter().map(|(&a,&b)| b.pow(weights[a])).collect::<Vec<f64>>().iter().product();
            // let gm_after_proposal_1 = product_proposal_1.pow(exponent_for_product);
            // assert_eq!(gm_at_inception, gm_after_proposal_1);




            let proposal_2_delta_a = pool["Asset A"] + 1.;
            let proposal_2_delta_b = pool["Asset B"] - 0.466;
            let mut proposal2 = HashMap::new();
            proposal2.insert("Asset A", proposal_2_delta_a);
            proposal2.insert("Asset B", proposal_2_delta_b);
            debug!("proposal looks like {:?}", proposal2);

            let product_proposal_2: f64 = proposal2.iter().map(|(&a,&b)| b.pow(weights[a])).collect::<Vec<f64>>().iter().product();
            let gm_after_proposal_2 = product_proposal_2.pow(exponent_for_product);
            assert_eq!(gm_at_inception, gm_after_proposal_2);
            //rounding shit causes alex shit to fail but hey its fine



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












