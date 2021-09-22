// this use statement gets you access to the lib file - you use crate instead of the package name, who the fuck knows why (see any bin rs)
use crate::*;

use serde::{Serialize,Deserialize};
use bson::serde_helpers::chrono_datetime_as_bson_datetime;
use chrono::{DateTime,Utc,TimeZone};
use chrono::format::ParseError;

use mongodb::{Collection};
use mongodb::{error::Error};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};
use futures::stream::TryStreamExt;

use time::Duration;
use average::{WeightedMean,Min,Max};


use std::fmt; // Import `fmt`



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoAsset {
    pub asset: String,
}

impl CryptoAsset {

    pub async fn get_last_updated_cap_suite(self: &Self, collection: &Collection<CapSuite>) -> Result<DateTime<Utc>, Error> {

        let filter = doc! {"asset": &self.asset};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 1, 444);

        while let Some(trade) = cursor.try_next().await? {
            big_bang = trade.trade_date;
        }
        Ok(big_bang)

    }
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CapSuite {
    pub asset: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub price: f64,
    pub cap_mvrv_cur: f64,
    pub cap_mvrv_ff: f64,
    pub cap_mrkt_cur_usd: f64,
    pub cap_mrkt_ff_usd: f64,
    pub cap_real_usd: f64,
}

impl CapSuite {

    pub fn get_csv(self: &Self) -> Result<CapSuiteCSV,ParseError> {
        Ok(CapSuiteCSV {
            asset: self.asset.clone(),
            trade_date: self.trade_date.to_rfc3339(),
            price: self.price,
            cap_mvrv_cur: self.cap_mvrv_cur,
            cap_mvrv_ff: self.cap_mvrv_ff,
            cap_mrkt_cur_usd: self.cap_mrkt_cur_usd,
            cap_mrkt_ff_usd: self.cap_mrkt_ff_usd,
            cap_real_usd: self.cap_real_usd
        })
    }

}

impl fmt::Display for CapSuite {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:>5} {:<9} {:>12.4} {:>12.4} {:>12.4} {:>12.4} {:>12.4} {:>12.4}", 
            self.asset, self.trade_date, self.price, self.cap_mvrv_cur, self.cap_mvrv_ff, self.cap_mrkt_cur_usd, self.cap_mrkt_ff_usd, self.cap_real_usd)
    }
}


#[derive(Debug, Serialize)]
pub struct CapSuiteCSV {
    #[serde(rename(serialize = "Asset"))]
    asset: String,
    #[serde(rename(serialize = "TradeDate"))]
    pub trade_date: String,
    #[serde(rename(serialize = "Price"))]
    price: f64,
    #[serde(rename(serialize = "CapMVRVCur"))]
    cap_mvrv_cur: f64,
    #[serde(rename(serialize = "CapMVRVFF"))]
    cap_mvrv_ff: f64,
    #[serde(rename(serialize = "CapMrktCurUSD"))]
    cap_mrkt_cur_usd: f64,
    #[serde(rename(serialize = "CapMrktFFUSD"))]
    cap_mrkt_ff_usd: f64,
    #[serde(rename(serialize = "CapRealUSD"))]
    cap_real_usd: f64,
}








#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoMarket {
    pub market: String,
}

impl CryptoMarket {


    pub fn drop_all_but_instrument_type(self: &Self) -> Result<&str, ParseError> {
        let market_array = self.market.split('-').collect::<Vec<&str>>();
        Ok(market_array[market_array.len()-1])
    }

    pub fn drop_exchange(self: &Self) -> Result<String, ParseError> {
        let mut market_array = self.market.split('-').collect::<Vec<&str>>();
        market_array.remove(0);
        Ok(market_array.join("-"))
    }

    pub fn just_exchange(self: &Self) -> Result<&str, ParseError> {
        let market_array = self.market.split('-').collect::<Vec<&str>>();
        Ok(market_array[0])
    }


    pub async fn get_last_updated_trade(self: &Self, collection: &Collection<CryptoTrade>) -> Result<DateTime<Utc>, Error> {

        let filter = doc! {"market": &self.market};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 1, 444);

        while let Some(trade) = cursor.try_next().await? {
            big_bang = trade.trade_date;
        }
        Ok(big_bang)

    }

    pub async fn get_last_updated_liquidation(self: &Self, collection: &Collection<CryptoLiquidation>) -> Result<DateTime<Utc>, Error> {

        let filter = doc! {"market": &self.market};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = Utc.ymd(1970, 1, 1).and_hms_nano(0, 0, 1, 444);

        while let Some(trade) = cursor.try_next().await? {
            big_bang = trade.trade_date;
        }
        Ok(big_bang)

    }

}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoLiquidation {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub coin_metrics_id: String,
    pub price: f64,
    pub quantity: f64,
    pub market: String,
    pub tx_type: String,
    pub cm_type: String,
    pub trade_llama_exchange: String,
    pub trade_llama_instrument: String,
    pub trade_llama_instrument_type: String,
}

impl CryptoLiquidation {

    pub fn get_net(self: &Self) -> Result<f64, ParseError> {
        Ok(&self.price * &self.quantity)
    }

    pub async fn get_comparables(self: &Self, lb: &i64, la: &i64, collection: &Collection<CryptoLiquidation>) -> Result<Vec<CryptoLiquidation>, Error> {

        let gtedate = self.trade_date - Duration::milliseconds(*lb);
        let ltdate = self.trade_date + Duration::milliseconds(*la);

        let mut crys: Vec<CryptoLiquidation> = Vec::new();
        let filter = doc! {"trade_date": {"$gte": gtedate, "$lt": ltdate}, "tx_type": &self.tx_type, "market": &self.market, "cm_type": &self.cm_type};
        let find_options = FindOptions::builder().sort(doc! { "trade_date":1}).build();
        let mut cursor = collection.find(filter, find_options).await?;
        while let Some(cry) = cursor.try_next().await? {
            crys.push(cry.clone());                
        }

        Ok(crys)

    }
}

impl fmt::Display for CryptoLiquidation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<9} {:<9} {:>6.2} {:>9.4} {:<30} {:<5} {:<5} {:<30} {:<30} {:<30}", 
            &self.coin_metrics_id, &self.trade_date, self.price, self.quantity, &self.market, &self.tx_type, &self.cm_type,
            &self.trade_llama_exchange, &self.trade_llama_instrument, &self.trade_llama_instrument_type)
    }
}






#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trades<T> {
    pub vts: Vec<T>
}
impl Trades<CryptoTrade> {

    pub fn get_total_volume(self: &Self) -> Result<f64, ParseError> {
        let volume: f64 = self.vts.iter().map(|s| s.quantity).sum();
        Ok(volume)
    }

    fn get_vector_of_prices(self: &Self) -> Result<Vec<f64>, ParseError> {
        let vector_of_prices: Vec<f64> = self.vts.iter().map(|s|s.price).collect();
        Ok(vector_of_prices)        
    }
    fn get_vector_of_quantities(self: &Self) -> Result<Vec<f64>, ParseError> {
        let vector_of_quantities: Vec<f64> = self.vts.iter().map(|s|s.quantity).collect();
        Ok(vector_of_quantities)        
    }

    pub fn get_weighted_mean(self: &Self) -> Result<WeightedMean, ParseError> {
        let vector_of_quantities = self.get_vector_of_quantities().unwrap();
        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let wm: WeightedMean = vector_of_prices.clone().iter().zip(vector_of_quantities.clone()).map(|(x,w)| (x.clone(),w.clone())).collect();    
        assert_eq!(wm.sum_weights(), self.get_total_volume().unwrap());
        Ok(wm)
    }

    pub fn get_min_price(self: &Self) -> Result<Min, ParseError> {
        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let min_price: Min = vector_of_prices.iter().collect();
        Ok(min_price)
    }

    pub fn get_max_price(self: &Self) -> Result<Max, ParseError> {
        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let max_price: Max = vector_of_prices.iter().collect();
        Ok(max_price)
    }

    pub fn get_standard_deviation_of_prices(self: &Self) -> Result<Option<f64>, ParseError> {
        let vector_of_prices = self.get_vector_of_prices().unwrap();
        // using a local lib std deviation as rust std libs a little bit of a pain - should change - not this is from affinities process
        let std = std_deviation(&vector_of_prices);
        Ok(std)
    }

    pub fn get_standard_deviation_of_quantities(self: &Self) -> Result<Option<f64>, ParseError> {
        let vector_of_quantities = self.get_vector_of_quantities().unwrap();
        // using a local lib std deviation as rust std libs a little bit of a pain - should change - not this is from affinities process
        let std = std_deviation(&vector_of_quantities);
        Ok(std)
    }

    pub fn get_not_weighted_mean_of_quantities(self: &Self) -> Result<Option<f64>, ParseError> {
        let vector_of_quantities = self.get_vector_of_quantities().unwrap();
        Ok(mean(&vector_of_quantities))
    }

    pub fn get_performance(self: &Self) -> Result<Option<f64>, ParseError> {

        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let perf = if vector_of_prices.len() > 1 {
            let start = vector_of_prices[0];
            let finish = vector_of_prices[vector_of_prices.len()-1];
            Some((finish - start) / start)
        } else {
            None
        };
        Ok(perf)
    }


    pub fn get_wildest(self: &Self) -> Result<Option<f64>, ParseError> {

        let vector_of_prices = self.get_vector_of_prices().unwrap();
        let wildest = if vector_of_prices.len() > 1 {
            let start = self.get_min_price().unwrap().min();
            let finish = self.get_max_price().unwrap().max();
            Some((finish - start) / start)
        } else {
            None
        };

        Ok(wildest)
    }

    pub fn get_last_quantity(self: &Self) -> Result<Option<f64>, ParseError> {
        let vector_of_quantities = self.get_vector_of_quantities().unwrap();
        let last_quantity = if vector_of_quantities.len() > 1 {
            Some(vector_of_quantities[vector_of_quantities.len()-1])
        } else {
            None
        };
        Ok(last_quantity)

    }





}







#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoTrade {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub coin_metrics_id: String,
    pub price: f64,
    pub quantity: f64,
    pub market: String,
    pub tx_type: String,
    pub trade_llama_exchange: String,
    pub trade_llama_instrument: String,
    pub trade_llama_instrument_type: String,
}
impl CryptoTrade {

    pub fn get_net(self: &Self) -> Result<f64, ParseError> {
        Ok(&self.price * &self.quantity)
    }

}


impl fmt::Display for CryptoTrade {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<9} {:<9} {:>6.2} {:>9.4} {:<30} {:<5} {:<30} {:<30} {:<30}", 
            &self.coin_metrics_id, &self.trade_date, self.price, self.quantity, &self.market, &self.tx_type, 
            &self.trade_llama_exchange, &self.trade_llama_instrument, &self.trade_llama_instrument_type)
    }
}


























#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoTradez {
    pub lookback: i64,
    pub lookahead: i64,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub price: f64,
    pub wm_price: f64,
    pub quantity: f64,
    pub market: String,
    pub tx_type: String,
    pub z_score: Option<f64>,
}

impl CryptoTradez {
    pub fn get_net(self: &Self) -> Result<f64, ParseError> {
        Ok(&self.price * &self.quantity)
    }
    pub fn get_difference(self: &Self) -> Result<f64, ParseError> {
        Ok((&self.price * &self.quantity) - (&self.wm_price * &self.quantity))
    }
}

impl fmt::Display for CryptoTradez {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let zstring = match self.z_score {
            Some(z) => z.to_string(),
            _ => "".to_string()
        };
        write!(f, "{:>4} {:>4} {:<9} {:>6.2} {:>6.2} {:>9.4} {:<30} {:<5} {:>6}", 
            self.lookback, self.lookahead, &self.trade_date, self.price, self.wm_price, self.quantity, &self.market, &self.tx_type, zstring)
    }
}



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CryptoCluster {
    pub lookback: i64,
    pub lookahead: i64,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub price: f64,
    pub wm_price: f64,
    pub quantity: f64,
    pub market: String,
    pub exchange: String,
    pub instrument_type: String,
    pub tx_type: String,
    pub z_score: f64,
    pub cluster: i32
}
impl CryptoCluster {
    pub fn get_net(self: &Self) -> Result<f64, ParseError> {
        Ok(&self.price * &self.quantity)
    }
    pub fn get_difference(self: &Self) -> Result<f64, ParseError> {
        Ok((&self.price * &self.quantity) - (&self.wm_price * &self.quantity))
    }
    pub fn get_csv(self: &Self) -> Result<CryptoClusterCSV,ParseError> {
        Ok(CryptoClusterCSV {
            market: self.market.clone(),
            exchange: self.exchange.clone(),
            instrument_type: self.instrument_type.clone(),
            tx_type: self.tx_type.clone(),
            net_amount: self.get_net().unwrap(),
            z_score: self.z_score,
            cluster: self.cluster
        })
    }
}


#[derive(Debug, Serialize)]
pub struct CryptoClusterCSV {
    #[serde(rename(serialize = "Market"))]
    market: String,
    #[serde(rename(serialize = "Exchange"))]
    exchange: String,
    #[serde(rename(serialize = "InstrumentType"))]
    instrument_type: String,
    #[serde(rename(serialize = "TX"))]
    tx_type: String,
    #[serde(rename(serialize = "NetAmount"))]
    net_amount: f64,
    #[serde(rename(serialize = "Z"))]
    z_score: f64,
    #[serde(rename(serialize = "Cluster"))]
    cluster: i32
}



#[derive(Debug, Serialize)]
pub struct ImpactItem {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub gtedate: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub ltdate: DateTime<Utc>,    
    pub lookback: i64,
    pub lookahead: i64,
    #[serde(rename(serialize = "Market"))]
    pub aggregation_thing: String,
    #[serde(rename(serialize = "TX"))]
    pub tx_type: String,
    #[serde(rename(serialize = "NetDifference"))]
    pub net_difference: f64
}

impl ImpactItem {
    pub fn get_csv(self: &Self) -> Result<ImpactItemCSV,ParseError> {
        Ok(ImpactItemCSV {
            gtedate: self.gtedate.to_rfc3339(),
            ltdate: self.gtedate.to_rfc3339(),
            lookback: self.lookback,
            lookahead: self.lookahead,
            aggregation_thing: self.aggregation_thing.clone(),
            tx_type: self.tx_type.clone(),
            net_difference: self.net_difference
        })
    }
}



impl fmt::Display for ImpactItem {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<9} {:<9} {:>8} {:>8} {:<30} {:<10} {:>10.2}", 
            &self.gtedate, &self.ltdate, self.lookback, self.lookahead, &self.aggregation_thing, &self.tx_type, self.net_difference)
    }
}




#[derive(Debug, Serialize)]
pub struct ImpactItemCSV {
    #[serde(rename(serialize = "BeginDate"))]
    pub gtedate: String,
    #[serde(rename(serialize = "EndDate"))]
    pub ltdate: String,    
    #[serde(rename(serialize = "Lookback"))]
    pub lookback: i64,
    #[serde(rename(serialize = "Lookahead"))]
    pub lookahead: i64,
    #[serde(rename(serialize = "Market Exchange"))]
    aggregation_thing: String,
    #[serde(rename(serialize = "TX"))]
    tx_type: String,
    #[serde(rename(serialize = "NetDifference"))]
    net_difference: f64
}



#[derive(Debug, Serialize)]
pub struct ADItem {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub gtedate: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub ltdate: DateTime<Utc>,    
    #[serde(rename(serialize = "Market"))]
    pub market: String,
    #[serde(rename(serialize = "TX"))]
    pub tx_type: String,
    #[serde(rename(serialize = "NetDifference"))]
    pub net_amount: f64
}

impl ADItem {
    pub fn get_csv(self: &Self) -> Result<ADItemCSV,ParseError> {
        Ok(ADItemCSV {
            gtedate: self.gtedate.to_rfc3339(),
            ltdate: self.gtedate.to_rfc3339(),
            market: self.market.clone(),
            tx_type: self.tx_type.clone(),
            net_amount: self.net_amount
        })
    }
}

#[derive(Debug, Serialize)]
pub struct ADItemCSV {
    #[serde(rename(serialize = "BeginDate"))]
    pub gtedate: String,
    #[serde(rename(serialize = "EndDate"))]
    pub ltdate: String,    
    #[serde(rename(serialize = "Market"))]
    market: String,
    #[serde(rename(serialize = "TX"))]
    tx_type: String,
    #[serde(rename(serialize = "NetAmount"))]
    net_amount: f64
}






#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RIA {
    pub business_name: String,
    pub crd: String,
    pub sec: String,
    pub total_employees: Option<f64>, //Approximately how many employees do you have? Include full- and part-time employees but do not include any clerical workers.
    pub total_employees_investment_advisory: Option<f64>, //Approximately how many of the employees reported in 5.A. perform investment advisory functions (including research)?
    pub reg_aum_discretionary: Option<f64>,
    pub reg_aum_non_discretionary: Option<f64>,
    pub reg_aum_total: Option<f64>,
    pub reg_accounts_discretionary: Option<f64>,
    pub reg_accounts_non_discretionary: Option<f64>,
    pub reg_accounts_total: Option<f64>,
    pub reg_aum_non_us_total: Option<f64>,
    pub financial_planning: bool, //Financial planning services
    pub pm_ind_smb: bool, //Portfolio management for individuals and/or small businesses
    pub pm_ic_bdcs54: bool, //Portfolio management for investment companies (as well as “business development companies” that have made an election pursuant to section 54 of the Investment Company Act of 1940)
    pub pm_piv_noic: bool, //Portfolio management for pooled investment vehicles (other than investment companies)
    pub pm_b_icl_nosmb_no_ic_no_piv: bool, //Portfolio management for businesses (other than small businesses) or institutional clients (other than registered investment companies and other pooled investment vehicles) 
    pub pension_consulting_services: bool, //Pension consulting services
    pub selection: bool, //Selection of other advisers (including private fund managers)
    pub publications: bool, //Publication of periodicals or newsletters
    pub rating_pricing: bool, //Security ratings or pricing services
    pub timing: bool, //Market timing services
    pub education: bool, //Educational seminars/workshops
    pub other: Option<String>,
    pub wrap_fee: bool,
    pub prop_ct_tx_with_clients: bool,
    pub prop_ct_tx_and_reco_with_clients: bool,
    pub prop_ct_other_reco_with_clients: bool,
    pub si_ct_tx: bool,
    pub si_ct_reco: bool,
    pub si_ct_reco_other: bool,
    pub bad_supervised_person: bool,
    pub felony_conviction: bool, 
    pub felony_charge: bool, 
    pub misdemeanor_conviction: bool,
    pub misdemeanor_charge: bool,
    pub sec_cftc_false_statement: bool,
    pub sec_ctfc_violation: bool,
    pub sec_cftc_dsrr: bool,
    pub sec_cftc_order: bool,
    pub sec_cftc_money_cease: bool,
    pub fed_state_foreign_reg_false_statement: bool,
    pub fed_state_foreign_reg_violation: bool,
    pub fed_state_foreign_reg_cause_dsrr: bool,
    pub fed_state_foreign_reg_order: bool,
    pub fed_state_foreign_reg_dsrr: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Prospect {
    pub first_name: String,
    pub last_name: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub last_spam_date: DateTime<Utc>,
    pub email: String,
    pub best_phone: Option<String>,
    pub title: String,
    pub company: String,
    pub crd: Option<String>,
    pub attributes: Vec<String>,
    pub website: String,
    pub notes: Option<String>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StatusUpdate {
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub began: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub updated: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Opportunity {
    pub company: String,
    pub crd: Option<String>,
    pub attributes: Vec<String>,
    pub website: String,
    pub status: Vec<StatusUpdate>,
    pub notes: Option<String>
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Quote {
    pub symbol: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
    pub company: String,
    pub entity: String
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trade {
    pub account_id: String,
    pub account_name: String,
    pub security_ticker: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub trade_date: DateTime<Utc>,
    pub price: f64,
    pub quantity: f64,
    pub commission: f64,
    pub net_amount: f64,
    pub tx_type: String,
    pub broker: String,
    pub source: String,
    pub company: String,
    pub entity: String
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WAP {
    pub company: String,
    pub security_ticker: String,
    pub tx_type: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub gtedate: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub ltdate: DateTime<Utc>,
    pub net_amount: f64,
    pub volume: f64,
    pub weighted_mean: f64,
    pub accounts: Vec<String>,
    pub t100_std: Option<f64>,
    pub t100_performance: Option<f64>,
    pub t100_wildest: Option<f64>,
    pub f3_performance: Option<f64>,
    pub f30_std: Option<f64>,
    pub f30_performance: Option<f64>,
    pub f30_wildest: Option<f64>,
    pub t100_not_weighted_mean_volume: Option<f64>,
    pub t100_std_volume: Option<f64>,
    pub market_volume: Option<f64>
}

// you're doing this at the WAP level so doesn't need to be a vector!
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WAPContra {
    pub company: String,
    pub wap: WAP,
    pub same_day_contra: Option<WAP>,
    pub one_day_contra: Option<WAP>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Position {
    pub company: String,
    pub account_id: String,
    pub account_name: String,
    pub security_ticker: String,
    pub direction: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub gtedate: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub ltdate: DateTime<Utc>,
    pub quantity: f64
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PositionQuantityVolatility {
    pub company: String,
    pub security_ticker: String,
    pub direction: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub gtedate: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub ltdate: DateTime<Utc>,
    pub normalized_std_deviation: f64
}



#[derive(Debug, Serialize, Deserialize)]
pub struct AffinityGroup {
    pub company: String,
    pub accounts: Vec<String>,
    pub actions: i32,    
    pub net_amount: f64,    
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub min_date: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub max_date: DateTime<Utc>,
    pub actions_buys: Option<i32>,    
    pub net_amount_buys: Option<f64>,    
    pub net_amount_buys_mean: Option<f64>,    
    pub net_amount_buys_std: Option<f64>,    
    pub actions_sells: Option<i32>,    
    pub net_amount_sells: Option<f64>,    
    pub net_amount_sells_mean: Option<f64>,    
    pub net_amount_sells_std: Option<f64>,    
    pub actions_shorts: Option<i32>,    
    pub net_amount_shorts: Option<f64>,    
    pub net_amount_shorts_mean: Option<f64>,    
    pub net_amount_shorts_std: Option<f64>,    
    pub actions_covers: Option<i32>,    
    pub net_amount_covers: Option<f64>,    
    pub net_amount_covers_mean: Option<f64>,    
    pub net_amount_covers_std: Option<f64>    
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AffinityGroupStats {
    pub company: String,
    pub accounts: Vec<String>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub gtedate: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub ltdate: DateTime<Utc>,
    pub allocations: Vec<f64>,
    pub net_amount: f64,    
    pub mean_net_amount_z: Option<f64>,
    pub weighted_mean: f64,
    pub weighted_means: Vec<f64>,
    pub tx_type: String,
    pub security_ticker: String,
    pub t100_std: Option<f64>,
    pub t100_performance: Option<f64>,
    pub t100_wildest: Option<f64>,
    pub f3_performance: Option<f64>,
    pub f30_std: Option<f64>,
    pub f30_performance: Option<f64>,
    pub f30_wildest: Option<f64>,
    pub volume: f64,
    pub market_volume: Option<f64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Statz {
    pub company: String,
    pub affinity_group_stats: AffinityGroupStats,
    pub zscores: Vec<Option<f64>>,
    pub max_distance: Option<f64>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Katz {
    pub company: String,
    pub y_axis: String,
    pub affinity_group_stats: AffinityGroupStats,
    pub max_distance: f64,   // note this is not an option vs above some day need to settle this
    pub cluster: i32
}



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Vatz {
    pub company: String,
    pub wap: WAP,
    pub same_day_contra_pct: Option<f64>,
    pub one_day_contra_pct: Option<f64>,
    pub pctv: f64,
    pub dow: f64,
    pub dom: f64,
    pub participants: i32,    
    pub cluster: i32
}


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AffinityGroupAllocations {
    pub company: String,
    pub accounts: Vec<String>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub min_date: DateTime<Utc>,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub max_date: DateTime<Utc>,
    pub weighted_mean_allocations_buys: Vec<Option<f64>>,
    pub weighted_mean_allocations_buys_std: Vec<Option<f64>>,
    pub weighted_mean_allocations_sells: Vec<Option<f64>>,
    pub weighted_mean_allocations_sells_std: Vec<Option<f64>>,
    pub weighted_mean_allocations_shorts: Vec<Option<f64>>,
    pub weighted_mean_allocations_shorts_std: Vec<Option<f64>>,
    pub weighted_mean_allocations_covers: Vec<Option<f64>>,
    pub weighted_mean_allocations_covers_std: Vec<Option<f64>>
}


#[derive(Debug, Clone)]
pub struct TimeRange {
    pub gtedate: DateTime<Utc>,
    pub ltdate: DateTime<Utc>,
}

impl TimeRange {

    pub fn get_hourlies(self: &Self) -> Result<Vec<TimeRange>,Error> {
        let mut time_ranges = Vec::new();
        let mut dt = self.gtedate;
        while dt < self.ltdate {
            let gtd = dt;
            let ltd = gtd + Duration::hours(1);
            dt = dt + Duration::hours(1);

            let tr = TimeRange {
                gtedate: gtd.clone(),
                ltdate: ltd.clone(),
            };
            time_ranges.push(tr);
        }
        Ok(time_ranges)
    }

}


