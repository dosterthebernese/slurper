use crate::*;

use std::collections::HashMap;
use serde::{Serialize,Deserialize};
use mongodb::{bson::doc};
use std::fmt; // Import `fmt`


const MARKETS_URL: &str = "https://api.dydx.exchange/v3/markets";

/// This is used to fetch all dydx asset pairs. It returns a vector of markets, which I usually then use to have an async / await pool that run in parallel - you can see this in other modules. Seemed like overkill here. 

pub async fn get_markets() -> Result<Vec<DYDXMarket>, Box<dyn Error>> {

     let mut rvec = Vec::new();

    let request_url = format!("{}", MARKETS_URL);
    let response = reqwest::get(&request_url).await?;

    debug!("here");
    let payload: DYDXMarkets = response.json().await?;
    debug!("and {:?}", payload);

    for (_,v) in payload.markets {
        debug!("{:?}",v);
        rvec.push(v);                    
    }

    Ok(rvec)
} 





/// Processes all dydx pairs, which means, every second, gets the quotes, and writes them to a Kafka topic (which must exist).
/// The broker, topic, and if we add mongo, those should be moved to env variables.  There are some hardcoded stops, which are just dumb.
pub async fn process_all_markets() -> Result<(), Box<dyn Error>> {

    let broker = "localhost:9092";
    let topic = "dydx-markets";

    let mut producer = Producer::from_hosts(vec![broker.to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;


    info!("this process should be daemonized");
    let mut interval = TokioTime::interval(TokioDuration::from_millis(1000));

    // you could fix this size 
    let mut trailing_prices: HashMap<String, Vec<f64>> = HashMap::new(); 

    let mut tmpcnt = 0;
    loop {
        if tmpcnt == 1000000 {
            break;
        } else {
            tmpcnt+=1;
        }




    // An example, of how to wrap:
    // let dcol: Vec<_> = get_asset_pairs().await.unwrap().into_iter().map(|item| process_trades(item)).collect();

        for item in dydx::get_markets().await.unwrap() {

            let index_price = item.index_price.parse::<f64>().unwrap();
            let oracle_price = item.oracle_price.parse::<f64>().unwrap();
            let tl_derived_index_oracle_spread = (index_price - oracle_price) / oracle_price;

            trailing_prices.entry(item.market.to_string()).or_insert(Vec::new()).push(index_price);                        
            // the max interval is 600 seconds, == 10 mins...so our vectors do not need to grow beyond that
            if trailing_prices[&item.market].len() > 600 {
                trailing_prices.get_mut(&item.market).unwrap().remove(0);                    
                assert_eq!(trailing_prices[&item.market].len(), 600);
            }

            let tl_derived_price_change_5s = get_interval_performance(index_price,5,&item.market,&trailing_prices);
            let tl_derived_price_change_10s = get_interval_performance(index_price,10,&item.market,&trailing_prices);
            let tl_derived_price_change_30s = get_interval_performance(index_price,30,&item.market,&trailing_prices);
            let tl_derived_price_change_1m = get_interval_performance(index_price,60,&item.market,&trailing_prices);
            let tl_derived_price_change_5m = get_interval_performance(index_price,300,&item.market,&trailing_prices);
            let tl_derived_price_change_10m = get_interval_performance(index_price,600,&item.market,&trailing_prices);
            let tl_derived_price_vol_1m = get_interval_volatility(60,&item.market,&trailing_prices);
            let tl_derived_price_vol_5m = get_interval_volatility(300,&item.market,&trailing_prices);
            let tl_derived_price_vol_10m = get_interval_volatility(600,&item.market,&trailing_prices);
            let tl_derived_price_mean_1m = get_interval_mean(60,&item.market,&trailing_prices); // not weighted
            let tl_derived_price_mean_5m = get_interval_mean(300,&item.market,&trailing_prices); // not weighted
            let tl_derived_price_mean_10m = get_interval_mean(600,&item.market,&trailing_prices); // not weighted


            let price_change_24h = item.price_change_24h.parse::<f64>().unwrap();
            let next_funding_rate = item.next_funding_rate.parse::<f64>().unwrap();
            let min_order_size = item.min_order_size.parse::<f64>().unwrap();

            let initial_margin_fraction = item.initial_margin_fraction.parse::<f64>().unwrap();
            let maintenance_margin_fraction = item.maintenance_margin_fraction.parse::<f64>().unwrap();
            let baseline_position_size = item.baseline_position_size.parse::<f64>().unwrap();
            let incremental_position_size = item.incremental_position_size.parse::<f64>().unwrap();
            let incremental_initial_margin_fraction = item.incremental_initial_margin_fraction.parse::<f64>().unwrap();
            let volume_24h = item.volume_24h.parse::<f64>().unwrap();
            let trades_24h = item.trades_24h.parse::<f64>().unwrap();
            let open_interest = item.open_interest.parse::<f64>().unwrap();
            let max_position_size = item.max_position_size.parse::<f64>().unwrap();
            let asset_resolution = item.asset_resolution.parse::<f64>().unwrap();

            let tlm = TLDYDXMarket {
                snapshot_date: &Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                market: &item.market,
                status: &item.status,
                base_asset: &item.base_asset,
                quote_asset: &item.quote_asset,
                step_size: item.step_size.parse::<f64>().unwrap(),
                tick_size: item.tick_size.parse::<f64>().unwrap(),
                index_price: index_price,
                oracle_price: oracle_price,
                tl_derived_index_oracle_spread: tl_derived_index_oracle_spread,
                price_change_24h: price_change_24h,
                tl_derived_price_change_5s: tl_derived_price_change_5s,
                tl_derived_price_change_10s: tl_derived_price_change_10s,
                tl_derived_price_change_30s: tl_derived_price_change_30s,
                tl_derived_price_change_1m: tl_derived_price_change_1m,
                tl_derived_price_change_5m: tl_derived_price_change_5m,
                tl_derived_price_change_10m: tl_derived_price_change_10m,
                tl_derived_price_vol_1m: tl_derived_price_vol_1m,
                tl_derived_price_vol_5m: tl_derived_price_vol_5m,
                tl_derived_price_vol_10m: tl_derived_price_vol_10m,
                tl_derived_price_mean_1m: tl_derived_price_mean_1m,
                tl_derived_price_mean_5m: tl_derived_price_mean_5m,
                tl_derived_price_mean_10m: tl_derived_price_mean_10m,
                next_funding_rate: next_funding_rate,
                next_funding_at: &item.next_funding_at,
                min_order_size: min_order_size,
                instrument_type: &item.instrument_type,
                initial_margin_fraction: initial_margin_fraction,
                maintenance_margin_fraction: maintenance_margin_fraction,
                baseline_position_size: baseline_position_size,
                incremental_position_size: incremental_position_size,
                incremental_initial_margin_fraction: incremental_initial_margin_fraction,
                volume_24h: volume_24h,
                trades_24h: trades_24h,
                open_interest: open_interest,
                max_position_size: max_position_size,
                asset_resolution: asset_resolution,
            };

            println!("{}", tlm);
            let data = serde_json::to_string(&tlm).expect("json serialization failed");
            let data_as_bytes = data.as_bytes();

            producer.send(&Record {
                topic,
                partition: -1,
                key: (),
                value: data_as_bytes,
            })?;
        }
        interval.tick().await; 
    }

    Ok(())
}




/// This will iterate all messages in the kafka topic dydx-markets, and build a vector for clustering, and write that return set to a csv in /tmp
pub async fn consume_dydx_topic() -> Result<(), Box<dyn Error>> {

    let mut wtr = Writer::from_path("/tmp/cluster_bombs.csv")?;


    let broker = "localhost:9092";
    let brokers = vec![broker.to_owned()];
    let topic = "dydx-markets";

    let mut con = Consumer::from_hosts(brokers)
        .with_topic(topic.to_string())
//                .with_group(group)
        .with_fallback_offset(FetchOffset::Earliest)
        .with_offset_storage(GroupOffsetStorage::Kafka)
        .create()?;

    let mut market_vectors: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set

    let mut cnt: i64 = 0;
    let mut min_quote_date = Utc::now();
    let mut max_quote_date = Utc::now();

    loop {
        let mss = con.poll()?;
        if mss.is_empty() {
            info!("Processed {} messages for range {} to {} which is {} minutes.", cnt, min_quote_date, max_quote_date, max_quote_date.signed_duration_since(min_quote_date).num_minutes());
            if market_vectors.is_empty() {
                warn!("Not yet 10 mins");
            }                    
            for (key,value) in market_vectors {
                info!("{} has {} which is {} data points, on range {} to {}.", key, value.len(), value.len() as f64 * 0.5, min_quote_date, max_quote_date);
                let km_for_v_duo = do_duo_kmeans(&value);                    
                debug!("Have a return set of length {} for {} from the kmeans call, matching 1/2 {} {}.", km_for_v_duo.len(), key, value.len(), value.len() as f64 * 0.5);
                for (idx, kg) in km_for_v_duo.iter().enumerate() {
                    debug!("{} from {} {}", kg, &value[idx*2], &value[(idx*2)+1]);
                    let new_cluster_bomb = ClusterBomb {
                        market: &key,
                        min_date: &min_quote_date.to_rfc3339_opts(SecondsFormat::Secs, true),
                        max_date: &max_quote_date.to_rfc3339_opts(SecondsFormat::Secs, true),
                        minutes: max_quote_date.signed_duration_since(min_quote_date).num_minutes(),
                        float_one: value[idx*2],
                        float_two: value[(idx*2)+1],
                        group: *kg
                    };
                    println!("{}", new_cluster_bomb);
                    wtr.serialize(new_cluster_bomb)?;

                }
            }
            println!("No messages available right now.");
            wtr.flush()?;
            return Ok(());
        }

        for ms in mss.iter() {
            for m in ms.messages() {
                let mut buf = Vec::with_capacity(1024);
                //println!("{}:{}@{}: {:?}", ms.topic(), ms.partition(), m.offset, m.value);
                buf.extend_from_slice(m.value);
                buf.push(b'\n');                        
                let cb =  str::from_utf8(&buf).unwrap();
                let des_tldm: TLDYDXMarket = serde_json::from_str(cb).unwrap();
                println!("{}", des_tldm);
                cnt += 1;

                let quote_date = DateTime::parse_from_rfc3339(&des_tldm.snapshot_date).unwrap().with_timezone(&Utc);
                
                if min_quote_date > quote_date {
                    min_quote_date = quote_date;
                }
                if max_quote_date < quote_date {
                    max_quote_date = quote_date;
                }

                if let Some(_vol10m) = des_tldm.tl_derived_price_vol_10m { // you can use the 10m check or any of them, as obviously narrow bands would exist
                    market_vectors.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.tl_derived_index_oracle_spread);
                    let vol = des_tldm.tl_derived_price_vol_10m.unwrap_or(0.);       // change this uwrap should check for none and not insert either HACK
                    let mn = des_tldm.tl_derived_price_mean_10m.unwrap_or(1.);       // change this uwrap should check for none and not insert either, cannot divide by zero HACK                                             
//                            market_vectors.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.index_price);                        
                    market_vectors.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(vol / mn);
                }
            }
            let _ = con.consume_messageset(ms);
        }
        con.commit_consumed()?;
    }   

}




































/// This is the object we work with after we consume from the dydx endpoint, the biggest implication being we have floats vs strings
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TLDYDXMarket<'a> {
    pub snapshot_date: &'a str,
    pub market: &'a str,
    pub status: &'a str,
    pub base_asset: &'a str,
    pub quote_asset: &'a str,
    pub step_size: f64,
    pub tick_size: f64,
    pub index_price: f64,
    pub oracle_price: f64,
    pub tl_derived_index_oracle_spread: f64,
    pub price_change_24h: f64,
    pub tl_derived_price_change_5s: Option<f64>,
    pub tl_derived_price_change_10s: Option<f64>,
    pub tl_derived_price_change_30s: Option<f64>,
    pub tl_derived_price_change_1m: Option<f64>,
    pub tl_derived_price_change_5m: Option<f64>,
    pub tl_derived_price_change_10m: Option<f64>,
    pub tl_derived_price_vol_1m: Option<f64>,
    pub tl_derived_price_vol_5m: Option<f64>,
    pub tl_derived_price_vol_10m: Option<f64>,
    pub tl_derived_price_mean_1m: Option<f64>,
    pub tl_derived_price_mean_5m: Option<f64>,
    pub tl_derived_price_mean_10m: Option<f64>,
    pub next_funding_rate: f64,
    pub next_funding_at: &'a str,
    pub min_order_size: f64,
    pub instrument_type: &'a str,
    pub initial_margin_fraction: f64,
    pub maintenance_margin_fraction: f64,
    pub baseline_position_size: f64,
    pub incremental_position_size: f64,
    pub incremental_initial_margin_fraction: f64,
    pub volume_24h: f64,
    pub trades_24h: f64,
    pub open_interest: f64,
    pub max_position_size: f64,
    pub asset_resolution: f64,
}

impl fmt::Display for TLDYDXMarket<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "\n{:<10} {:<10} {:<10} {:>10} {:>10} 
            {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} 
            {:>10.4} 
            {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} 
            {:>10.4} {:>10.4} {:>10.4}  
            {:>10.4} {:>10.4} {:>10.4}  
            {:>10.4}
            {:>10} 
            {:>10.4} 
            {:>10} 
            {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4} {:>10.4}", 
            self.snapshot_date, self.market, self.status, self.base_asset, self.quote_asset, 
            self.step_size, self.tick_size, self.index_price, self.oracle_price, self.tl_derived_index_oracle_spread, 
            self.price_change_24h, 
            self.tl_derived_price_change_5s.unwrap_or(0.),self.tl_derived_price_change_10s.unwrap_or(0.),self.tl_derived_price_change_30s.unwrap_or(0.),self.tl_derived_price_change_1m.unwrap_or(0.),self.tl_derived_price_change_5m.unwrap_or(0.),self.tl_derived_price_change_10m.unwrap_or(0.),
            self.tl_derived_price_vol_1m.unwrap_or(0.),self.tl_derived_price_vol_5m.unwrap_or(0.),self.tl_derived_price_vol_10m.unwrap_or(0.),            
            self.tl_derived_price_mean_1m.unwrap_or(0.),self.tl_derived_price_mean_5m.unwrap_or(0.),self.tl_derived_price_mean_10m.unwrap_or(0.),            
            self.next_funding_rate, 
            self.next_funding_at, 
            self.min_order_size, 
            self.instrument_type, 
            self.initial_margin_fraction, self.maintenance_margin_fraction, self.baseline_position_size, self.incremental_position_size, self.incremental_initial_margin_fraction, self.volume_24h, self.trades_24h, self.open_interest, self.max_position_size, self.asset_resolution)
    }
}


/// This is the basic object for consuming the endpoint, as the api has most things as Strings
#[derive(Deserialize, Debug)]
pub struct DYDXMarket {
    pub market: String,
    pub status: String,
    #[serde(rename(deserialize = "baseAsset"))]
    pub base_asset: String,
    #[serde(rename(deserialize = "quoteAsset"))]
    pub quote_asset: String,
    #[serde(rename(deserialize = "stepSize"))]
    pub step_size: String,
    #[serde(rename(deserialize = "tickSize"))]
    pub tick_size: String,
    #[serde(rename(deserialize = "indexPrice"))]
    pub index_price: String,
    #[serde(rename(deserialize = "oraclePrice"))]
    pub oracle_price: String,
    #[serde(rename(deserialize = "priceChange24H"))]
    pub price_change_24h: String,
    #[serde(rename(deserialize = "nextFundingRate"))]
    pub next_funding_rate: String,
    #[serde(rename(deserialize = "nextFundingAt"))]
    pub next_funding_at: String,
    #[serde(rename(deserialize = "minOrderSize"))]
    pub min_order_size: String,
    #[serde(rename(deserialize = "type"))]
    pub instrument_type: String,
    #[serde(rename(deserialize = "initialMarginFraction"))]
    pub initial_margin_fraction: String,
    #[serde(rename(deserialize = "maintenanceMarginFraction"))]
    pub maintenance_margin_fraction: String,
    #[serde(rename(deserialize = "baselinePositionSize"))]
    pub baseline_position_size: String,
    #[serde(rename(deserialize = "incrementalPositionSize"))]
    pub incremental_position_size: String,
    #[serde(rename(deserialize = "incrementalInitialMarginFraction"))]
    pub incremental_initial_margin_fraction: String,
    #[serde(rename(deserialize = "volume24H"))]
    pub volume_24h: String,
    #[serde(rename(deserialize = "trades24H"))]
    pub trades_24h: String,
    #[serde(rename(deserialize = "openInterest"))]
    pub open_interest: String,
    #[serde(rename(deserialize = "maxPositionSize"))]
    pub max_position_size: String,
    #[serde(rename(deserialize = "assetResolution"))]
    pub asset_resolution: String,

}


impl fmt::Display for DYDXMarket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}", self.market, self.status, self.base_asset, self.quote_asset, self.step_size, self.tick_size, self.index_price, self.oracle_price, self.price_change_24h, self.next_funding_rate, self.next_funding_at, self.min_order_size, self.instrument_type, self.initial_margin_fraction, self.maintenance_margin_fraction, self.baseline_position_size, self.incremental_position_size, self.incremental_initial_margin_fraction, self.volume_24h, self.trades_24h, self.open_interest, self.max_position_size, self.asset_resolution)
    }
}

///This is a wrapper around the api endpoint array of pairs.
#[derive(Deserialize, Debug)]
pub struct DYDXMarkets {
    #[serde(rename(deserialize = "markets"))]
    pub markets: HashMap<String,DYDXMarket>
}
