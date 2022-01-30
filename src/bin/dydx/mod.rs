use crate::*;

use bson::serde_helpers::chrono_datetime_as_bson_datetime;
use std::time::Duration as HackDuration;
use std::collections::HashMap;
use serde::{Serialize,Deserialize};
//use mongodb::{bson::doc};
use std::fmt; // Import `fmt`
use futures::stream::TryStreamExt;
use mongodb::error::Error as MongoError;
use mongodb::{Client,Collection};
use mongodb::{bson::doc};
use mongodb::options::{FindOptions,FindOneOptions};
use time::Duration;
use std::convert::TryFrom;

const MARKETS_URL: &str = "https://api.dydx.exchange/v3/markets";
const ORDERBOOK_URL: &str = "https://api.dydx.exchange/v3/orderbook";




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

/// This is used to fetch an order book for a market - you could have made it a member of the DYDXMarket, but then you're in the async map issue of returning an owned async item, blah.  Better this way.
pub async fn get_orderbook(market: String) -> Result<TLDYDXOrderbook, Box<dyn Error>> {
    let request_url = format!("{}/{}", ORDERBOOK_URL, market);
    let response = reqwest::get(&request_url).await?;
    let payload: DYDXOrderbook = response.json().await?;

    let mut asks: Vec<(f64,f64)> = Vec::new();
    let mut bids: Vec<(f64,f64)> = Vec::new();

    for ask in &payload.asks {
        let ask_size = ask.size.parse::<f64>().unwrap();
        let ask_price = ask.price.parse::<f64>().unwrap();
        asks.push((ask_size,ask_price));
    }
    for bid in &payload.bids {
        let bid_size = bid.size.parse::<f64>().unwrap();
        let bid_price = bid.price.parse::<f64>().unwrap();
        bids.push((bid_size,bid_price));
    }

    let tld = TLDYDXOrderbook {
        market: market,
        mongo_snapshot_date: Utc::now(),
        asks: asks,
        bids: bids
    };

    Ok(tld)
}



/// A little bit unecessary as the lookback is forced to match the 10 min cap back / forward, but we can refactor that at some point, making those values
/// and array in Mongo, vs multiple fields.
pub struct KDYDX {
    pub k: utils::KafkaSpecs,
    pub lookback: usize,
    pub frequency: u64,
    pub cap: i64,

}

impl KDYDX {

    /// Processes all dydx pairs, which means, every second, gets the quotes, and writes them to a Kafka topic (which must exist).
    /// The broker, topic, and if we add mongo, those should be moved to env variables.  There are some hardcoded stops, which are just dumb.
    pub async fn process_all_markets(self: &Self) -> Result<(), Box<dyn Error>> {

        let brokers = vec![self.k.broker.to_owned()];

        let mut producer = Producer::from_hosts(brokers)
            .with_ack_timeout(HackDuration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()?;


        info!("this process should be daemonized");
        let mut interval = TokioTime::interval(TokioDuration::from_millis(self.frequency));

        // you could fix this size 
        let mut trailing_prices: HashMap<String, Vec<f64>> = HashMap::new(); 
        let mut trailing_open_interest: HashMap<String, Vec<f64>> = HashMap::new(); 

        let mut tmpcnt = 0;
        loop {
            if tmpcnt == self.cap {
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

                let open_interest = item.open_interest.parse::<f64>().unwrap();

                trailing_prices.entry(item.market.to_string()).or_insert(Vec::new()).push(index_price);                        
                // the max interval is 600 seconds, == 10 mins...so our vectors do not need to grow beyond that
                if trailing_prices[&item.market].len() > self.lookback {
                    trailing_prices.get_mut(&item.market).unwrap().remove(0);                    
                    assert_eq!(trailing_prices[&item.market].len(), self.lookback);
                }

                trailing_open_interest.entry(item.market.to_string()).or_insert(Vec::new()).push(open_interest);                        
                // the max interval is 600 seconds, == 10 mins...so our vectors do not need to grow beyond that
                if trailing_open_interest[&item.market].len() > self.lookback {
                    trailing_open_interest.get_mut(&item.market).unwrap().remove(0);                    
                    assert_eq!(trailing_open_interest[&item.market].len(), self.lookback);
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
                let tl_derived_open_interest_change_5s = get_interval_performance(open_interest,5,&item.market,&trailing_open_interest);
                let tl_derived_open_interest_change_10s = get_interval_performance(open_interest,10,&item.market,&trailing_open_interest);
                let tl_derived_open_interest_change_30s = get_interval_performance(open_interest,30,&item.market,&trailing_open_interest);
                let tl_derived_open_interest_change_1m = get_interval_performance(open_interest,60,&item.market,&trailing_open_interest);
                let tl_derived_open_interest_change_5m = get_interval_performance(open_interest,300,&item.market,&trailing_open_interest);
                let tl_derived_open_interest_change_10m = get_interval_performance(open_interest,600,&item.market,&trailing_open_interest);


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
                let max_position_size = item.max_position_size.parse::<f64>().unwrap();
                let asset_resolution = item.asset_resolution.parse::<f64>().unwrap();

                let tlm = TLDYDXMarket {
                    snapshot_date: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                    mongo_snapshot_date: Utc::now(),
                    market: item.market.clone(),
                    status: item.status,
                    base_asset: item.base_asset,
                    quote_asset: item.quote_asset,
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
                    tl_derived_open_interest_change_5s: tl_derived_open_interest_change_5s,
                    tl_derived_open_interest_change_10s: tl_derived_open_interest_change_10s,
                    tl_derived_open_interest_change_30s: tl_derived_open_interest_change_30s,
                    tl_derived_open_interest_change_1m: tl_derived_open_interest_change_1m,
                    tl_derived_open_interest_change_5m: tl_derived_open_interest_change_5m,
                    tl_derived_open_interest_change_10m: tl_derived_open_interest_change_10m,
                    next_funding_rate: next_funding_rate,
                    next_funding_at: item.next_funding_at,
                    min_order_size: min_order_size,
                    instrument_type: item.instrument_type,
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
                    topic: &self.k.topic,
                    partition: -1,
    //                key: (),
                    key: item.market,
                    value: data_as_bytes,
                })?;
            }
            interval.tick().await; 
        }

        Ok(())
    }




}



/// So you have the general Kafka Details and Mongo details in the KafkaMongo struct, and you add the Mongo target, and call consume.  Should be a trait.
pub struct KMDYDX<'a> {
    pub km: utils::KafkaMongo<'a, >,
    pub dydxcol: mongodb::Collection::<TLDYDXMarket>
}

impl KMDYDX<'_> {

    /// This will iterate all messages in the kafka topic dydx-markets, and and write to mongo (with a date lookup that needs refactoring).
    pub async fn consume(self: &Self) -> Result<(), Box<dyn Error>> {


        let brokers = vec![self.km.k.broker.to_owned()];

        let mut max_market_date_hm = HashMap::new();
        for dydxm in get_markets().await? {
            let edydxm = DYDXM::DYDXMarket(dydxm.clone());
            let last_updated = edydxm.get_last_migrated_from_kafka(&self.dydxcol).await?;
            max_market_date_hm.entry(dydxm.market.clone()).or_insert(last_updated);                
        }


        let mut con = Consumer::from_hosts(brokers)
            .with_topic(self.km.k.topic.to_string())
    //                .with_group(group)
            .with_fallback_offset(FetchOffset::Earliest)
    //        .with_fallback_offset(FetchOffset::Latest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .create()?;

        let mut cnt: i64 = 0;
        let mut mcnt: i64 = 0;
        let mut min_quote_date = Utc::now();
        let mut max_quote_date = Utc::now();

        loop {
            let mss = con.poll()?;
            if mss.is_empty() {
                info!("Processed {} messages (only {} mongo valid) for range {} to {} which is {} minutes.", cnt, mcnt, min_quote_date, max_quote_date, max_quote_date.signed_duration_since(min_quote_date).num_minutes());
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

                    debug!("date comp for {} {} {}", des_tldm.market, max_market_date_hm[&des_tldm.market], des_tldm.mongo_snapshot_date);
                    if max_market_date_hm[&des_tldm.market] < des_tldm.mongo_snapshot_date {
                        debug!("Saving to Mongo");
                        let _result = &self.dydxcol.insert_one(&des_tldm, None).await?;                                                                                                    
                        mcnt += 1;
                    }

                    cnt += 1;
                    let quote_date = DateTime::parse_from_rfc3339(&des_tldm.snapshot_date).unwrap().with_timezone(&Utc);
                    
                    if min_quote_date > quote_date {
                        min_quote_date = quote_date;
                    }
                    if max_quote_date < quote_date {
                        max_quote_date = quote_date;
                    }

                }
                let _ = con.consume_messageset(ms);
            }
            con.commit_consumed()?;
        }   

    }




}


pub async fn get_first_snapshot<'a>(market: &'a str) -> Result<Option<DYDXM>, Box<MongoError>> {
    let client = Client::with_uri_str(&Config::from_env().expect("Server configuration").local_mongo).await?;
    let database = client.database(&Config::from_env().expect("Server configuration").tldb);
    let dydxcol = database.collection::<TLDYDXMarket>(THE_TRADELLAMA_DYDX_SNAPSHOT_COLLECTION);


    let filter = doc! {"market": market};
    let find_options = FindOneOptions::builder().sort(doc! { "snapshot_date":1}).build();
    let d = dydxcol.find_one(filter, find_options).await?;
    match d {
        Some(snap) => Ok(Some(DYDXM::TLDYDXMarket(snap))),
        None => Ok(None),
    }


}

/// Useful to have a struct for certain variables, as the data would be helpful in rendering to an artifact outside the calcs.  Note that we use a snap count as an i64, vs usual lt date, as it is a reminder of the assumption about the quotes being one second intervals.  You could add an assert that the limit is close to 100 (or equal).  This should move to utils and become useful for exchanges.  
#[derive(Debug, Clone)]
pub struct ClusterConfiguration {
    pub gtedate: DateTime<Utc>,
    pub ltdate: DateTime<Utc>,
    pub snap_count: i64 // I could use the less than model, but this is a reminder that this is a 1 second assumption on each snap
}

impl ClusterConfiguration {


    async fn get_range_of_orderbook<'a>(self: &Self, market: &'a str, dydxcol: &Collection<TLDYDXOrderbook>) -> Result<Vec<TLDYDXOrderbook>, MongoError> {
        let filter = doc! {"mongo_snapshot_date": {"$gte": self.gtedate, "$lt": self.ltdate}, "market":market};
        debug!("{:?}", filter);
        let find_options = FindOptions::builder().sort(doc! { "mongo_snapshot_date":1}).build();
        let mut cursor = dydxcol.find(filter, find_options).await?;
        let mut sss: Vec<TLDYDXOrderbook> = Vec::new();
        while let Some(des_tldm) = cursor.try_next().await? {
            sss.push(des_tldm);
        }
        Ok(sss)
    }


    async fn get_range_of_quotes<'a>(self: &Self, market: &'a str, dydxcol: &Collection<TLDYDXMarket>) -> Result<Vec<TLDYDXMarket>, MongoError> {
        let filter = doc! {"mongo_snapshot_date": {"$gte": self.gtedate, "$lt": self.ltdate}, "market":market};
        debug!("{:?}", filter);
        let find_options = FindOptions::builder().sort(doc! { "mongo_snapshot_date":1}).build();
        let mut cursor = dydxcol.find(filter, find_options).await?;
        let mut sss: Vec<TLDYDXMarket> = Vec::new();
        while let Some(des_tldm) = cursor.try_next().await? {
            sss.push(des_tldm);
        }
        Ok(sss)
    }


    fn sixlet(self: &Self, vfs: &[f64]) -> (f64,f64,f64,f64,f64,f64) {
        let beginning_index_price = vfs[0];
        let ending_index_price = vfs[vfs.len()-1];
        let index_performance = (ending_index_price - beginning_index_price) / beginning_index_price;
        let index_prices_std = std_deviation(&vfs).unwrap_or(0.); // no bueno
        let index_prices_mean = mean(&vfs).unwrap_or(1.); // no bueno
        let index_prices_normalized_std = index_prices_std / index_prices_mean; // that's really no bueno BUT likely never not going to have a return :)
        (beginning_index_price,ending_index_price,index_performance,index_prices_std,index_prices_mean,index_prices_normalized_std)
    }




    /// This will query the mongo dydx collection (migrated from kafka consumer), and build a vector for clustering, and write that return set to a csv in /tmp.  We do NOT need to process that with the consumer, as it doesn't have a real time need.  It writes a double kmeans return set to one cluster bomb, and a triple (with perf) to another.  You cannot  use generic collection, need the supporting struct (vs TimeRange), because you're using find.
    pub async fn orderbook_spread_price_volatility<'a>(self: &Self, dydxcol: &Collection<TLDYDXMarket>, dydxobcol: &Collection<TLDYDXOrderbook>) -> Result<(), Box<dyn Error>> {

        let hack_for_fname = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let fname = format!("{}{}-{}{}.csv","/tmp/",hack_for_fname,"cluster_bomb_triple_","obspv");
        let mut wtr3 = Writer::from_path(fname)?;
        let mut market_vectors_triple: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        let mut index_prices: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set

        for mkt in dydx::get_markets().await.unwrap() {

            let tlrob = self.get_range_of_orderbook(&mkt.market,dydxobcol).await?;
            debug!("tlrob length is {} for {}", tlrob.len(), &mkt.market);

            for (cnt, tlob) in tlrob.iter().enumerate() {

                let tlrq = tlob.get_forward_500_quotes(dydxcol).await?;

                if tlrq.len() < 500 {
                    warn!("LIKELY YOUR DATA SET IS NEARING ITs END - should not happen on prod, as long as roughly 10 or 15 minutes has pased from ingestion to running this.  DEV ISSUE LIKELY");
                    warn!("BOUNDS {} {}", tlob.mongo_snapshot_date, tlrq.len());
                    warn!("Skipping");                    
                    continue;
                }

                if let Some(_vol10m) = tlrq[0].tl_derived_price_vol_10m { // the first would be the closest to the order book date
                    index_prices.entry(tlob.market.to_string()).or_insert(Vec::new()).push(tlrq[0].index_price);
                    let vol = tlrq[0].tl_derived_price_vol_10m.unwrap_or(0.);       // change this uwrap should check for none and not insert either HACK
                    let mn = tlrq[0].tl_derived_price_mean_10m.unwrap_or(1.);       // change this uwrap should check for none and not insert either, cannot divide by zero HACK                                             

                    debug!("length of tlrq is {} and cnt is {} and snapcount is {}", tlrq.len(), cnt, self.snap_count);
                    //let vfut = tlrq[0].get_next_n_snapshots_from_vec(cnt as i64, self.snap_count,&tlrq); // this can have cnt as 0 vs iterator, as the vec is always "fresh" per iteration of the orderbook snapshot
                    let vfut = tlrq[0].get_next_n_snapshots_from_vec(0, self.snap_count,&tlrq); // this can have cnt as 0 vs iterator, as the vec is always "fresh" per iteration of the orderbook snapshot

                    if let Some(snaps) = vfut {
                        market_vectors_triple.entry(tlrq[0].market.to_string()).or_insert(Vec::new()).push(tlob.bid_ask_tally().2);
                        market_vectors_triple.entry(tlrq[0].market.to_string()).or_insert(Vec::new()).push(vol / mn);
                        let fut_index_price = snaps[snaps.len()-1].index_price;
                        let delta = (fut_index_price - tlrq[0].index_price) / tlrq[0].index_price;
                        market_vectors_triple.entry(tlrq[0].market.to_string()).or_insert(Vec::new()).push(delta);
                    }

                }
                debug!("{} {}", market_vectors_triple.len(), cnt);

            }

        }



        let mut index_prices_tuple: HashMap<String, (f64,f64,f64,f64,f64,f64)> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        for (ipsk,ipsv) in &index_prices {
            debug!("ipsk");
            let stup = self.sixlet(&ipsv);
            index_prices_tuple.insert(ipsk.clone(),stup);
        }


        if market_vectors_triple.is_empty() {
            warn!("I really cannot say.");
        }                    
        for (key,value) in market_vectors_triple {

            info!("{} has {} which is {} data points, on range {} to {} - BAD calc, cause it's thirds.", key, value.len(), value.len() as f64 * 0.5, self.gtedate, self.ltdate);
            let km_for_v_triple = do_triple_kmeans(&value);                    
            debug!("Have a return set of length {} for {} from the kmeans call, matching 1/2 {} {}.", km_for_v_triple.len(), key, value.len(), value.len() as f64 * 0.5);
            for (idx, kg) in km_for_v_triple.iter().enumerate() {
                debug!("{} from {} {} {}", kg, &value[idx*3], &value[(idx*3)+1], &value[(idx*3)+2]);
                let new_cluster_bomb = ClusterBombTriple {
                    market: &key,
                    min_date: &self.gtedate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    max_date: &self.ltdate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    minutes: self.ltdate.signed_duration_since(self.gtedate).num_minutes(),
                    interval_return: index_prices_tuple[&key].2,
                    interval_std: index_prices_tuple[&key].5,                    
                    float_one: value[idx*3],
                    float_two: value[(idx*3)+1],
                    float_three: value[(idx*3)+2],
                    group: *kg
                };
                println!("{}", new_cluster_bomb);
                wtr3.serialize(new_cluster_bomb)?;

            }
        }

        wtr3.flush()?;
        Ok(())


    }




    /// This will query the mongo dydx collection (migrated from kafka consumer), and build a vector for clustering, and write that return set to a csv in /tmp.  We do NOT need to process that with the consumer, as it doesn't have a real time need.  It writes a double kmeans return set to one cluster bomb, and a triple (with perf) to another.  You cannot  use generic collection, need the supporting struct (vs TimeRange), because you're using find.
    /// Current version assumes kmeans (and interval performance, std) in R - just makes the server side a processing enging for aggregation.
    pub async fn index_oracle_price_volatility<'a>(self: &Self, dydxcol: &Collection<TLDYDXMarket>) -> Result<(), Box<dyn Error>> {

        let hack_for_fname = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let fname = format!("{}{}-{}{}.csv","/tmp/",hack_for_fname,"cluster_bomb_triple_","iopv");

        let mut wtr3 = Writer::from_path(fname)?;
        let mut market_vectors_triple: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        let mut index_prices: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set

        for mkt in dydx::get_markets().await.unwrap() {

            let tldmv = self.get_range_of_quotes(&mkt.market,dydxcol).await?;

            for (cnt, des_tldm) in tldmv.iter().enumerate() {

                if let Some(_vol10m) = des_tldm.tl_derived_price_vol_10m { // you can use the 10m check or any of them, as obviously narrow bands would exist
                    index_prices.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.index_price);
                    let vol = des_tldm.tl_derived_price_vol_10m.unwrap_or(0.);       // change this uwrap should check for none and not insert either HACK
                    let mn = des_tldm.tl_derived_price_mean_10m.unwrap_or(1.);       // change this uwrap should check for none and not insert either, cannot divide by zero HACK                                             

                    // this will use the existing vector of snapshots, till it gets close to the backend (ltdate), as then it needs to query beyond (in the 3min world kind of silly but forward look can be configured longer at some point)
                    let differential_padding = 1000;
                    let differential_cnt = (tldmv.len()-cnt) as i64;
                    let differential_cutoff = (28*self.snap_count) + differential_padding; // 28 pairs * snap count which is the lookahead, plust some padding
                    let vfut = if differential_cnt > differential_cutoff {
                        debug!("using the new method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots_from_vec(cnt as i64, self.snap_count,&tldmv)
                    } else {
                        debug!("using the old method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots(self.snap_count,&dydxcol).await?
                    };


                    if let Some(snaps) = vfut {
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.tl_derived_index_oracle_spread);
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(vol / mn);
                        let fut_index_price = snaps[snaps.len()-1].index_price;
                        let delta = (fut_index_price - des_tldm.index_price) / des_tldm.index_price;
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(delta);

                        let new_3de = ThreeDimensionalExtract { // now doing the kmeans in R
                            market: &des_tldm.market,
                            min_date: &self.gtedate.to_rfc3339_opts(SecondsFormat::Secs, true),
                            max_date: &self.ltdate.to_rfc3339_opts(SecondsFormat::Secs, true),
                            minutes: self.ltdate.signed_duration_since(self.gtedate).num_minutes(),
                            float_one: des_tldm.tl_derived_index_oracle_spread,
                            float_two: vol / mn,
                            float_three: delta,
                        };
                        wtr3.serialize(new_3de)?;
                    }

                }
            }
        }

        wtr3.flush()?;
        Ok(())

    }




    /// This will query the mongo dydx collection (migrated from kafka consumer), and build a vector for clustering, and write that return set to a csv in /tmp.  This is a trio only, open interest (trail), volatility (absolute), price (post).
    pub async fn open_interest_price_volatility<'a>(self: &Self, dydxcol: &Collection<TLDYDXMarket>) -> Result<(), Box<dyn Error>> {


        let hack_for_fname = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let fname = format!("{}{}-{}{}.csv","/tmp/",hack_for_fname,"cluster_bomb_triple_","oipv");

        let mut wtr3 = Writer::from_path(fname)?;

        let mut market_vectors_triple: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        let mut index_prices: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set


        for mkt in dydx::get_markets().await.unwrap() {

            let tldmv = self.get_range_of_quotes(&mkt.market,dydxcol).await?;

    //        for (cnt, des_tldm) in self.get_range_of_quotes(dydxcol).await?.iter().enumerate() {
            for (cnt, des_tldm) in tldmv.iter().enumerate() {

                if let Some(_vol10m) = des_tldm.tl_derived_price_vol_10m { // you can use the 10m check or any of them, as obviously narrow bands would exist
                    index_prices.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.index_price);
                    let vol = des_tldm.tl_derived_price_vol_10m.unwrap_or(0.);       // change this uwrap should check for none and not insert either HACK
                    let mn = des_tldm.tl_derived_price_mean_10m.unwrap_or(1.);       // change this uwrap should check for none and not insert either, cannot divide by zero HACK                                             



                    // this will use the existing vector of snapshots, till it gets close to the backend (ltdate), as then it needs to query beyond (in the 3min world kind of silly but forward look can be configured longer at some point)
                    let differential_padding = 1000;
                    let differential_cnt = (tldmv.len()-cnt) as i64;
                    let differential_cutoff = (28*self.snap_count) + differential_padding; // 28 pairs * snap count which is the lookahead, plust some padding
                    let vfut = if differential_cnt > differential_cutoff {
                        debug!("using the new method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots_from_vec(cnt as i64, self.snap_count,&tldmv)
                    } else {
                        debug!("using the old method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots(self.snap_count,&dydxcol).await?
                    };


                    if let Some(snaps) = vfut {
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.open_interest);
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(vol / mn);
                        let fut_index_price = snaps[snaps.len()-1].index_price;
                        let delta = (fut_index_price - des_tldm.index_price) / des_tldm.index_price;
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(delta);
                    }

                }

                debug!("{} {}", market_vectors_triple.len(), cnt);

            }



        }

        let mut index_prices_tuple: HashMap<String, (f64,f64,f64,f64,f64,f64)> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        for (ipsk,ipsv) in &index_prices {
            debug!("ipsk");
            let stup = self.sixlet(&ipsv);
            index_prices_tuple.insert(ipsk.clone(),stup);
        }


        if market_vectors_triple.is_empty() {
            warn!("I really cannot say.");
        }                    
        for (key,value) in market_vectors_triple {

            info!("{} has {} which is {} data points, on range {} to {} - BAD calc, cause it's thirds.", key, value.len(), value.len() as f64 * 0.5, self.gtedate, self.ltdate);
            let km_for_v_triple = do_triple_kmeans(&value);                    
            debug!("Have a return set of length {} for {} from the kmeans call, matching 1/2 {} {}.", km_for_v_triple.len(), key, value.len(), value.len() as f64 * 0.5);
            for (idx, kg) in km_for_v_triple.iter().enumerate() {
                debug!("{} from {} {} {}", kg, &value[idx*3], &value[(idx*3)+1], &value[(idx*3)+2]);
                let new_cluster_bomb = ClusterBombTriple {
                    market: &key,
                    min_date: &self.gtedate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    max_date: &self.ltdate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    minutes: self.ltdate.signed_duration_since(self.gtedate).num_minutes(),
                    interval_return: index_prices_tuple[&key].2,
                    interval_std: index_prices_tuple[&key].5,                    
                    float_one: value[idx*3],
                    float_two: value[(idx*3)+1],
                    float_three: value[(idx*3)+2],
                    group: *kg
                };
                println!("{}", new_cluster_bomb);
                wtr3.serialize(new_cluster_bomb)?;

            }
        }

        wtr3.flush()?;
        Ok(())

    }



    /// This will query the mongo dydx collection (migrated from kafka consumer), and build a vector for clustering, and write that return set to a csv in /tmp.  This is a trio only, open interest (trail), volatility (trail), price (post).
    pub async fn open_interest_price_volatility_v2<'a>(self: &Self, dydxcol: &Collection<TLDYDXMarket>) -> Result<(), Box<dyn Error>> {

        let hack_for_fname = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let fname = format!("{}{}-{}{}.csv","/tmp/",hack_for_fname,"cluster_bomb_triple_","oipvv2");

        let mut wtr3 = Writer::from_path(fname)?;



        let mut market_vectors_triple: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        let mut index_prices: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set


        for mkt in dydx::get_markets().await.unwrap() {
            let tldmv = self.get_range_of_quotes(&mkt.market,dydxcol).await?;
    //        for (cnt, des_tldm) in self.get_range_of_quotes(dydxcol).await?.iter().enumerate() {
            for (cnt, des_tldm) in tldmv.iter().enumerate() {
                if let Some(_vol10m) = des_tldm.tl_derived_price_vol_10m { // you can use the 10m check or any of them, as obviously narrow bands would exist
                    index_prices.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.index_price);
                    let vol = des_tldm.tl_derived_price_vol_10m.unwrap_or(0.);       // change this uwrap should check for none and not insert either HACK
                    let mn = des_tldm.tl_derived_price_mean_10m.unwrap_or(1.);       // change this uwrap should check for none and not insert either, cannot divide by zero HACK                                             

                    // this will use the existing vector of snapshots, till it gets close to the backend (ltdate), as then it needs to query beyond (in the 3min world kind of silly but forward look can be configured longer at some point)
                    let differential_padding = 1000;
                    let differential_cnt = (tldmv.len()-cnt) as i64;
                    let differential_cutoff = (28*self.snap_count) + differential_padding; // 28 pairs * snap count which is the lookahead, plust some padding
                    let vfut = if differential_cnt > differential_cutoff {
                        debug!("using the new method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots_from_vec(cnt as i64, self.snap_count,&tldmv)
                    } else {
                        debug!("using the old method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots(self.snap_count,&dydxcol).await?
                    };


                    if let Some(snaps) = vfut {
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.tl_derived_open_interest_change_10m.unwrap_or(0.));
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(vol / mn);
                        let fut_index_price = snaps[snaps.len()-1].index_price;
                        let delta = (fut_index_price - des_tldm.index_price) / des_tldm.index_price;
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(delta);
                    }

                }
                debug!("{} {}", market_vectors_triple.len(), cnt);
            }
        }


        let mut index_prices_tuple: HashMap<String, (f64,f64,f64,f64,f64,f64)> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        for (ipsk,ipsv) in &index_prices {
            debug!("ipsk");
            let stup = self.sixlet(&ipsv);
            index_prices_tuple.insert(ipsk.clone(),stup);
        }



        if market_vectors_triple.is_empty() {
            warn!("I really cannot say.");
        }                    
        for (key,value) in market_vectors_triple {

            info!("{} has {} which is {} data points, on range {} to {} - BAD calc, cause it's thirds.", key, value.len(), value.len() as f64 * 0.5, self.gtedate, self.ltdate);
            let km_for_v_triple = do_triple_kmeans(&value);                    
            debug!("Have a return set of length {} for {} from the kmeans call, matching 1/2 {} {}.", km_for_v_triple.len(), key, value.len(), value.len() as f64 * 0.5);
            for (idx, kg) in km_for_v_triple.iter().enumerate() {
                debug!("{} from {} {} {}", kg, &value[idx*3], &value[(idx*3)+1], &value[(idx*3)+2]);
                let new_cluster_bomb = ClusterBombTriple {
                    market: &key,
                    min_date: &self.gtedate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    max_date: &self.ltdate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    minutes: self.ltdate.signed_duration_since(self.gtedate).num_minutes(),
                    interval_return: index_prices_tuple[&key].2,
                    interval_std: index_prices_tuple[&key].5,                    
                    float_one: value[idx*3],
                    float_two: value[(idx*3)+1],
                    float_three: value[(idx*3)+2],
                    group: *kg
                };
                println!("{}", new_cluster_bomb);
                wtr3.serialize(new_cluster_bomb)?;

            }
        }

        wtr3.flush()?;
        Ok(())

    }






    /// This will query the mongo dydx collection (migrated from kafka consumer), and build a vector for clustering, and write that return set to a csv in /tmp.  This is a trio only, open interest (trail), volatility (trail), price (post).
    pub async fn funding_rate_price_volatility<'a>(self: &Self, dydxcol: &Collection<TLDYDXMarket>) -> Result<(), Box<dyn Error>> {

        let hack_for_fname = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let fname = format!("{}{}-{}{}.csv","/tmp/",hack_for_fname,"cluster_bomb_triple_","nfrpv");

        let mut wtr3 = Writer::from_path(fname)?;

        let mut market_vectors_triple: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        let mut index_prices: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set



        for mkt in dydx::get_markets().await.unwrap() {
            let tldmv = self.get_range_of_quotes(&mkt.market,dydxcol).await?;
    //        for (cnt, des_tldm) in self.get_range_of_quotes(dydxcol).await?.iter().enumerate() {
            for (cnt, des_tldm) in tldmv.iter().enumerate() {
                if let Some(_vol10m) = des_tldm.tl_derived_price_vol_10m { // you can use the 10m check or any of them, as obviously narrow bands would exist
                    index_prices.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.index_price);
                    let vol = des_tldm.tl_derived_price_vol_10m.unwrap_or(0.);       // change this uwrap should check for none and not insert either HACK
                    let mn = des_tldm.tl_derived_price_mean_10m.unwrap_or(1.);       // change this uwrap should check for none and not insert either, cannot divide by zero HACK                                             

                    // this will use the existing vector of snapshots, till it gets close to the backend (ltdate), as then it needs to query beyond (in the 3min world kind of silly but forward look can be configured longer at some point)
                    let differential_padding = 1000;
                    let differential_cnt = (tldmv.len()-cnt) as i64;
                    let differential_cutoff = (28*self.snap_count) + differential_padding; // 28 pairs * snap count which is the lookahead, plust some padding
                    let vfut = if differential_cnt > differential_cutoff {
                        debug!("using the new method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots_from_vec(cnt as i64, self.snap_count,&tldmv)
                    } else {
                        debug!("using the old method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots(self.snap_count,&dydxcol).await?
                    };

                    if let Some(snaps) = vfut {
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.next_funding_rate);
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(vol / mn);
                        let fut_index_price = snaps[snaps.len()-1].index_price;
                        let delta = (fut_index_price - des_tldm.index_price) / des_tldm.index_price;
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(delta);
                    }
                }
                debug!("{} {}", market_vectors_triple.len(), cnt);
            }
        }

        let mut index_prices_tuple: HashMap<String, (f64,f64,f64,f64,f64,f64)> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        for (ipsk,ipsv) in &index_prices {
            debug!("ipsk");
            let stup = self.sixlet(&ipsv);
            index_prices_tuple.insert(ipsk.clone(),stup);
        }


        if market_vectors_triple.is_empty() {
            warn!("I really cannot say.");
        }                    
        for (key,value) in market_vectors_triple {
            info!("{} has {} which is {} data points, on range {} to {} - BAD calc, cause it's thirds.", key, value.len(), value.len() as f64 * 0.5, self.gtedate, self.ltdate);
            let km_for_v_triple = do_triple_kmeans(&value);                    
            debug!("Have a return set of length {} for {} from the kmeans call, matching 1/2 {} {}.", km_for_v_triple.len(), key, value.len(), value.len() as f64 * 0.5);
            for (idx, kg) in km_for_v_triple.iter().enumerate() {
                debug!("{} from {} {} {}", kg, &value[idx*3], &value[(idx*3)+1], &value[(idx*3)+2]);
                let new_cluster_bomb = ClusterBombTriple {
                    market: &key,
                    min_date: &self.gtedate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    max_date: &self.ltdate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    minutes: self.ltdate.signed_duration_since(self.gtedate).num_minutes(),
                    interval_return: index_prices_tuple[&key].2,
                    interval_std: index_prices_tuple[&key].5,                    
                    float_one: value[idx*3],
                    float_two: value[(idx*3)+1],
                    float_three: value[(idx*3)+2],
                    group: *kg
                };
                println!("{}", new_cluster_bomb);
                wtr3.serialize(new_cluster_bomb)?;

            }
        }

        wtr3.flush()?;
        Ok(())

    }






    /// This will query the mongo dydx collection (migrated from kafka consumer), and build a vector for clustering, and write that return set to a csv in /tmp.  This is a trio only, open interest (trail), volatility (trail), price (post).
    pub async fn volatility_delta_price_volatility<'a>(self: &Self, dydxcol: &Collection<TLDYDXMarket>) -> Result<(), Box<dyn Error>> {

        let hack_for_fname = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        let fname_all = format!("{}{}-{}{}.csv","/tmp/",hack_for_fname,"cluster_bomb_triple_","vdpv");
        let fname_negative = format!("{}{}-{}{}.csv","/tmp/",hack_for_fname,"cluster_bomb_triple_","negvdpv");

        let mut wtr_all_deltas = Writer::from_path(fname_all)?;
        let mut wtr_negative_deltas = Writer::from_path(fname_negative)?;


        let mut market_vectors_triple: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        let mut market_vectors_triple_bonused: HashMap<String, Vec<(f64,f64,String)>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set

        let mut market_vectors_triple_negative: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        let mut market_vectors_triple_negative_bonused: HashMap<String, Vec<(f64,f64,String)>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set

        let mut index_prices: HashMap<String, Vec<f64>> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set


        for mkt in dydx::get_markets().await.unwrap() {
            let tldmv = self.get_range_of_quotes(&mkt.market,dydxcol).await?;
    //        for (cnt, des_tldm) in self.get_range_of_quotes(dydxcol).await?.iter().enumerate() {
            for (cnt, des_tldm) in tldmv.iter().enumerate() {
                if let Some(_vol10m) = des_tldm.tl_derived_price_vol_10m { // you can use the 10m check or any of them, as obviously narrow bands would exist
                    index_prices.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.index_price);
                    let vol = des_tldm.tl_derived_price_vol_10m.unwrap_or(0.);       // change this uwrap should check for none and not insert either HACK
                    let mn = des_tldm.tl_derived_price_mean_10m.unwrap_or(1.);       // change this uwrap should check for none and not insert either, cannot divide by zero HACK                                             

                    // this will use the existing vector of snapshots, till it gets close to the backend (ltdate), as then it needs to query beyond (in the 3min world kind of silly but forward look can be configured longer at some point)
                    let differential_padding = 1000;
                    let differential_cnt = (tldmv.len()-cnt) as i64;
                    let differential_cutoff = (28*self.snap_count) + differential_padding; // 28 pairs * snap count which is the lookahead, plust some padding
                    let vfut = if differential_cnt > differential_cutoff {
//                        debug!("using the new method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots_from_vec(cnt as i64, self.snap_count,&tldmv)
                    } else {
//                        debug!("using the old method at: {:?} {:?} {:?} {:?} {:?}", &mkt.market, cnt, self.snap_count, differential_cnt, differential_cutoff);
                        des_tldm.get_next_n_snapshots(self.snap_count,&dydxcol).await?
                    };


                    if let Some(snaps) = vfut {
                        // i don't reuse above vol as i want the unwrap hack to be 1 for div by zero shit

                        let vol10m = des_tldm.tl_derived_price_vol_10m.unwrap_or(1.);       // change this uwrap should check for none and not insert either HACK
                        let vol1m = des_tldm.tl_derived_price_vol_1m.unwrap_or(0.);       // change this uwrap should check for none and not insert either HACK
                        let vold = (vol1m - vol10m) / vol10m;

                        debug!("10 1m and vold: {} {} {}", vol10m, vol1m, vold);

                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(vold);
                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(vol / mn);
                        let fut_index_price = snaps[snaps.len()-1].index_price;
                        let delta = (fut_index_price - des_tldm.index_price) / des_tldm.index_price;

                        market_vectors_triple.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(delta);
                        market_vectors_triple_bonused.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(
                            (
                            des_tldm.tl_derived_price_change_10m.unwrap_or(0.),
                            des_tldm.tl_derived_open_interest_change_10m.unwrap_or(0.),
                            des_tldm.mongo_snapshot_date.to_rfc3339_opts(SecondsFormat::Secs, true)
                            )
                        );


                        if vold < 0. {

                            // the negative can use the vol del abs for size, and you can then cluster t10 perf, vold delta, and price delta - can do same with the > 0.
                            market_vectors_triple_negative.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(vold);
                            market_vectors_triple_negative.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(des_tldm.tl_derived_price_change_10m.unwrap_or(0.));
                            market_vectors_triple_negative.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(delta);

                            market_vectors_triple_negative_bonused.entry(des_tldm.market.to_string()).or_insert(Vec::new()).push(
                                (
                                des_tldm.tl_derived_price_change_1m.unwrap_or(0.),
                                des_tldm.tl_derived_open_interest_change_10m.unwrap_or(0.),
                                des_tldm.mongo_snapshot_date.to_rfc3339_opts(SecondsFormat::Secs, true)
                                )
                            );


                        }

                        
                    }

                }
                debug!("{} {}", market_vectors_triple.len(), cnt);
            }
        }


        let mut index_prices_tuple: HashMap<String, (f64,f64,f64,f64,f64,f64)> = HashMap::new(); // forced to spell out type, to use len calls, otherwise would have to loop a get markets return set
        for (ipsk,ipsv) in &index_prices {
            debug!("ipsk");
            let stup = self.sixlet(&ipsv);
            index_prices_tuple.insert(ipsk.clone(),stup);
        }



        if market_vectors_triple.is_empty() {
            warn!("I really cannot say.");
        }                    
        for (key,value) in market_vectors_triple {

            info!("{} has {} which is {} data points, on range {} to {} - BAD calc, cause it's thirds.", key, value.len(), value.len() as f64 * 0.5, self.gtedate, self.ltdate);
            let km_for_v_triple = do_triple_kmeans(&value);                    
            debug!("Have a return set of length {} for {} from the kmeans call, matching 1/2 {} {}.", km_for_v_triple.len(), key, value.len(), value.len() as f64 * 0.5);
            for (idx, kg) in km_for_v_triple.iter().enumerate() {
                debug!("{} from {} {} {}", kg, &value[idx*3], &value[(idx*3)+1], &value[(idx*3)+2]);
                let new_cluster_bomb = ClusterBombTripleBonused {
                    market: &key,
                    min_date: &self.gtedate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    max_date: &self.ltdate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    minutes: self.ltdate.signed_duration_since(self.gtedate).num_minutes(),
                    interval_return: index_prices_tuple[&key].2,
                    interval_std: index_prices_tuple[&key].5,                    
                    float_one: value[idx*3],
                    float_two: value[(idx*3)+1],
                    float_three: value[(idx*3)+2],
                    group: *kg,
                    tl_derived_price_change_1m: market_vectors_triple_bonused[&key][idx].0,
                    tl_derived_open_interest_change_10m: market_vectors_triple_bonused[&key][idx].1,
                    mongo_snapshot_date: &market_vectors_triple_bonused[&key][idx].2
                };
                println!("{}", new_cluster_bomb);
                wtr_all_deltas.serialize(new_cluster_bomb)?;

            }
        }






        if market_vectors_triple_negative.is_empty() {
            warn!("I really cannot say.");
        }                    
        for (key,value) in market_vectors_triple_negative {

            info!("{} has {} which is {} data points, on range {} to {} - BAD calc, cause it's thirds.", key, value.len(), value.len() as f64 * 0.5, self.gtedate, self.ltdate);
            let km_for_v_triple = do_triple_kmeans(&value);                    
            debug!("Have a return set of length {} for {} from the kmeans call, matching 1/2 {} {}.", km_for_v_triple.len(), key, value.len(), value.len() as f64 * 0.5);
            for (idx, kg) in km_for_v_triple.iter().enumerate() {
                debug!("{} from {} {} {}", kg, &value[idx*3], &value[(idx*3)+1], &value[(idx*3)+2]);
                let new_cluster_bomb = ClusterBombTripleBonused {
                    market: &key,
                    min_date: &self.gtedate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    max_date: &self.ltdate.to_rfc3339_opts(SecondsFormat::Secs, true),
                    minutes: self.ltdate.signed_duration_since(self.gtedate).num_minutes(),
                    interval_return: index_prices_tuple[&key].2,
                    interval_std: index_prices_tuple[&key].5,                    
                    float_one: value[idx*3],
                    float_two: value[(idx*3)+1],
                    float_three: value[(idx*3)+2],
                    group: *kg,
                    tl_derived_price_change_1m: market_vectors_triple_negative_bonused[&key][idx].0,
                    tl_derived_open_interest_change_10m: market_vectors_triple_negative_bonused[&key][idx].1,
                    mongo_snapshot_date: &market_vectors_triple_negative_bonused[&key][idx].2
                };
                println!("{}", new_cluster_bomb);
                wtr_negative_deltas.serialize(new_cluster_bomb)?;

            }
        }


        wtr_all_deltas.flush()?;
        wtr_negative_deltas.flush()?;
        Ok(())

    }







}


pub enum DYDXM {
    TLDYDXMarket(TLDYDXMarket),
    DYDXMarket(DYDXMarket)
}

impl DYDXM {
    /// Used to get the last update date for a given market, so you do not re-consume and save to Mongo.  If no date exists (say, first run), it just goes back an hour.
    pub async fn get_last_migrated_from_kafka<'a>(self: &Self, collection: &Collection<TLDYDXMarket>) -> Result<DateTime<Utc>, MongoError> {

        let market = match &*self {
            DYDXM::TLDYDXMarket(t) => &t.market, 
            DYDXM::DYDXMarket(d) => &d.market, 
        };

        let filter = doc! {"market": market};
        let find_options = FindOptions::builder().sort(doc! { "mongo_snapshot_date":-1}).limit(1).build();
        let mut cursor = collection.find(filter, find_options).await?;

        let mut big_bang = chrono::offset::Utc::now();
        big_bang = big_bang - Duration::minutes(60000);

        while let Some(cm) = cursor.try_next().await? {
            big_bang = cm.mongo_snapshot_date;
        }
        Ok(big_bang)

    }

}

/// This is the object we work with after we consume from the dydx endpoint, the biggest implication being we have floats vs strings
/// Because Kafka likes strings for timestamps (we had string slices) and Mongo wants UTC, in order to have deserialize work properly, we changed to Strings.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TLDYDXMarket {
    pub snapshot_date: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub mongo_snapshot_date: DateTime<Utc>,
    pub market: String,
    pub status: String,
    pub base_asset: String,
    pub quote_asset: String,
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
    pub tl_derived_open_interest_change_5s: Option<f64>,
    pub tl_derived_open_interest_change_10s: Option<f64>,
    pub tl_derived_open_interest_change_30s: Option<f64>,
    pub tl_derived_open_interest_change_1m: Option<f64>,
    pub tl_derived_open_interest_change_5m: Option<f64>,
    pub tl_derived_open_interest_change_10m: Option<f64>,
    pub next_funding_rate: f64,
    pub next_funding_at: String,
    pub min_order_size: f64,
    pub instrument_type: String,
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

impl TLDYDXMarket {
    ///Get the price movement after a given trade (self), to use for clustering on possible signal generation.
    pub async fn get_next_n_snapshots(self: &Self, n: i64, collection: &Collection<TLDYDXMarket>) -> Result<Option<Vec<TLDYDXMarket>>,MongoError> {
        let filter = doc! {"market": &self.market, "mongo_snapshot_date": {"$gte": self.mongo_snapshot_date}};
        let find_options = FindOptions::builder().sort(doc! { "mongo_snapshot_date":1}).limit(n).build();
        let mut cursor = collection.find(filter, find_options).await?;
        let mut sss: Vec<TLDYDXMarket> = Vec::new();
        while let Some(ss) = cursor.try_next().await? {
            sss.push(ss.clone());                
        }

        if sss.len() < usize::try_from(n).unwrap_or(0) {
            warn!("Not enough trades following self, likely need to wait and run again. {} {}", sss.len(), n);
            Ok(None)
        } else {
            Ok(Some(sss))        
        }

    }

    /// this doesn't really need to be a method of tldydxmarket but it's a trial for opt anyways
    pub fn get_next_n_snapshots_from_vec(self: &Self, cnt: i64, n: i64, v: &Vec<TLDYDXMarket>) -> Option<Vec<TLDYDXMarket>> {
        let mut sss: Vec<TLDYDXMarket> = Vec::new();
        for i in cnt..cnt+n {
            sss.push(v[i as usize].clone());
        }
        Some(sss)
    }



}


impl fmt::Display for TLDYDXMarket {
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



#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TLDYDXOrderbook {
    pub market: String,
    #[serde(with = "chrono_datetime_as_bson_datetime")]
    pub mongo_snapshot_date: DateTime<Utc>,
    //size, price
    pub asks:  Vec<(f64,f64)>,
    //size, price
    pub bids:  Vec<(f64,f64)>
}

impl TLDYDXOrderbook {
    pub fn bid_ask_tally(&self) -> (f64,f64,f64) {
        let bids: f64 = self.bids.clone().into_iter().map(|e| e.0*e.1).sum();
        let asks: f64 = self.asks.clone().into_iter().map(|e| e.0*e.1).sum();
        let spread = asks-bids;
        (bids,asks,spread)
    }

    /// This is pretty much a hack for orderbook to quotes, to have a large enough lookahead that you can refrain from going forward in the ob loop - and I don't think it's really useful
    pub async fn get_forward_500_quotes<'a>(self: &Self, dydxcol: &Collection<TLDYDXMarket>) -> Result<Vec<TLDYDXMarket>, MongoError> {
        let filter = doc! {"mongo_snapshot_date": {"$gte": self.mongo_snapshot_date}, "market": &self.market};
        debug!("{:?}", filter);
        let find_options = FindOptions::builder().limit(500).sort(doc! { "mongo_snapshot_date":1}).build();
        let mut cursor = dydxcol.find(filter, find_options).await?;
        let mut sss: Vec<TLDYDXMarket> = Vec::new();
        while let Some(des_tldm) = cursor.try_next().await? {
            sss.push(des_tldm);
        }
        Ok(sss)
    }



}

impl fmt::Display for TLDYDXOrderbook {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:<10} {:<10} {:>5} {:>5} {:>10.4} {:>10.4} {:>10.4}", self.market, self.mongo_snapshot_date,self.asks.len(), self.bids.len(), self.bid_ask_tally().0, self.bid_ask_tally().1, self.bid_ask_tally().2)
    }
}





/// This is the basic object for consuming the endpoint, as the api has most things as Strings
#[derive(Deserialize, Debug, Clone)]
pub struct DYDXOrder {
    pub size: String,
    pub price: String
}


/// This is the basic object for consuming the endpoint, as the api has most things as Strings
#[derive(Deserialize, Debug, Clone)]
pub struct DYDXOrderbook {
    pub asks: Vec<DYDXOrder>,
    pub bids: Vec<DYDXOrder>,
}

/// This is the basic object for consuming the endpoint, as the api has most things as Strings
#[derive(Deserialize, Debug, Clone)]
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















#[cfg(test)]
mod tests {

    use mongodb::Client;
    use super::*;

    #[tokio::test]
    async fn dydx_opt_forward_price() {

        env_logger::init(); 

        // almost all use mongo, so declaring for all options
        let client = Client::with_uri_str(&Config::from_env().expect("Server configuration").local_mongo).await.expect("holy sheep shit");
        let database = client.database(&Config::from_env().expect("Server configuration").tldb);
        let dydxcol = database.collection::<TLDYDXMarket>(THE_TRADELLAMA_DYDX_SNAPSHOT_COLLECTION);            

        let time_ranges = utils::get_time_ranges("2021-12-13 00:00:00","2021-12-14 00:00:00","%Y-%m-%d %H:%M:%S",&1).unwrap();
        for tr in time_ranges{
            let iopv = dydx::ClusterConfiguration {
                gtedate: tr.gtedate, 
                ltdate: tr.ltdate, 
                snap_count: 180,
            };

            for mkt in dydx::get_markets().await.unwrap() {
                let tldmv = iopv.get_range_of_quotes(&mkt.market,&dydxcol).await.expect("shit");
                debug!("mkt: {:?} {:?}", mkt.market, tldmv.len());
                for (cnt, des_tldm) in tldmv.iter().enumerate() {
                    if cnt == 10000 {
                        let vfut = des_tldm.get_next_n_snapshots(iopv.snap_count,&dydxcol).await.expect("holy holy");
                        let vfutalt = des_tldm.get_next_n_snapshots_from_vec(cnt as i64,iopv.snap_count,&tldmv).unwrap();
                        if let Some(snaps) = vfut {
                            let fut_index_price = snaps[snaps.len()-1].index_price;
                            let delta = (fut_index_price - des_tldm.index_price) / des_tldm.index_price;
                            let alt_fut_index_price = vfutalt[vfutalt.len()-1].index_price;
                            let alt_delta = (alt_fut_index_price - des_tldm.index_price) / des_tldm.index_price;
                            debug!("{:?} {:?}", delta, alt_delta);
                            assert_eq!(delta, alt_delta);
                        }
                        break;
                    }
                }
            }

        }
    }


    #[tokio::test]
    async fn dydx_still_has_markets() {
        let mkts = dydx::get_markets().await.expect("holy sheep shit");
        assert_ne!(0,mkts.len())
    }


}


