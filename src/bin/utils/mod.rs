// this use statement gets you access to the lib file - you use crate instead of the package name, who the fuck knows why (see any bin rs)

use crate::*;

use std::fmt; 
use std::collections::HashMap;
use chrono::{NaiveDateTime,DateTime,Utc};
use mongodb::{Collection};
use mongodb::error::Error as MongoError;
use mongodb::{bson::doc};
use mongodb::options::{FindOptions};
use futures::stream::TryStreamExt;
use time::Duration;


#[derive(Debug, Clone)]
pub struct KafkaSpecs {
    pub broker: String,
    pub topic: String
}

#[derive(Debug, Clone)]
pub struct MongoSpecs<'a> {
    pub client: &'a mongodb::Client,
    pub database: &'a mongodb::Database,
}

#[derive(Debug, Clone)]
pub struct KafkaMongo<'a> {
    pub k: KafkaSpecs,
    pub m: MongoSpecs<'a>
}


/// Just a helper utility for getting a TimeRange struct passing two strings, and a formatter.
pub fn get_time_range<'a>(
    d1: &'a str,
    d2: &'a str,
    df: &'a str,
) -> Result<TimeRange, Box<dyn Error>> {
    let d1 = NaiveDateTime::parse_from_str(d1, df)?;
    let d2 = NaiveDateTime::parse_from_str(d2, df)?;
    let utcd1 = DateTime::<Utc>::from_utc(d1, Utc);
    let utcd2 = DateTime::<Utc>::from_utc(d2, Utc);

    let time_range = TimeRange {
        gtedate: utcd1,
        ltdate: utcd2,
    };

    Ok(time_range)
}

/// Similar to get time range, but you can actually get a bunch of them.  Honestly, I can't remember why the hell I did this.
pub fn  get_time_ranges<'a>(
    d1: &'a str,
    d2: &'a str,
    df: &'a str,
    dint: &'a i64,
) -> Result<Vec<TimeRange>, Box<dyn Error>> {
    let d1 = NaiveDateTime::parse_from_str(d1, df)?;
    let d2 = NaiveDateTime::parse_from_str(d2, df)?;
    // let utcd1 = DateTime::<Utc>::from_utc(d1,Utc);
    // let utcd2 = DateTime::<Utc>::from_utc(d2,Utc);

    let mut dt = d1;

    let mut time_ranges = Vec::new();

    while dt < d2 {
        let gtd = dt;
        let ltd = gtd + Duration::days(*dint);
        dt = dt + Duration::days(1);
        let tr = TimeRange {
            gtedate: DateTime::<Utc>::from_utc(gtd, Utc),
            ltdate: DateTime::<Utc>::from_utc(ltd, Utc),
        };
        time_ranges.push(tr);
    }

    Ok(time_ranges)
}


/// Very useful - set a begin and end, and have generic collections for calls in the methods.  Note that you get away with the complete generic on collection, because not finding (not needing any data parm knowledge).
/// So all methods need to be very grandiose, like delete all.
#[derive(Debug, Clone)]
pub struct TimeRange {
    pub gtedate: DateTime<Utc>,
    pub ltdate: DateTime<Utc>,
}

impl TimeRange {

    // Use this when tracking ranges in an iteration - the self has to be defined as mut to work
    pub fn adjust(self: &mut TimeRange, d: &DateTime<Utc>) -> bool {        
        let mut adj = false;
        if self.gtedate > *d {
            self.gtedate = *d;
            adj = true
        }
        if self.ltdate < *d {
            self.ltdate = *d;
            adj = true
        }
        adj
    }

    // Use this for cleanup.  Assuming no data 1000000 (mm) before today.
    pub fn annihilation() -> Self {
        Self { 
            gtedate: Utc::now() - Duration::days(1000000),
            ltdate: Utc::now(),
        }
    }


    /// So you give it a span, say a day, in the struct, and it returns a vec of same struct, hourly intervals.  Great to feed into a multithread where you want to process hourlies in tandem for a day.
    pub fn get_hourlies(self: &Self) -> Result<Vec<TimeRange>,Box<dyn Error>> {
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

    /// Pass any collection, delete the range.  Delete many works with generic collection, weird.  I don't think a find op would.  This flavor requires a gte and lt date.
    pub async fn delete_exact_range<T>(self: &Self, collection: &Collection<T>) -> Result<(),MongoError> {
        collection.delete_many(doc!{"gtedate": &self.gtedate, "ltdate": &self.ltdate}, None).await?;    
        debug!("deleted {} {}", &self.gtedate, &self.ltdate);
        Ok(())
    }

    /// This requires a TLDYDXMarket collection to be passed in.  
    pub async fn delete_exact_range_tldydxmarket(self: &Self, dydxcol: &Collection<TLDYDXMarket>) -> Result<(), MongoError> {
        dydxcol.delete_many(doc!{"mongo_snapshot_date": {"$gte": self.gtedate}, "mongo_snapshot_date": {"$lt": self.ltdate}}, None).await?;    
        Ok(())
    }

    ///This is used to fetch a range count per asset pair.  It's primary use is to identify when the quote process stalled, and how long it was out for.
    ///Note that it is specifc to TLDYDXMarket struct.
    pub async fn get_range_count<'a>(self: &Self, dydxcol: &Collection<TLDYDXMarket>) -> Result<HashMap<String,i32>, MongoError> {
        let filter = doc! {"mongo_snapshot_date": {"$gte": self.gtedate}, "mongo_snapshot_date": {"$lt": self.ltdate}};
        let find_options = FindOptions::builder().sort(doc! { "mongo_snapshot_date":1}).build();
        let mut cursor = dydxcol.find(filter, find_options).await?;
        let mut hm: HashMap<String, i32> = HashMap::new(); 
        while let Some(des_tldm) = cursor.try_next().await? {
            hm.entry(des_tldm.market.clone()).and_modify(|e| { *e += 1}).or_insert(1);
        }
        Ok(hm)
    }

}

/// The last hour is the default.
impl Default for TimeRange {
    fn default() -> Self {
        Self { 
            gtedate: Utc::now() - Duration::minutes(60),
            ltdate: Utc::now() 
        }
    }
}

impl fmt::Display for TimeRange {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:>10.4} {:>10.4}", self.gtedate, self.ltdate)
    }
}


