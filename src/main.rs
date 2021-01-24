use chrono::{serde::ts_milliseconds, DateTime, Utc};
use dotenv::dotenv;
use futures_util::{pin_mut, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use sqlx::{pool::PoolConnection, postgres::PgPool, Postgres};
use std::env;
use tokio::io::AsyncWriteExt;
use tokio_tungstenite::connect_async;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv()?;
    let ref database_url = env::var("DATABASE_URL")?;

    let pool = PgPool::connect(database_url).await?;

    let stream = Stream::Trade {
        pair: "btcusdt".to_string(),
    };

    stream.add_to_db(&pool).await?;

    // let stream = Stream::Kline {
    //     pair: "btcusdt".to_string(),
    //     interval: Interval::_1m,
    // };
    // stream.add_to_db(&pool).await?;

    Ok(())
}

impl Stream {
    async fn add_to_db(&self, pool: &PgPool) -> anyhow::Result<()> {
        let url = self.to_url()?;

        let (ws_stream, _) = connect_async(url).await?;
        let (_, read) = ws_stream.split();

        let ws_to_stdout = {
            read.for_each(|message| async {
                let pool_connection = pool.acquire().await.unwrap();
                // me trying to use async io correctly lmao
                tokio::spawn(async move {
                    let data = message.unwrap().into_data();

                    if let Ok(event) = serde_json::from_slice::<SerializableEvent>(&data) {
                        let event: Event = event.into();
                        event.add_to_db(pool_connection).await.unwrap();
                    } else {
                        print!("failed to deserialize: ");
                        tokio::io::stdout().write_all(&data).await.unwrap();
                    }
                });
            })
        };

        pin_mut!(ws_to_stdout); // no idea what this does
        ws_to_stdout.await;
        Ok(())
    }
}
// "ActiveRecord style"
// a better style: event log queue with Redis or 0MQ
// <feed> -> <redis> -> <activerecord> -> <db>
// put redis between your binary and feed, if parsing fails you don't lose data
// because of replay
impl Event {
    async fn add_to_db(
        self,
        mut pool: PoolConnection<Postgres>,
    ) -> Result<&'static str, sqlx::Error> {
        match self {
            Self::Kline {
                event_time,
                symbol,
                start_time,
                close_time,
                interval,
                open_price,
                close_price,
                high_price,
                low_price,
                num_trades,
                is_kline_closed,
                ..
            } => {
                match is_kline_closed {
                    false => Ok("kline not closed"), // what to do here?
                    true => {
                        sqlx::query!(
                            "
                            insert into kline(event_time, symbol, start_time, close_time, interval, open_price, close_price, high_price, low_price, num_trades)
                            values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
                            ",
                            event_time,
                            symbol,
                            start_time,
                            close_time,
                            // disables compile time check, needed with custom postgres types
                            interval as _,
                            open_price,
                            close_price,
                            high_price,
                            low_price,
                            num_trades,
                        ).execute(&mut pool).await?;
                        Ok("added kline")
                    }
                }
            }
            Self::Trade {
                event_time,
                symbol,
                price,
                quantity,
                trade_time,
                ..
            } => {
                sqlx::query!(
                    "
                    insert into trade(event_time, symbol, price, quantity, trade_time)
                    values ($1, $2, $3, $4, $5)
                    ",
                    event_time,
                    symbol,
                    price,
                    quantity,
                    trade_time
                )
                .execute(&mut pool)
                .await?;
                Ok("added trade")
            }
        }
    }
}

enum Stream {
    Trade { pair: String },
    Kline { pair: String, interval: Interval },
}

impl Stream {
    fn to_url(&self) -> Result<url::Url, url::ParseError> {
        let endpoint = match self {
            // pair.to_lowercase() is for safety purposes
            // TODO: build better safety
            Self::Trade { pair } => format!("{}@trade", pair.to_lowercase()),
            Self::Kline { pair, interval } => {
                format!("{}@kline_{}", pair.to_lowercase(), interval.to_string())
            }
        };

        let formatted_url = format!("wss://stream.binance.com:9443/ws/{}", endpoint);
        url::Url::parse(&formatted_url)
    }
}

#[derive(Deserialize, Debug, sqlx::Type)]
#[sqlx(rename = "kline_interval")]
enum Interval {
    #[serde(rename = "1m")]
    #[sqlx(rename = "1m")]
    _1m,
    #[serde(rename = "3m")]
    #[sqlx(rename = "3m")]
    _3m,
    #[serde(rename = "5m")]
    #[sqlx(rename = "5m")]
    _5m,
    #[serde(rename = "15m")]
    #[sqlx(rename = "15m")]
    _15m,
    #[serde(rename = "30m")]
    #[sqlx(rename = "30m")]
    _30m,
    #[serde(rename = "1h")]
    #[sqlx(rename = "1h")]
    _1h,
    #[serde(rename = "2h")]
    #[sqlx(rename = "2h")]
    _2h,
    #[serde(rename = "4h")]
    #[sqlx(rename = "4h")]
    _4h,
    #[serde(rename = "6h")]
    #[sqlx(rename = "6h")]
    _6h,
    #[serde(rename = "8h")]
    #[sqlx(rename = "8h")]
    _8h,
    #[serde(rename = "12h")]
    #[sqlx(rename = "12h")]
    _12h,
    #[serde(rename = "1d")]
    #[sqlx(rename = "1d")]
    _1d,
    #[serde(rename = "3d")]
    #[sqlx(rename = "3d")]
    _3d,
    #[serde(rename = "1w")]
    #[sqlx(rename = "1w")]
    _1w,
    #[serde(rename = "1M")]
    #[sqlx(rename = "1M")]
    _1M,
}

impl Interval {
    fn to_string(&self) -> String {
        format!("{:?}", self)[1..].to_owned()
    }
}

#[derive(Debug)]
enum Event {
    Kline {
        event_time: DateTime<Utc>, // Event time
        symbol: String,            // Symbol
        start_time: DateTime<Utc>,
        close_time: DateTime<Utc>,
        interval: Interval,
        first_trade_id: Id,
        last_trade_id: Id,
        open_price: Decimal,
        close_price: Decimal,
        high_price: Decimal,
        low_price: Decimal,
        base_asset_volume: Decimal,
        num_trades: i64,
        is_kline_closed: bool,
        quote_asset_volume: Decimal,
        taker_buy_base_asset_volume: Decimal,
        taker_buy_quote_asset_volume: Decimal,
    },
    Trade {
        event_time: DateTime<Utc>, // Event time
        symbol: String,            // Symbol
        trade_id: Id,              // Trade ID
        price: Decimal,            // Price
        quantity: Decimal,         // Quantity
        buyer_order_id: Id,        // Buyer order ID
        seller_order_id: Id,       // Seller order ID
        trade_time: DateTime<Utc>, // Trade time
        is_buyer_market_maker: bool, // Is the buyer the market maker?
                                   // M: bool,   // Ignore
    },
}

#[derive(Deserialize, Debug)]
#[serde(transparent)]
struct Id(pub u64); // Postgres can't handle u64? or sqlx can't?

#[derive(Deserialize, Debug)]
struct KlineData {
    #[serde(rename = "t", with = "ts_milliseconds")]
    start_time: DateTime<Utc>,
    #[serde(rename = "T", with = "ts_milliseconds")] //, with = "ts_milliseconds")]
    close_time: DateTime<Utc>,
    // #[serde(rename = "s")]
    // symbol: String,
    #[serde(rename = "i")]
    interval: Interval,
    #[serde(rename = "f")]
    first_trade_id: Id,
    #[serde(rename = "L")]
    last_trade_id: Id,
    #[serde(rename = "o")]
    open_price: Decimal,
    #[serde(rename = "c")]
    close_price: Decimal,
    #[serde(rename = "h")]
    high_price: Decimal,
    #[serde(rename = "l")]
    low_price: Decimal,
    #[serde(rename = "v")]
    base_asset_volume: Decimal,
    #[serde(rename = "n")]
    num_trades: i64,
    #[serde(rename = "x")]
    is_kline_closed: bool, // I'm only interested in this struct if this value is true
    #[serde(rename = "q")]
    quote_asset_volume: Decimal,
    #[serde(rename = "V")]
    taker_buy_base_asset_volume: Decimal,
    #[serde(rename = "Q")]
    taker_buy_quote_asset_volume: Decimal,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "e")]
enum SerializableEvent {
    #[serde(rename = "kline")]
    Kline {
        #[serde(rename = "E", with = "ts_milliseconds")]
        event_time: DateTime<Utc>, // Event time
        #[serde(rename = "s")]
        symbol: String, // Symbol
        #[serde(rename = "k")]
        kline_data: KlineData,
    },
    #[serde(rename = "trade")]
    Trade {
        #[serde(rename = "E", with = "ts_milliseconds")]
        event_time: DateTime<Utc>, // Event time
        #[serde(rename = "s")]
        symbol: String, // Symbol
        #[serde(rename = "t")]
        trade_id: Id, // Trade ID
        #[serde(rename = "p")]
        price: Decimal, // Price
        #[serde(rename = "q")]
        quantity: Decimal, // Quantity
        #[serde(rename = "b")]
        buyer_order_id: Id, // Buyer order ID
        #[serde(rename = "a")]
        seller_order_id: Id, // Seller order ID
        #[serde(rename = "T", with = "ts_milliseconds")]
        trade_time: DateTime<Utc>, // Trade time
        #[serde(rename = "m")]
        is_buyer_market_maker: bool, // Is the buyer the market maker?
                                     // M: bool,   // Ignore
    },
}

impl From<SerializableEvent> for Event {
    fn from(item: SerializableEvent) -> Self {
        match item {
            SerializableEvent::Kline {
                event_time,
                symbol,
                kline_data:
                    KlineData {
                        start_time,
                        close_time,
                        interval,
                        first_trade_id,
                        last_trade_id,
                        open_price,
                        close_price,
                        high_price,
                        low_price,
                        base_asset_volume,
                        num_trades,
                        quote_asset_volume,
                        taker_buy_base_asset_volume,
                        taker_buy_quote_asset_volume,
                        is_kline_closed,
                        ..
                    },
            } => Self::Kline {
                event_time,
                symbol,
                start_time,
                close_time,
                interval,
                first_trade_id,
                last_trade_id,
                open_price,
                close_price,
                high_price,
                low_price,
                base_asset_volume,
                num_trades,
                quote_asset_volume,
                taker_buy_base_asset_volume,
                taker_buy_quote_asset_volume,
                is_kline_closed,
            },
            SerializableEvent::Trade {
                event_time,
                symbol,
                trade_id,
                price,
                quantity,
                buyer_order_id,
                seller_order_id,
                trade_time,
                is_buyer_market_maker,
            } => Self::Trade {
                event_time,
                symbol,
                trade_id,
                price,
                quantity,
                buyer_order_id,
                seller_order_id,
                trade_time,
                is_buyer_market_maker,
            },
        }
    }
}
