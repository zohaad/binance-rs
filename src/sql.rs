  // TODO: Automate this
    // failsafe
    // sqlx::query!("drop table if exists trade")
    //     .execute(&pool)
    //     .await
    //     .expect("Couldn't drop trade table");
    // sqlx::query!("drop table if exists kline")
    //     .execute(&pool)
    //     .await
    //     .expect("Couldn't drop kline table");

    // sqlx::query!(
    // "create table trade(
    //     id bigserial primary key,
    //     event_time timestamptz,
    //     symbol text,
    //     price numeric(16,8),
    //     quantity numeric(16,8),
    //     trade_time timestamptz
    // );"
    // )
    // .execute(&pool)
    // .await
    // .expect("Failed to create trade table");

    // // I did: create type kline_interval as enum ('1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M')
    // sqlx::query!(
    //     "create table kline(
    //         id bigserial primary key,
    //         event_time timestamptz,
    //         symbol text,
    //         start_time timestamptz,
    //         close_time timestamptz,
    //         interval kline_interval,
    //         open_price numeric(16,8),
    //         close_price numeric(16,8),
    //         high_price numeric(16,8),
    //         low_price numeric(16,8),
    //         num_trades bigint
    //     );"
    // )
    // .execute(&pool)
    // .await
    // .expect("Failed to create kline table");

    // STREAM STUFF
    //
    //