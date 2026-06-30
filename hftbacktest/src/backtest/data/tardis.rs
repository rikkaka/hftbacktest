use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead, BufReader, Error as IoError, ErrorKind},
    path::{Path, PathBuf},
};

use flate2::read::GzDecoder;
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::{
    backtest::data::{Data, DataStream},
    depth::FusedHashMapMarketDepth,
    types::{
        BUY_EVENT,
        DEPTH_BBO_EVENT,
        DEPTH_CLEAR_EVENT,
        DEPTH_EVENT,
        DEPTH_SNAPSHOT_EVENT,
        EXCH_EVENT,
        Event,
        LOCAL_EVENT,
        SELL_EVENT,
        Side,
        TRADE_EVENT,
    },
};

const DEFAULT_CHUNK_SIZE: usize = 1_000_000;

const TRADE_HEADER: &[&str] = &[
    "exchange",
    "symbol",
    "timestamp",
    "local_timestamp",
    "id",
    "side",
    "price",
    "amount",
];
const DEPTH_HEADER: &[&str] = &[
    "exchange",
    "symbol",
    "timestamp",
    "local_timestamp",
    "is_snapshot",
    "side",
    "price",
    "amount",
];
const BOOK_TICKER_HEADER: &[&str] = &[
    "exchange",
    "symbol",
    "timestamp",
    "local_timestamp",
    "ask_amount",
    "ask_price",
    "bid_price",
    "bid_amount",
];

/// Snapshot handling mode for streamed Tardis CSV data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TardisSnapshotMode {
    Process,
    IgnoreSod,
    Ignore,
}

/// Configuration for streaming Tardis fused level-2 CSV data.
#[derive(Clone, Debug)]
pub struct TardisCsvConfig {
    trades: PathBuf,
    depth: PathBuf,
    book_ticker: PathBuf,
    tick_size: f64,
    lot_size: f64,
    chunk_size: usize,
    snapshot_mode: TardisSnapshotMode,
}

impl TardisCsvConfig {
    pub fn new(
        trades: impl Into<PathBuf>,
        depth: impl Into<PathBuf>,
        book_ticker: impl Into<PathBuf>,
        tick_size: f64,
        lot_size: f64,
    ) -> Self {
        Self {
            trades: trades.into(),
            depth: depth.into(),
            book_ticker: book_ticker.into(),
            tick_size,
            lot_size,
            chunk_size: DEFAULT_CHUNK_SIZE,
            snapshot_mode: TardisSnapshotMode::Process,
        }
    }

    pub fn chunk_size(self, chunk_size: usize) -> Self {
        Self { chunk_size, ..self }
    }

    pub fn snapshot_mode(self, snapshot_mode: TardisSnapshotMode) -> Self {
        Self {
            snapshot_mode,
            ..self
        }
    }
}

pub struct TardisCsvStream {
    config: TardisCsvConfig,
    inner: Option<TardisCsvStreamInner>,
    finished: bool,
}

impl TardisCsvStream {
    pub fn new(config: TardisCsvConfig) -> Self {
        Self {
            config,
            inner: None,
            finished: false,
        }
    }

    fn inner(&mut self) -> Result<&mut TardisCsvStreamInner, IoError> {
        if self.inner.is_none() {
            if self.config.chunk_size == 0 {
                return Err(invalid_data("chunk_size must be greater than zero"));
            }
            self.inner = Some(TardisCsvStreamInner::new(self.config.clone())?);
        }
        Ok(self.inner.as_mut().unwrap())
    }
}

impl std::fmt::Debug for TardisCsvStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TardisCsvStream")
            .field("config", &self.config)
            .field("finished", &self.finished)
            .finish()
    }
}

impl DataStream<Event> for TardisCsvStream {
    fn next_data(&mut self) -> Result<Option<Data<Event>>, IoError> {
        if self.finished {
            return Ok(None);
        }

        let inner = self.inner()?;
        let data = inner.next_data()?;
        if data.is_none() {
            self.finished = true;
        }
        Ok(data)
    }
}

struct TardisCsvStreamInner {
    config: TardisCsvConfig,
    trades: CsvFileReader,
    depth: CsvFileReader,
    book_ticker: CsvFileReader,
    next_trade: Option<Event>,
    next_depth: Option<DepthRow>,
    next_ticker: Option<BookTickerRow>,
    fuse_depth: FusedHashMapMarketDepth,
    pending_fused: VecDeque<Event>,
    ss_bid: Vec<Event>,
    ss_ask: Vec<Event>,
    is_sod_snapshot: bool,
    last_exch_ts: Option<i64>,
    last_local_ts: Option<i64>,
}

impl TardisCsvStreamInner {
    fn new(config: TardisCsvConfig) -> Result<Self, IoError> {
        let mut trades = CsvFileReader::new(&config.trades, TRADE_HEADER)?;
        let mut depth = CsvFileReader::new(&config.depth, DEPTH_HEADER)?;
        let mut book_ticker = CsvFileReader::new(&config.book_ticker, BOOK_TICKER_HEADER)?;

        let next_trade = trades.next_row(parse_trade)?;
        let next_depth = depth.next_row(parse_depth)?;
        let next_ticker = book_ticker.next_row(parse_book_ticker)?;
        let fuse_depth = FusedHashMapMarketDepth::new(config.tick_size, config.lot_size);

        Ok(Self {
            config,
            trades,
            depth,
            book_ticker,
            next_trade,
            next_depth,
            next_ticker,
            fuse_depth,
            pending_fused: Default::default(),
            ss_bid: Default::default(),
            ss_ask: Default::default(),
            is_sod_snapshot: true,
            last_exch_ts: None,
            last_local_ts: None,
        })
    }

    fn next_data(&mut self) -> Result<Option<Data<Event>>, IoError> {
        let mut events = Vec::with_capacity(self.config.chunk_size);
        while events.len() < self.config.chunk_size {
            let Some(mut event) = self.next_event()? else {
                break;
            };
            event.ev |= LOCAL_EVENT | EXCH_EVENT;
            self.validate_order(&event)?;
            events.push(event);
        }

        if events.is_empty() {
            Ok(None)
        } else {
            Ok(Some(Data::from_data(&events)))
        }
    }

    fn next_event(&mut self) -> Result<Option<Event>, IoError> {
        loop {
            if self.pending_fused.is_empty() {
                let _ = self.produce_next_fused_event()?;
            }

            match (self.next_trade.as_ref(), self.pending_fused.front()) {
                (Some(trade), Some(fused)) => {
                    if trade.local_ts <= fused.local_ts {
                        return self.take_trade_event();
                    } else {
                        return Ok(self.pending_fused.pop_front());
                    }
                }
                (Some(_), None) => return self.take_trade_event(),
                (None, Some(_)) => return Ok(self.pending_fused.pop_front()),
                (None, None) => return Ok(None),
            }
        }
    }

    fn take_trade_event(&mut self) -> Result<Option<Event>, IoError> {
        let event = self.next_trade.take();
        self.next_trade = self.trades.next_row(parse_trade)?;
        Ok(event)
    }

    fn produce_next_fused_event(&mut self) -> Result<bool, IoError> {
        while self.pending_fused.is_empty() {
            let ticker_ts = self
                .next_ticker
                .as_ref()
                .map(|row| row.local_ts)
                .unwrap_or(0);
            let depth_ts = self
                .next_depth
                .as_ref()
                .map(|row| row.local_ts)
                .unwrap_or(0);

            if ticker_ts > 0 && depth_ts > 0 {
                if ticker_ts < depth_ts {
                    self.process_ticker_head()?;
                } else {
                    self.process_depth_head()?;
                }
            } else if ticker_ts > 0 {
                self.process_ticker_head()?;
            } else if depth_ts > 0 {
                self.process_depth_head()?;
            } else {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn process_ticker_head(&mut self) -> Result<(), IoError> {
        let row = self.next_ticker.take().unwrap();

        self.process_fuse_event(
            event(
                DEPTH_BBO_EVENT | SELL_EVENT,
                row.exch_ts,
                row.local_ts,
                row.ask_price,
                row.ask_amount,
            ),
            true,
        )?;
        self.process_fuse_event(
            event(
                DEPTH_BBO_EVENT | BUY_EVENT,
                row.exch_ts,
                row.local_ts,
                row.bid_price,
                row.bid_amount,
            ),
            true,
        )?;

        self.next_ticker = self.book_ticker.next_row(parse_book_ticker)?;
        Ok(())
    }

    fn process_depth_head(&mut self) -> Result<(), IoError> {
        let row = self.next_depth.take().unwrap();
        if row.is_snapshot {
            self.ss_bid.clear();
            self.ss_ask.clear();
            self.push_snapshot_row(row)?;

            loop {
                match self.depth.next_row(parse_depth)? {
                    Some(row) if row.is_snapshot => self.push_snapshot_row(row)?,
                    Some(row) => {
                        self.next_depth = Some(row);
                        self.flush_snapshot()?;
                        break;
                    }
                    None => {
                        self.flush_snapshot()?;
                        break;
                    }
                }
            }
        } else {
            self.process_depth_update(row)?;
            self.next_depth = self.depth.next_row(parse_depth)?;
        }
        Ok(())
    }

    fn push_snapshot_row(&mut self, row: DepthRow) -> Result<(), IoError> {
        match row.side {
            BookSide::Bid => self.ss_bid.push(event(
                DEPTH_SNAPSHOT_EVENT | BUY_EVENT,
                row.exch_ts,
                row.local_ts,
                row.px,
                row.qty,
            )),
            BookSide::Ask => self.ss_ask.push(event(
                DEPTH_SNAPSHOT_EVENT | SELL_EVENT,
                row.exch_ts,
                row.local_ts,
                row.px,
                row.qty,
            )),
        }
        Ok(())
    }

    fn flush_snapshot(&mut self) -> Result<(), IoError> {
        let add = !matches!(self.config.snapshot_mode, TardisSnapshotMode::Ignore)
            && !(matches!(self.config.snapshot_mode, TardisSnapshotMode::IgnoreSod)
                && self.is_sod_snapshot);
        self.is_sod_snapshot = false;

        let ss_bid = std::mem::take(&mut self.ss_bid);
        if !ss_bid.is_empty() {
            self.process_fuse_event(
                event(
                    DEPTH_CLEAR_EVENT | BUY_EVENT,
                    ss_bid[0].exch_ts,
                    ss_bid[0].local_ts,
                    ss_bid[ss_bid.len() - 1].px,
                    0.0,
                ),
                add,
            )?;
            for event in ss_bid {
                self.process_fuse_event(event, add)?;
            }
        }

        let ss_ask = std::mem::take(&mut self.ss_ask);
        if !ss_ask.is_empty() {
            self.process_fuse_event(
                event(
                    DEPTH_CLEAR_EVENT | SELL_EVENT,
                    ss_ask[0].exch_ts,
                    ss_ask[0].local_ts,
                    ss_ask[ss_ask.len() - 1].px,
                    0.0,
                ),
                add,
            )?;
            for event in ss_ask {
                self.process_fuse_event(event, add)?;
            }
        }
        Ok(())
    }

    fn process_depth_update(&mut self, row: DepthRow) -> Result<(), IoError> {
        let side = match row.side {
            BookSide::Bid => BUY_EVENT,
            BookSide::Ask => SELL_EVENT,
        };
        self.process_fuse_event(
            event(
                DEPTH_EVENT | side,
                row.exch_ts,
                row.local_ts,
                row.px,
                row.qty,
            ),
            true,
        )
    }

    fn process_fuse_event(&mut self, mut event: Event, add: bool) -> Result<(), IoError> {
        if event.is(DEPTH_EVENT) || event.is(DEPTH_SNAPSHOT_EVENT) {
            let mut events = if event.is(BUY_EVENT) {
                self.fuse_depth.update_bid_depth(event)
            } else if event.is(SELL_EVENT) {
                self.fuse_depth.update_ask_depth(event)
            } else {
                return Err(invalid_data("depth event has no side"));
            };
            if add {
                self.pending_fused.extend(events.drain(..));
            }
        } else if event.is(DEPTH_CLEAR_EVENT) {
            if event.is(BUY_EVENT) {
                self.fuse_depth
                    .clear_depth(Side::Buy, event.px, event.exch_ts);
            } else if event.is(SELL_EVENT) {
                self.fuse_depth
                    .clear_depth(Side::Sell, event.px, event.exch_ts);
            } else {
                self.fuse_depth.clear_depth(Side::None, 0.0, event.exch_ts);
            }
            if add {
                self.pending_fused.push_back(event);
            }
        } else if event.is(DEPTH_BBO_EVENT) {
            event.ev = (event.ev & !DEPTH_BBO_EVENT) | DEPTH_EVENT;
            let mut events = if event.is(BUY_EVENT) {
                self.fuse_depth.update_best_bid(event)
            } else if event.is(SELL_EVENT) {
                self.fuse_depth.update_best_ask(event)
            } else {
                return Err(invalid_data("book ticker event has no side"));
            };
            if add {
                self.pending_fused.extend(events.drain(..));
            }
        } else {
            return Err(invalid_data("unsupported fused Tardis event"));
        }
        Ok(())
    }

    fn validate_order(&mut self, event: &Event) -> Result<(), IoError> {
        if let Some(last_exch_ts) = self.last_exch_ts
            && event.exch_ts < last_exch_ts
        {
            return Err(invalid_data(format!(
                "Tardis stream exchange timestamp is out of order: {} < {}",
                event.exch_ts, last_exch_ts
            )));
        }
        if let Some(last_local_ts) = self.last_local_ts
            && event.local_ts < last_local_ts
        {
            return Err(invalid_data(format!(
                "Tardis stream local timestamp is out of order: {} < {}",
                event.local_ts, last_local_ts
            )));
        }

        self.last_exch_ts = Some(event.exch_ts);
        self.last_local_ts = Some(event.local_ts);
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum BookSide {
    Bid,
    Ask,
}

struct DepthRow {
    exch_ts: i64,
    local_ts: i64,
    px: f64,
    qty: f64,
    side: BookSide,
    is_snapshot: bool,
}

struct BookTickerRow {
    exch_ts: i64,
    local_ts: i64,
    ask_amount: f64,
    ask_price: f64,
    bid_price: f64,
    bid_amount: f64,
}

struct CsvFileReader {
    path: PathBuf,
    reader: Box<dyn BufRead>,
    line: String,
    row: usize,
}

impl CsvFileReader {
    fn new(path: &Path, expected_header: &[&str]) -> Result<Self, IoError> {
        let mut reader = open_reader(path)?;
        let mut line = String::new();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            return Err(invalid_data(format!("{} is empty", path.display())));
        }
        let header = trim_line(&line);
        let fields = split_csv_line(header);
        if fields.as_slice() != expected_header {
            return Err(invalid_data(format!(
                "{} has unexpected header: {}",
                path.display(),
                header
            )));
        }

        Ok(Self {
            path: path.to_path_buf(),
            reader,
            line: String::new(),
            row: 1,
        })
    }

    fn next_row<T>(
        &mut self,
        parse: fn(&[&str]) -> Result<T, IoError>,
    ) -> Result<Option<T>, IoError> {
        self.line.clear();
        let read = self.reader.read_line(&mut self.line)?;
        if read == 0 {
            return Ok(None);
        }
        self.row += 1;

        let line = trim_line(&self.line);
        let fields = split_csv_line(line);
        parse(&fields).map(Some).map_err(|err| {
            IoError::new(
                err.kind(),
                format!("{}:{}: {}", self.path.display(), self.row, err),
            )
        })
    }
}

fn open_reader(path: &Path) -> Result<Box<dyn BufRead>, IoError> {
    let file = File::open(path)?;
    let path_str = path.to_string_lossy();
    if path_str.ends_with(".gz") {
        Ok(Box::new(BufReader::new(GzDecoder::new(file))))
    } else if path_str.ends_with(".zst") {
        Ok(Box::new(BufReader::new(ZstdDecoder::new(file)?)))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

fn trim_line(line: &str) -> &str {
    line.trim_end_matches('\n').trim_end_matches('\r')
}

fn split_csv_line(line: &str) -> Vec<&str> {
    line.split(',').collect()
}

fn parse_trade(fields: &[&str]) -> Result<Event, IoError> {
    check_len(fields, TRADE_HEADER)?;

    let ev = match fields[5] {
        "buy" => BUY_EVENT | TRADE_EVENT,
        "sell" => SELL_EVENT | TRADE_EVENT,
        side => {
            return Err(invalid_data(format!("invalid trade side: {side}")));
        }
    };
    Ok(event(
        ev,
        parse_timestamp(fields[2], "timestamp")?,
        parse_timestamp(fields[3], "local_timestamp")?,
        parse_f64(fields[6], "price")?,
        parse_f64(fields[7], "amount")?,
    ))
}

fn parse_depth(fields: &[&str]) -> Result<DepthRow, IoError> {
    check_len(fields, DEPTH_HEADER)?;

    Ok(DepthRow {
        exch_ts: parse_timestamp(fields[2], "timestamp")?,
        local_ts: parse_timestamp(fields[3], "local_timestamp")?,
        is_snapshot: parse_bool(fields[4], "is_snapshot")?,
        side: parse_book_side(fields[5])?,
        px: parse_f64(fields[6], "price")?,
        qty: parse_f64(fields[7], "amount")?,
    })
}

fn parse_book_ticker(fields: &[&str]) -> Result<BookTickerRow, IoError> {
    check_len(fields, BOOK_TICKER_HEADER)?;

    Ok(BookTickerRow {
        exch_ts: parse_timestamp(fields[2], "timestamp")?,
        local_ts: parse_timestamp(fields[3], "local_timestamp")?,
        ask_amount: parse_f64(fields[4], "ask_amount")?,
        ask_price: parse_f64(fields[5], "ask_price")?,
        bid_price: parse_f64(fields[6], "bid_price")?,
        bid_amount: parse_f64(fields[7], "bid_amount")?,
    })
}

fn check_len(fields: &[&str], expected_header: &[&str]) -> Result<(), IoError> {
    if fields.len() != expected_header.len() {
        return Err(invalid_data(format!(
            "expected {} fields, found {}",
            expected_header.len(),
            fields.len()
        )));
    }
    Ok(())
}

fn parse_book_side(value: &str) -> Result<BookSide, IoError> {
    match value {
        "bid" | "buy" => Ok(BookSide::Bid),
        "ask" | "sell" => Ok(BookSide::Ask),
        side => Err(invalid_data(format!("invalid book side: {side}"))),
    }
}

fn parse_bool(value: &str, name: &str) -> Result<bool, IoError> {
    match value {
        "true" | "True" | "1" => Ok(true),
        "false" | "False" | "0" => Ok(false),
        value => Err(invalid_data(format!("invalid {name}: {value}"))),
    }
}

fn parse_timestamp(value: &str, name: &str) -> Result<i64, IoError> {
    let timestamp = value
        .parse::<i64>()
        .map_err(|err| invalid_data(format!("invalid {name}: {err}")))?;
    timestamp
        .checked_mul(1000)
        .ok_or_else(|| invalid_data(format!("{name} overflows when converted to nanoseconds")))
}

fn parse_f64(value: &str, name: &str) -> Result<f64, IoError> {
    value
        .parse::<f64>()
        .map_err(|err| invalid_data(format!("invalid {name}: {err}")))
}

fn event(ev: u64, exch_ts: i64, local_ts: i64, px: f64, qty: f64) -> Event {
    Event {
        ev,
        exch_ts,
        local_ts,
        px,
        qty,
        order_id: 0,
        ival: 0,
        fval: 0.0,
    }
}

fn invalid_data(message: impl Into<String>) -> IoError {
    IoError::new(ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{create_dir, remove_dir_all, write},
        io::Write,
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use flate2::{Compression, write::GzEncoder};

    use super::*;
    use crate::{
        backtest::{
            BacktestError,
            data::{DataSource, Reader},
        },
        types::{DEPTH_EVENT, EXCH_EVENT, LOCAL_EVENT},
    };

    const TRADES: &str = "\
exchange,symbol,timestamp,local_timestamp,id,side,price,amount
binance-futures,BTCUSDT,1000,1010,1,buy,100.0,0.1
binance-futures,BTCUSDT,3000,3010,2,sell,101.0,0.2
";

    const DEPTH: &str = "\
exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
binance-futures,BTCUSDT,2000,2010,false,bid,99.0,1.0
";

    const BOOK_TICKER: &str = "\
exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount
binance-futures,BTCUSDT,1500,1510,1.2,101.0,100.0,1.0
";

    const EMPTY_TRADES: &str = "\
exchange,symbol,timestamp,local_timestamp,id,side,price,amount
";

    const EMPTY_DEPTH: &str = "\
exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
";

    const EMPTY_BOOK_TICKER: &str = "\
exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount
";

    static NEXT_TEST_DIR_ID: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn streams_plain_csv_in_chunks() {
        let dir = TestDir::new();
        let trades = dir.write("trades.csv", TRADES);
        let depth = dir.write("depth.csv", DEPTH);
        let book_ticker = dir.write("book_ticker.csv", BOOK_TICKER);

        let events = collect_events(
            TardisCsvConfig::new(trades, depth, book_ticker, 1.0, 1.0).chunk_size(2),
        )
        .unwrap();

        assert_eq!(events.len(), 5);
        assert!(events.iter().all(|event| event.is(LOCAL_EVENT)));
        assert!(events.iter().all(|event| event.is(EXCH_EVENT)));
        assert!(
            events
                .windows(2)
                .all(|events| events[0].local_ts <= events[1].local_ts)
        );
        assert!(
            events
                .windows(2)
                .all(|events| events[0].exch_ts <= events[1].exch_ts)
        );
        assert!(events[0].is(TRADE_EVENT));
        assert!(events[1].is(DEPTH_EVENT));
        assert!(events[1].is(SELL_EVENT));
        assert!(events[2].is(DEPTH_EVENT));
        assert!(events[2].is(BUY_EVENT));
        assert!(events[3].is(DEPTH_EVENT));
        assert!(events[3].is(BUY_EVENT));
        assert!(events[4].is(TRADE_EVENT));
    }

    #[test]
    fn streams_gzip_and_zstd_csv() {
        let dir = TestDir::new();
        let trades = dir.write_gz("trades.csv.gz", TRADES);
        let depth = dir.write_zst("depth.csv.zst", DEPTH);
        let book_ticker = dir.write("book_ticker.csv", BOOK_TICKER);

        let events = collect_events(
            TardisCsvConfig::new(trades, depth, book_ticker, 1.0, 1.0).chunk_size(3),
        )
        .unwrap();

        assert_eq!(events.len(), 5);
        assert!(events.iter().all(|event| event.is(LOCAL_EVENT)));
        assert!(events.iter().all(|event| event.is(EXCH_EVENT)));
    }

    #[test]
    fn flushes_snapshot_at_depth_eof() {
        let dir = TestDir::new();
        let trades = dir.write("trades.csv", EMPTY_TRADES);
        let depth = dir.write(
            "depth.csv",
            "\
exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount
binance-futures,BTCUSDT,1000,1010,true,bid,100.0,1.0
binance-futures,BTCUSDT,1000,1010,true,ask,101.0,2.0
",
        );
        let book_ticker = dir.write("book_ticker.csv", EMPTY_BOOK_TICKER);

        let events = collect_events(
            TardisCsvConfig::new(trades, depth, book_ticker, 1.0, 1.0).chunk_size(10),
        )
        .unwrap();

        assert_eq!(events.len(), 4);
        assert!(events.iter().all(|event| event.is(LOCAL_EVENT)));
        assert!(events.iter().all(|event| event.is(EXCH_EVENT)));
        assert!(
            events
                .iter()
                .any(|event| event.is(DEPTH_CLEAR_EVENT) && event.is(BUY_EVENT))
        );
        assert!(
            events
                .iter()
                .any(|event| event.is(DEPTH_CLEAR_EVENT) && event.is(SELL_EVENT))
        );
        assert!(
            events
                .iter()
                .any(|event| event.is(DEPTH_SNAPSHOT_EVENT) && event.is(BUY_EVENT))
        );
        assert!(
            events
                .iter()
                .any(|event| event.is(DEPTH_SNAPSHOT_EVENT) && event.is(SELL_EVENT))
        );
    }

    #[test]
    fn rejects_out_of_order_timestamps() {
        let dir = TestDir::new();
        let trades = dir.write(
            "trades.csv",
            "\
exchange,symbol,timestamp,local_timestamp,id,side,price,amount
binance-futures,BTCUSDT,2000,2000,1,buy,100.0,0.1
binance-futures,BTCUSDT,1000,1000,2,sell,101.0,0.2
",
        );
        let depth = dir.write("depth.csv", EMPTY_DEPTH);
        let book_ticker = dir.write("book_ticker.csv", EMPTY_BOOK_TICKER);

        let err = collect_events(
            TardisCsvConfig::new(trades, depth, book_ticker, 1.0, 1.0).chunk_size(10),
        )
        .unwrap_err();

        assert!(matches!(err, BacktestError::DataError(_)));
    }

    fn collect_events(config: TardisCsvConfig) -> Result<Vec<Event>, BacktestError> {
        let mut reader = Reader::builder()
            .data(vec![DataSource::tardis_csv(config)])
            .build()
            .unwrap();
        let mut events = Vec::new();

        loop {
            match reader.next_data() {
                Ok(data) => {
                    for rn in 0..data.len() {
                        events.push(data[rn].clone());
                    }
                    reader.release(data);
                }
                Err(BacktestError::EndOfData) => return Ok(events),
                Err(err) => return Err(err),
            }
        }
    }

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let id = NEXT_TEST_DIR_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "hftbacktest_tardis_{}_{}_{}",
                std::process::id(),
                nanos,
                id
            ));
            create_dir(&path).unwrap();
            Self { path }
        }

        fn write(&self, filename: &str, contents: &str) -> PathBuf {
            let path = self.path.join(filename);
            write(&path, contents).unwrap();
            path
        }

        fn write_gz(&self, filename: &str, contents: &str) -> PathBuf {
            let path = self.path.join(filename);
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(contents.as_bytes()).unwrap();
            write(&path, encoder.finish().unwrap()).unwrap();
            path
        }

        fn write_zst(&self, filename: &str, contents: &str) -> PathBuf {
            let path = self.path.join(filename);
            let encoded = zstd::stream::encode_all(contents.as_bytes(), 0).unwrap();
            write(&path, encoded).unwrap();
            path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = remove_dir_all(Path::new(&self.path));
        }
    }
}
