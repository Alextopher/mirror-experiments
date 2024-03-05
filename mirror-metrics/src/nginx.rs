use std::{net::IpAddr, path::Path};

// Data that I can to aggregate
#[derive(Debug, Default, Clone, Copy)]
pub struct Metric {
    pub requests: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
}

impl Metric {
    pub fn new(requests: u64, bytes_sent: u64, bytes_received: u64) -> Self {
        Metric {
            requests,
            bytes_sent,
            bytes_received,
        }
    }
}

impl std::ops::AddAssign for Metric {
    fn add_assign(&mut self, other: Self) {
        self.requests += other.requests;
        self.bytes_sent += other.bytes_sent;
        self.bytes_received += other.bytes_received;
    }
}

impl std::ops::Add for Metric {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Metric {
            requests: self.requests + other.requests,
            bytes_sent: self.bytes_sent + other.bytes_sent,
            bytes_received: self.bytes_received + other.bytes_received,
        }
    }
}

// "07/Feb/2024:00:01:06 -0500" "184.146.232.53" "GET /msys2/mingw/ucrt64/mingw-w64-ucrt-x86_64-mpfr-4.2.1-2-any.pkg.tar.zst.sig HTTP/1.1" "200" "566" "203" "pacman/6.0.2 (MINGW64_NT-10.0-22631 x86_64) libalpm/13.0.2"
#[derive(Debug)]
pub struct LogEntry<'a> {
    pub timestamp: chrono::DateTime<chrono::FixedOffset>,
    pub ip: IpAddr,
    pub method: &'a str,
    pub path: &'a Path,
    pub version: &'a str,
    pub status: u16,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub user_agent: &'a str,
}

/// A faster way to parse the log entry
pub fn parse_line(line: &str) -> anyhow::Result<LogEntry> {
    // We're always expecting 7 pairs of quotes, so we can just split the string
    let quote_list = line.split('"').collect::<Vec<_>>();

    if quote_list.len() != 15 {
        return Err(anyhow::anyhow!("invalid number of parameters in log entry"));
    }

    // Time
    let t = "%d/%b/%Y:%H:%M:%S %z";
    let tm = chrono::DateTime::parse_from_str(quote_list[1], t)?;

    // IPv4 or IPv6 address
    let ip = quote_list[3].parse()?;

    // Method url http version
    let split = quote_list[5].split(' ').collect::<Vec<_>>();
    if split.len() != 3 {
        return Err(anyhow::anyhow!("invalid number of strings in request"));
    }

    let method = split[0];
    let path = split[1].as_ref();
    let version = split[2];

    // HTTP response status
    let status = quote_list[7].parse()?;
    let bytes_sent = quote_list[9].parse()?;
    let bytes_received = quote_list[11].parse()?;
    let user_agent = quote_list[13];

    Ok(LogEntry {
        timestamp: tm,
        ip,
        method,
        path,
        version,
        status,
        bytes_sent,
        bytes_received,
        user_agent,
    })
}
