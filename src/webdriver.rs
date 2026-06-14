use async_net::TcpStream;
use smol::io::{AsyncReadExt, AsyncWriteExt};
use smol::unblock;
use std::time::Duration;

use crate::enums::{Country, Game, Platform};
use crate::errors::WizPatchError;

#[derive(Debug, Clone)]
pub struct PatchUrls {
    pub file_list_url: String,
    pub base_url: String,
}

/// Speaks the patch server's binary handshake and parses the two URLs out of
/// the second packet. Mirrors `wizdiff.webdriver.WebDriver.get_patch_urls`.
pub async fn get_patch_urls(
    game: Game,
    platform: Platform,
    country: Country,
) -> Result<PatchUrls, WizPatchError> {
    let addr = format!("{}:{}", game.host(country), platform.port());
    let mut stream = TcpStream::connect(&addr).await?;

    let mut request = [0u8; 40];
    request[..11].copy_from_slice(&[
        0x0D, 0xF0, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x01, 0x20,
    ]);
    stream.write_all(&request).await?;

    let mut discard = [0u8; 4096];
    let _ = stream.read(&mut discard).await?;

    let mut data = vec![0u8; 4096];
    let n = stream.read(&mut data).await?;
    data.truncate(n);

    let file_list_idx = find(&data, b"http")
        .ok_or_else(|| WizPatchError::Protocol("no http URL in response".into()))?;
    let base_idx = rfind(&data, b"http")
        .ok_or_else(|| WizPatchError::Protocol("no base http URL in response".into()))?;

    Ok(PatchUrls {
        file_list_url: read_pascal_url(&data, file_list_idx.saturating_sub(2))?,
        base_url: read_pascal_url(&data, base_idx.saturating_sub(2))?,
    })
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

fn rfind(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .rposition(|w| w == needle)
}

fn read_pascal_url(data: &[u8], start: usize) -> Result<String, WizPatchError> {
    if start + 2 > data.len() {
        return Err(WizPatchError::Protocol("URL length out of bounds".into()));
    }
    let len = u16::from_le_bytes([data[start], data[start + 1]]) as usize;
    let str_start = start + 2;
    let str_end = str_start + len;
    if str_end > data.len() {
        return Err(WizPatchError::Protocol("URL string out of bounds".into()));
    }
    String::from_utf8(data[str_start..str_end].to_vec()).map_err(Into::into)
}

/// Builds a connection-pooling ureq agent. Share one across many downloads to
/// keep HTTP keep-alive sockets reusable.
pub fn build_agent() -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(30))
        .timeout_read(Duration::from_secs(120))
        .user_agent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) \
             Gecko/20100101 Firefox/91.0",
        )
        .build()
}

/// HTTP GET against the patch CDN with a shared agent. Optional byte range.
/// Buffers the response fully — use [`download_to_file`] for large files.
pub async fn get_url_data_with(
    agent: &ureq::Agent,
    url: &str,
    range: Option<(u64, u64)>,
) -> Result<Vec<u8>, WizPatchError> {
    let agent = agent.clone();
    let url = url.to_string();
    unblock(move || {
        let mut req = agent.get(&url);
        if let Some((start, end)) = range {
            req = req.set("Range", &format!("bytes={start}-{end}"));
        }
        let resp = req
            .call()
            .map_err(|e| WizPatchError::Http(e.to_string()))?;
        let mut buf = Vec::new();
        resp.into_reader()
            .read_to_end(&mut buf)
            .map_err(WizPatchError::Io)?;
        Ok(buf)
    })
    .await
}

/// Convenience: builds a one-shot agent. Prefer [`get_url_data_with`] for any
/// batch of requests.
pub async fn get_url_data(
    url: &str,
    range: Option<(u64, u64)>,
) -> Result<Vec<u8>, WizPatchError> {
    let agent = build_agent();
    get_url_data_with(&agent, url, range).await
}

/// Streams an HTTP GET directly into a file on disk in fixed-size chunks.
/// Constant memory regardless of file size. Creates parent directories.
pub async fn download_to_file(
    agent: &ureq::Agent,
    url: &str,
    dest: std::path::PathBuf,
) -> Result<u64, WizPatchError> {
    use std::io::{Read, Write};

    let agent = agent.clone();
    let url = url.to_string();
    unblock(move || -> Result<u64, WizPatchError> {
        let resp = agent
            .get(&url)
            .call()
            .map_err(|e| WizPatchError::Http(e.to_string()))?;

        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::File::create(&dest)?;
        let mut reader = resp.into_reader();
        let mut buf = vec![0u8; 64 * 1024];
        let mut total: u64 = 0;
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n])?;
            total += n as u64;
        }
        file.flush()?;
        Ok(total)
    })
    .await
}
