//! Transport boundary for endpoints selected by a user, rather than an operator.
//! DNS is checked in the connector itself: no second resolution occurs between
//! validation and connect, including when a pooled connection is replaced.
use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::{Arc, OnceLock},
};

use reqwest::{
    Url,
    dns::{Addrs, Name, Resolve, Resolving},
};

use super::model::ModelSpec;

pub(crate) fn validate_url(value: &str) -> Result<(), String> {
    let url = Url::parse(value).map_err(|_| "invalid model endpoint".to_owned())?;
    if url.scheme() != "https"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host().is_none()
    {
        return Err(
            "user model endpoint must be HTTPS without credentials, query or fragment".into(),
        );
    }
    if let Some(host) = url.host_str()
        && let Ok(ip) = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
        && !public_ip(ip)
    {
        return Err("model endpoint is not public".into());
    }
    Ok(())
}

fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, d] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 168 && b == 63 && c == 129 && d == 16)
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && b == 168)
                || (a == 192 && b == 0 && c == 0)
                || (a == 192 && b == 0 && c == 2)
                || (a == 192 && b == 88 && c == 99)
                || (a == 198 && (b == 18 || b == 19))
                || (a == 198 && b == 51 && c == 100)
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            // Global unicast only; reject translation, mapped, transition,
            // documentation and special protocol allocations conservatively.
            (s[0] & 0xe000) == 0x2000
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
                && s[0] != 0x2002
                && s[0] != 0x3ffe
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}

fn checked_addresses(addresses: Vec<SocketAddr>) -> io::Result<Addrs> {
    if addresses.is_empty() || addresses.iter().any(|address| !public_ip(address.ip())) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "model DNS destination is not public",
        ));
    }
    Ok(Box::new(addresses.into_iter()))
}

struct PublicResolver;
impl Resolve for PublicResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_owned();
        Box::pin(async move {
            let addresses = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            checked_addresses(addresses).map_err(Into::into)
        })
    }
}

fn builder() -> reqwest::ClientBuilder {
    reqwest::Client::builder()
        .user_agent(concat!("sumi-agent/", env!("CARGO_PKG_VERSION")))
        .connect_timeout(super::CONNECT_TIMEOUT)
        .no_proxy()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(Arc::new(PublicResolver))
}

pub(crate) fn client(spec: &ModelSpec) -> Result<&'static reqwest::Client, String> {
    if !spec.public_endpoint {
        return super::http_client();
    }
    validate_url(&spec.base_url)?;
    static CLIENT: OnceLock<Result<reqwest::Client, String>> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            builder()
                .build()
                .map_err(|_| "failed to build public model transport".into())
        })
        .as_ref()
        .map_err(Clone::clone)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_public_and_ambiguous_urls() {
        for value in [
            "http://api.example/v1",
            "https://a:b@api.example/v1",
            "https://api.example/?key=x",
            "https://api.example/#x",
            "https://127.1/v1",
            "https://2130706433/v1",
            "https://[::1]/v1",
            "https://[::ffff:8.8.8.8]/v1",
            "https://100.100.100.200/v1",
            "https://169.254.169.254/v1",
        ] {
            assert!(validate_url(value).is_err(), "{value}");
        }
        assert!(validate_url("https://api.example:8443/custom/v1").is_ok());
    }

    #[test]
    fn every_dns_answer_is_checked_including_rebinding_and_mixed_answers() {
        let public: SocketAddr = "8.8.8.8:0".parse().unwrap();
        let private: SocketAddr = "10.0.0.1:0".parse().unwrap();
        assert!(checked_addresses(vec![public]).is_ok());
        // Same host's next resolution must not inherit the preceding verdict.
        assert!(checked_addresses(vec![private]).is_err());
        assert!(checked_addresses(vec![public, private]).is_err());
        assert!(checked_addresses(vec![]).is_err());
        for value in [
            "192.168.0.1",
            "198.19.1.1",
            "224.0.0.1",
            "fc00::1",
            "fe80::1",
            "64:ff9b::a00:1",
            "2002:a00:1::",
            "2001:db8::1",
        ] {
            assert!(!public_ip(value.parse().unwrap()), "{value}");
        }
        assert!(public_ip("2606:4700:4700::1111".parse().unwrap()));
    }

    #[tokio::test]
    async fn connection_resolver_blocks_localhost_before_connecting() {
        let resolver = PublicResolver;
        assert!(
            resolver
                .resolve("localhost".parse().unwrap())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn redirect_response_does_not_forward_credentials() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            let n = socket.read(&mut request).await.unwrap();
            assert!(String::from_utf8_lossy(&request[..n]).contains("Bearer test-secret"));
            socket.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{address}/steal\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
        });
        // Test fixture relaxes HTTPS only, retaining the production redirect
        // policy. Production never permits this loopback HTTP URL.
        let client = builder().https_only(false).build().unwrap();
        let response = client
            .post(format!("http://{address}/v1"))
            .bearer_auth("test-secret")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 307);
        server.await.unwrap();
    }
}
