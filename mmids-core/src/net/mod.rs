//! Networking layer for Mmids applications

use cidr_utils::cidr::{IpCidr, Ipv4Cidr};
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use std::net::Ipv4Addr;
use thiserror::Error;

pub mod tcp;

/// A unique identifier for any given TCP connection, or unique UDP client.  If a TCP client
/// disconnects and reconnects it will be seen with a brand new connection id
#[derive(Clone, Debug, Eq)]
pub struct ConnectionId(pub String);

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<Self> for ConnectionId {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for ConnectionId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

/// Enumeration to make handling ip addresses vs subnets easier
#[derive(Debug, PartialEq)]
pub enum IpAddress {
    Exact(Ipv4Addr),
    Cidr(Ipv4Cidr),
}

/// Error when a given ip address or subnet could not be parsed from a given input
#[derive(Error, Debug)]
pub enum IpAddressParseError {
    #[error("The value '{0}' was not a valid ip address or cidr value")]
    InvalidValue(String),
}

impl IpAddress {
    /// Checks if the other exact ip address is a match for the current ip address specification.
    /// An address is a match if the current ip address is an exact one and both are exactly equal,
    /// or if the current ip address is a CIDR subnet mask and the other ip address is contained
    /// within.
    pub fn matches(&self, other_address: &Ipv4Addr) -> bool {
        match self {
            IpAddress::Exact(self_address) => self_address == other_address,
            IpAddress::Cidr(cidr) => cidr.contains(other_address),
        }
    }

    /// Attempts to parse a string supposedly containing a comma delimited list of ip addresses
    /// and cidr values.  An empty string will return an empty collection of ips.
    pub fn parse_comma_delimited_list(
        input: Option<&String>,
    ) -> Result<Vec<IpAddress>, IpAddressParseError> {
        let mut ips = Vec::new();
        match input {
            None => (),
            Some(input) => {
                for input in input.split(",") {
                    let ip = if let Ok(ip) = input.parse::<Ipv4Addr>() {
                        Some(IpAddress::Exact(ip))
                    } else if let Ok(cidr) = IpCidr::from_str(input) {
                        match cidr {
                            IpCidr::V4(cidr) => Some(IpAddress::Cidr(cidr)),
                            _ => None,
                        }
                    } else {
                        None
                    };

                    if let Some(ip) = ip {
                        ips.push(ip);
                    } else {
                        return Err(IpAddressParseError::InvalidValue(input.to_string()));
                    }
                }
            }
        }

        Ok(ips)
    }
}
