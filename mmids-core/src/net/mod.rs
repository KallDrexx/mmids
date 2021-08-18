use std::fmt::Formatter;

pub mod tcp;

#[derive(Clone, Debug, Eq, Hash)]
pub struct ConnectionId(String);

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
