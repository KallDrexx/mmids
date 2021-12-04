//! Provides mechanisms to define routes for the Mmids HTTP apis, and what code should be executed
//! for each route.

use async_trait::async_trait;
use hyper::{Body, Method, Request, Response};
use std::collections::HashMap;

/// Defines how a single fragment of the URL path should be read as.  Each part is the whole value
/// between a `/` and either another `/` or the end of the string.  Query parameters are not
/// considered.
#[derive(Clone)]
pub enum PathPart {
    /// The fragment of the path should match this exact string value.  This *is* case sensitive.
    Exact { value: String },

    /// The fragment of the path can match any string.  The string value of this part of the path
    /// will be stored as a parameter with the key being the specified name of the parameter, and
    /// the value being the actual string in the path.
    Parameter { name: String },
}

/// Represents code that will be executed for a given route.
///
/// Note: this trait uses the `async_trait` crate
#[async_trait]
pub trait RouteHandler {
    /// Executes the handler for the specified HTTP request and pre-parsed path parameters.
    ///
    /// Note that implementors can use `async_trait` to clean up the signature.
    async fn execute(
        &self,
        request: &mut Request<Body>,
        path_parameters: HashMap<String, String>,
    ) -> Result<Response<Body>, hyper::Error>;
}

/// Defines the HTTP method, a specific path, and which handler should execute requests that match
/// the route.
pub struct Route {
    pub method: Method,
    pub path: Vec<PathPart>,
    pub handler: Box<dyn RouteHandler + Sync + Send>,
}

/// Errors that can occur when registering new routes with the routing table
#[derive(thiserror::Error, Debug)]
pub enum RouteRegistrationError {
    /// Raised when attempting to register a route whose http method and path parts match an
    /// route that's already been registered.
    #[error("A route is already registered that conflicts with this route")]
    RouteConflict,
}

/// A system that contains all available routes.  Routes may be registered with it and can then be
/// looked up from.
pub struct RoutingTable {
    routes: HashMap<Method, RouteNode>,
}

#[derive(PartialEq, Eq, Hash)]
enum SearchablePathPart {
    Exact(String),
    Parameter,
}

struct RouteNode {
    leaf: Option<Route>,
    children: HashMap<SearchablePathPart, RouteNode>,
}

impl RoutingTable {
    /// Creates an empty routing table
    pub fn new() -> Self {
        RoutingTable {
            routes: HashMap::new(),
        }
    }

    /// Registers a route to be available by the routing table
    pub fn register(&mut self, route: Route) -> Result<(), RouteRegistrationError> {
        let mut node = self
            .routes
            .entry(route.method.clone())
            .or_insert(RouteNode {
                leaf: None,
                children: HashMap::new(),
            });

        for part in &route.path {
            let searchable_part = match part {
                PathPart::Exact { value: name } => SearchablePathPart::Exact(name.clone()),
                PathPart::Parameter { .. } => SearchablePathPart::Parameter,
            };

            node = node.children.entry(searchable_part).or_insert(RouteNode {
                leaf: None,
                children: HashMap::new(),
            });
        }

        if node.leaf.is_some() {
            return Err(RouteRegistrationError::RouteConflict);
        }

        node.leaf = Some(route);

        Ok(())
    }

    pub(super) fn get_route(&self, method: &Method, path_parts: &Vec<&str>) -> Option<&Route> {
        let node = match self.routes.get(method) {
            Some(node) => node,
            None => return None,
        };

        find_route(0, &path_parts, node)
    }
}

fn find_route<'a>(
    index: usize,
    parts: &Vec<&str>,
    current_node: &'a RouteNode,
) -> Option<&'a Route> {
    if index >= parts.len() {
        return match &current_node.leaf {
            Some(route) => Some(route),
            None => None,
        };
    }

    if let Some(exact_child) = current_node
        .children
        .get(&SearchablePathPart::Exact(parts[index].to_string()))
    {
        if let Some(route) = find_route(index + 1, parts, exact_child) {
            return Some(route);
        }
    }

    if let Some(parameter_child) = current_node.children.get(&SearchablePathPart::Parameter) {
        if let Some(route) = find_route(index + 1, parts, parameter_child) {
            return Some(route);
        }
    }

    None
}

impl Route {
    pub(super) fn get_parameters(&self, path_parts: &Vec<&str>) -> HashMap<String, String> {
        let mut results = HashMap::new();
        for x in 0..self.path.len() {
            if let PathPart::Parameter { name } = &self.path[x] {
                results.insert(name.clone(), path_parts[x].to_string());
            }
        }

        results
    }
}
