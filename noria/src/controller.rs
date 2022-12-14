use crate::consensus::Authority;
use crate::debug::stats;
use crate::rpc::{ControllerHandle, ControllerRequest};
use crate::table::{Table, TableBuilder};
use crate::view::{View, ViewBuilder};
use crate::ActivationResult;
use failure::{self, ResultExt};
use futures_util::future;
use petgraph::graph::NodeIndex;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::{
    future::Future,
    task::{Context, Poll},
};
use tower_service::Service;

// Needed b/c of https://github.com/rust-lang/rust/issues/65442
pub(crate) async fn finalize<R, E>(
    fut: impl Future<Output = Result<hyper::body::Bytes, E>>,
    err: &'static str,
) -> Result<R, failure::Error>
where
    for<'de> R: Deserialize<'de>,
    E: std::fmt::Display + Send + Sync + 'static,
{
    let body: hyper::body::Bytes = fut.await.map_err(failure::Context::new).context(err)?;

    serde_json::from_slice::<R>(&body)
        .context("failed to response")
        .context(err)
        .map_err(failure::Error::from)
}

impl<A: Authority + 'static> ControllerHandle<A> {
    /// Check that the `ControllerHandle` can accept another request.
    ///
    /// Note that this method _must_ return `Poll::Ready` before any other methods that return
    /// a `Future` on `ControllerHandle` can be called.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), failure::Error>> {
        self.handle
            .poll_ready(cx)
            .map_err(failure::Error::from_boxed_compat)
    }

    /// A future that resolves when the controller can accept more messages.
    ///
    /// When this future resolves, you it is safe to call any methods that require `poll_ready` to
    /// have returned `Poll::Ready`.
    pub async fn ready(&mut self) -> Result<(), failure::Error> {
        future::poll_fn(move |cx| self.poll_ready(cx)).await
    }

    /// Enumerate all known base tables.
    ///
    /// These have all been created in response to a `CREATE TABLE` statement in a recipe.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn inputs(
        &mut self,
    ) -> impl Future<Output = Result<BTreeMap<String, NodeIndex>, failure::Error>> {
        let fut = self
            .handle
            .call(ControllerRequest::new("inputs", &()).unwrap());

        async move {
            let body: hyper::body::Bytes = fut
                .await
                .map_err(failure::Context::new)
                .context("failed to fetch inputs")?;

            serde_json::from_slice(&body)
                .context("couldn't parse input response")
                .map_err(failure::Error::from)
        }
    }

    /// Enumerate all known external views.
    ///
    /// These have all been created in response to a `CREATE EXT VIEW` statement in a recipe.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn outputs(
        &mut self,
    ) -> impl Future<Output = Result<BTreeMap<String, NodeIndex>, failure::Error>> {
        let fut = self
            .handle
            .call(ControllerRequest::new("outputs", &()).unwrap());

        async move {
            let body: hyper::body::Bytes = fut
                .await
                .map_err(failure::Context::new)
                .context("failed to fetch outputs")?;

            serde_json::from_slice(&body)
                .context("couldn't parse output response")
                .map_err(failure::Error::from)
        }
    }

    /// Obtain a `View` that allows you to query the given external view.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn view(&mut self, name: &str) -> impl Future<Output = Result<View, failure::Error>> {
        // This call attempts to detect if this function is being called in a loop. If this is
        // getting false positives, then it is safe to increase the allowed hit count, however, the
        // limit_mutator_creation test in src/controller/handle.rs should then be updated as well.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        let views = self.views.clone();
        let name = name.to_string();
        let fut = self
            .handle
            .call(ControllerRequest::new("view_builder", &name).unwrap());
        async move {
            let body: hyper::body::Bytes = fut
                .await
                .map_err(failure::Context::new)
                .context("failed to fetch view builder")?;

            match serde_json::from_slice::<Option<ViewBuilder>>(&body) {
                Ok(Some(vb)) => Ok(vb.build(views)?),
                Ok(None) => Err(failure::err_msg("view does not exist")),
                Err(e) => Err(failure::Error::from(e)),
            }
            .map_err(move |e| e.context(format!("building view for {}", name)).into())
        }
    }

    /// Obtain a `Table` that allows you to perform writes, deletes, and other operations on the
    /// given base table.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn table(&mut self, name: &str) -> impl Future<Output = Result<Table, failure::Error>> {
        // This call attempts to detect if this function is being called in a loop. If this
        // is getting false positives, then it is safe to increase the allowed hit count.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        let domains = self.domains.clone();
        let name = name.to_string();
        let fut = self
            .handle
            .call(ControllerRequest::new("table_builder", &name).unwrap());

        async move {
            let body: hyper::body::Bytes = fut
                .await
                .map_err(failure::Context::new)
                .context("failed to fetch table builder")?;

            match serde_json::from_slice::<Option<TableBuilder>>(&body) {
                Ok(Some(tb)) => Ok(tb.build(domains)?),
                Ok(None) => Err(failure::err_msg("view table not exist")),
                Err(e) => Err(failure::Error::from(e)),
            }
            .map_err(move |e| e.context(format!("building table for {}", name)).into())
        }
    }

    /// Get statistics about the time spent processing different parts of the graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn statistics(
        &mut self,
    ) -> impl Future<Output = Result<stats::GraphStats, failure::Error>> {
        self.rpc("get_statistics", (), "failed to get stats")
    }

    /// Flush all partial state, evicting all rows present.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn flush_partial(&mut self) -> impl Future<Output = Result<(), failure::Error>> {
        self.rpc("flush_partial", (), "failed to flush partial")
    }

    /// Extend the existing recipe with the given set of queries.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn extend_recipe(
        &mut self,
        recipe_addition: &str,
    ) -> impl Future<Output = Result<ActivationResult, failure::Error>> {
        self.rpc("extend_recipe", recipe_addition, "failed to extend recipe")
    }

    /// Replace the existing recipe with this one.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn install_recipe(
        &mut self,
        new_recipe: &str,
    ) -> impl Future<Output = Result<ActivationResult, failure::Error>> {
        self.rpc("install_recipe", new_recipe, "failed to install recipe")
    }

    /// Fetch a graphviz description of the dataflow graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn graphviz(&mut self) -> impl Future<Output = Result<String, failure::Error>> {
        self.rpc("graphviz", (), "failed to fetch graphviz output")
    }

    /// Fetch a simplified graphviz description of the dataflow graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn simple_graphviz(&mut self) -> impl Future<Output = Result<String, failure::Error>> {
        self.rpc(
            "simple_graphviz",
            (),
            "failed to fetch simple graphviz output",
        )
    }

    /// Remove the given external view from the graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn remove_node(
        &mut self,
        view: NodeIndex,
    ) -> impl Future<Output = Result<(), failure::Error>> {
        // TODO: this should likely take a view name, and we should verify that it's a Reader.
        self.rpc("remove_node", view, "failed to remove node")
    }
}
