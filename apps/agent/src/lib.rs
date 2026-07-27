// The binary remains the production composition root. These private module
// mirrors keep the library's compactor unit tests linked to the durable Store
// and runtime contracts without exposing a second public lib/bin API.
mod agent;
#[allow(dead_code, unused_imports)]
mod apiclient;
#[allow(dead_code, unused_imports)]
mod approval;
#[allow(dead_code, unused_imports)]
mod config;
#[allow(dead_code, unused_imports)]
mod gateway;
pub mod memory;
#[allow(dead_code, unused_imports)]
mod prompts;
pub mod provider;
#[allow(dead_code)]
mod runtime;
#[allow(dead_code, unused_imports)]
mod store;
#[allow(unused_imports)]
mod tools;

#[cfg(test)]
pub(crate) const fn canonical_live_responses_harness_available() -> bool {
    false
}

#[cfg(test)]
pub(crate) async fn run_canonical_live_responses_roundtrip(
    _spec: provider::ModelSpec,
    _api_key: String,
) {
    panic!("canonical live Session harness is available only in the sumi-agent binary tests");
}
