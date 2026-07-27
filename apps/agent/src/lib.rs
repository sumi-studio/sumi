pub mod memory;
pub mod provider;

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
