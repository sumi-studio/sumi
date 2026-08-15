//! Process-wide memory disclosure hardening installed before mode dispatch.

use anyhow::{Result, bail};

pub(crate) fn disable_dumps_and_core_files() -> Result<()> {
    let core_limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let limit_result = unsafe { libc::setrlimit(libc::RLIMIT_CORE, &core_limit) };
    if limit_result != 0 {
        bail!(
            "failed to disable core files: {}",
            std::io::Error::last_os_error()
        );
    }

    let dump_result = unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) };
    if dump_result != 0 {
        bail!(
            "failed to disable process dumping: {}",
            std::io::Error::last_os_error()
        );
    }
    Ok(())
}
