//! Cancellation-safe Unix pipe support for merged command output.

use std::{
    fs::File,
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, ReadBuf, unix::AsyncFd};

pub(super) struct NonblockingPipeRead {
    inner: AsyncFd<OwnedFd>,
}

impl NonblockingPipeRead {
    fn new(fd: OwnedFd) -> io::Result<Self> {
        Ok(Self {
            inner: AsyncFd::new(fd)?,
        })
    }
}

impl AsyncRead for NonblockingPipeRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            let mut ready = match self.inner.poll_read_ready_mut(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(ready)) => ready,
            };
            match ready.try_io(|inner| read_into(inner.get_ref().as_raw_fd(), buffer)) {
                Ok(result) => return Poll::Ready(result),
                Err(_would_block) => continue,
            }
        }
    }
}

fn read_into(fd: RawFd, buffer: &mut ReadBuf<'_>) -> io::Result<()> {
    let destination = buffer.initialize_unfilled();
    if destination.is_empty() {
        return Ok(());
    }
    loop {
        let read = unsafe { libc::read(fd, destination.as_mut_ptr().cast(), destination.len()) };
        if read >= 0 {
            buffer.advance(usize::try_from(read).expect("read returned a nonnegative ssize_t"));
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

pub(super) fn merged_output_pipe() -> io::Result<(NonblockingPipeRead, File)> {
    let mut descriptors = [0; 2];
    #[cfg(target_os = "linux")]
    let result =
        unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
    #[cfg(not(target_os = "linux"))]
    let result = unsafe { libc::pipe(descriptors.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }

    let read = unsafe { OwnedFd::from_raw_fd(descriptors[0]) };
    let write = unsafe { OwnedFd::from_raw_fd(descriptors[1]) };
    #[cfg(not(target_os = "linux"))]
    {
        set_descriptor_flag(read.as_raw_fd(), libc::FD_CLOEXEC)?;
        set_descriptor_flag(write.as_raw_fd(), libc::FD_CLOEXEC)?;
        set_status_flag(read.as_raw_fd(), libc::O_NONBLOCK, true)?;
    }
    // pipe2 applies O_NONBLOCK to both ends, but command writers must retain
    // ordinary blocking pipe semantics and backpressure.
    #[cfg(target_os = "linux")]
    set_status_flag(write.as_raw_fd(), libc::O_NONBLOCK, false)?;

    Ok((NonblockingPipeRead::new(read)?, File::from(write)))
}

#[cfg(not(target_os = "linux"))]
fn set_descriptor_flag(fd: RawFd, flag: libc::c_int) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | flag) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn set_status_flag(fd: RawFd, flag: libc::c_int, enabled: bool) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    let updated = if enabled { flags | flag } else { flags & !flag };
    if unsafe { libc::fcntl(fd, libc::F_SETFL, updated) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
