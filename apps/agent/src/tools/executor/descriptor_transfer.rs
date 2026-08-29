//! Reliable, ordered read-only descriptor transfer over the authenticated
//! executor Unix stream.
//!
//! The executor's update frames are volatile and cannot carry authoritative
//! attachment bytes, and the runtime has no Workspace mount. Passing the
//! opened, policy-checked descriptors themselves with `SCM_RIGHTS` on the
//! terminal frame keeps every byte read on the executor's read-only Workspace
//! while the runtime streams them into exact-scoped Messaging uploads. Order
//! is the ancillary array order; the count is checked against the manifest.

use std::{
    io, mem,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    ptr,
};

use tokio::io::{Interest, unix::AsyncFd};

use super::protocol::MAX_SOURCE_FILES_PER_OPERATION;

/// Send `bytes` on `socket`, attaching `fds` (in order) to the first written
/// byte. Returns the number of payload bytes accepted by the kernel.
pub(super) fn sendmsg_with_fds(socket: RawFd, bytes: &[u8], fds: &[RawFd]) -> io::Result<usize> {
    if fds.len() > MAX_SOURCE_FILES_PER_OPERATION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "too many descriptors for one message",
        ));
    }
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr() as *mut libc::c_void,
        iov_len: bytes.len(),
    };
    let fd_bytes = std::mem::size_of_val(fds);
    let control_len = if fds.is_empty() {
        0
    } else {
        // SAFETY: CMSG_SPACE is a pure size computation.
        unsafe { libc::CMSG_SPACE(fd_bytes as u32) as usize }
    };
    let mut control = vec![0u8; control_len];
    // SAFETY: msghdr is plain data; all pointers reference live buffers.
    let mut header: libc::msghdr = unsafe { mem::zeroed() };
    header.msg_iov = &mut iov;
    header.msg_iovlen = 1;
    if !fds.is_empty() {
        header.msg_control = control.as_mut_ptr() as *mut libc::c_void;
        header.msg_controllen = control_len as _;
        // SAFETY: control is at least CMSG_SPACE(fd_bytes) long.
        unsafe {
            let cmsg = libc::CMSG_FIRSTHDR(&header);
            (*cmsg).cmsg_level = libc::SOL_SOCKET;
            (*cmsg).cmsg_type = libc::SCM_RIGHTS;
            (*cmsg).cmsg_len = libc::CMSG_LEN(fd_bytes as u32) as _;
            ptr::copy_nonoverlapping(fds.as_ptr() as *const u8, libc::CMSG_DATA(cmsg), fd_bytes);
        }
    }
    // SAFETY: header and its buffers are valid for the call duration.
    let sent = unsafe { libc::sendmsg(socket, &header, libc::MSG_NOSIGNAL) };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(sent as usize)
}

/// Receive into `buffer`, appending any `SCM_RIGHTS` descriptors to `fds`.
/// A truncated control message closes every received descriptor and fails:
/// silently losing descriptors would desynchronise order.
pub(super) fn recvmsg_with_fds(
    socket: RawFd,
    buffer: &mut [u8],
    fds: &mut Vec<OwnedFd>,
) -> io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: buffer.as_mut_ptr() as *mut libc::c_void,
        iov_len: buffer.len(),
    };
    let capacity = mem::size_of::<RawFd>() * (MAX_SOURCE_FILES_PER_OPERATION + 1);
    // SAFETY: CMSG_SPACE is a pure size computation.
    let control_len = unsafe { libc::CMSG_SPACE(capacity as u32) as usize };
    let mut control = vec![0u8; control_len];
    // SAFETY: msghdr is plain data; all pointers reference live buffers.
    let mut header: libc::msghdr = unsafe { mem::zeroed() };
    header.msg_iov = &mut iov;
    header.msg_iovlen = 1;
    header.msg_control = control.as_mut_ptr() as *mut libc::c_void;
    header.msg_controllen = control_len as _;
    // SAFETY: header and its buffers are valid for the call duration.
    let received = unsafe { libc::recvmsg(socket, &mut header, libc::MSG_CMSG_CLOEXEC) };
    if received < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut received_fds = Vec::new();
    // SAFETY: the kernel filled msg_control up to msg_controllen with valid
    // cmsg headers; CMSG_* walk them.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&header);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let data_len = (*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
                let count = data_len / mem::size_of::<RawFd>();
                let data = libc::CMSG_DATA(cmsg) as *const RawFd;
                for index in 0..count {
                    let raw = ptr::read_unaligned(data.add(index));
                    received_fds.push(OwnedFd::from_raw_fd(raw));
                }
            }
            cmsg = libc::CMSG_NXTHDR(&header, cmsg);
        }
    }
    if header.msg_flags & libc::MSG_CTRUNC != 0 {
        drop(received_fds);
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "descriptor control message was truncated",
        ));
    }
    if fds.len() + received_fds.len() > MAX_SOURCE_FILES_PER_OPERATION {
        drop(received_fds);
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer sent more descriptors than one operation allows",
        ));
    }
    fds.extend(received_fds);
    Ok(received as usize)
}

/// Write one complete frame with descriptors attached to its first byte,
/// waiting for writability between partial writes.
pub(super) async fn send_frame_with_fds(
    socket: &AsyncFd<OwnedFd>,
    bytes: &[u8],
    fds: &[RawFd],
) -> io::Result<()> {
    let mut written = 0usize;
    while written < bytes.len() {
        let mut guard = socket.writable().await?;
        let attach: &[RawFd] = if written == 0 { fds } else { &[] };
        match guard.try_io(|inner| sendmsg_with_fds(inner.as_raw_fd(), &bytes[written..], attach)) {
            Ok(Ok(sent)) => {
                if sent == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "socket accepted no bytes",
                    ));
                }
                written += sent;
            }
            Ok(Err(error)) => return Err(error),
            Err(_would_block) => continue,
        }
    }
    Ok(())
}

/// Read available bytes once the stream is readable, collecting descriptors.
pub(super) async fn recv_chunk_with_fds(
    stream: &tokio::net::UnixStream,
    buffer: &mut [u8],
    fds: &mut Vec<OwnedFd>,
) -> io::Result<usize> {
    loop {
        stream.readable().await?;
        match stream.try_io(Interest::READABLE, || {
            recvmsg_with_fds(stream.as_raw_fd(), buffer, fds)
        }) {
            Ok(read) => return Ok(read),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Seek, SeekFrom, Write},
        os::fd::AsFd,
    };

    use super::*;

    fn scratch_file(index: u8) -> std::fs::File {
        let path = std::env::temp_dir().join(format!(
            "sumi-descriptor-transfer-{}-{index}",
            std::process::id()
        ));
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let _ = std::fs::remove_file(&path);
        file.write_all(&[index; 8]).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        file
    }

    #[tokio::test]
    async fn descriptors_ride_the_frame_in_order() {
        let (left, right) = tokio::net::UnixStream::pair().unwrap();
        let sender = AsyncFd::new(left.as_fd().try_clone_to_owned().unwrap()).unwrap();
        let mut files = Vec::new();
        for index in 0..3u8 {
            files.push(scratch_file(index));
        }
        let raw: Vec<RawFd> = files.iter().map(|file| file.as_raw_fd()).collect();
        let payload = b"{\"type\":\"terminal\"}\n".to_vec();
        send_frame_with_fds(&sender, &payload, &raw).await.unwrap();

        let mut buffer = vec![0u8; 1024];
        let mut received = Vec::new();
        let read = recv_chunk_with_fds(&right, &mut buffer, &mut received)
            .await
            .unwrap();
        assert_eq!(&buffer[..read], payload.as_slice());
        assert_eq!(received.len(), 3);
        for (index, fd) in received.into_iter().enumerate() {
            let mut file = std::fs::File::from(fd);
            let mut content = Vec::new();
            file.read_to_end(&mut content).unwrap();
            assert_eq!(content, vec![index as u8; 8]);
        }
    }

    #[test]
    fn refuses_more_descriptors_than_one_operation_allows() {
        let (left, _right) = std::os::unix::net::UnixStream::pair().unwrap();
        let fds = vec![0 as RawFd; MAX_SOURCE_FILES_PER_OPERATION + 1];
        assert!(sendmsg_with_fds(left.as_raw_fd(), b"x", &fds).is_err());
    }
}
