// fork-delay.c — deterministic regression shim for the runlimited
// signal-handoff window (F373).
//
// LD_PRELOAD'd into runlimited, it delays ONLY the CHILD's return from
// the real fork() for $FORKDELAY_SECS seconds after creating the marker
// file named by $FORKDELAY_MARKER. That marks the exact interval in
// which the child still carries the parent's signal dispositions but
// has not yet reset them — the window in which a forwarded SIGTERM was
// previously swallowed by the inherited handler (which saw child==0/-1
// and no-opped).
//
// The test asserts the fix: the signal pends in the child's inherited
// blocked mask and is delivered as an ordinary kill after the child
// restores defaults — the exec'd workload can never survive.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>

pid_t fork(void) {
  pid_t (*real_fork)(void) = dlsym(RTLD_NEXT, "fork");
  pid_t p = real_fork();
  if (p == 0) {
    const char *mark = getenv("FORKDELAY_MARKER");
    if (mark) {
      int fd = open(mark, O_WRONLY | O_CREAT | O_EXCL, 0600);
      if (fd >= 0) { write(fd, "ready", 5); close(fd); }
    }
    long secs = 1;
    const char *s = getenv("FORKDELAY_SECS");
    if (s) { long v = atol(s); if (v > 0) secs = v; }
    struct timespec left = { secs, 0 };
    while (nanosleep(&left, &left) == -1 && errno == EINTR) {}
  }
  return p;
}
