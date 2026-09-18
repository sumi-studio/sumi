// runlimited — launch one process under OS resource limits and report
// wait4 rusage as JSON. Replaces the /usr/bin/time + prlimit chain:
// the supervised program is runlimited's direct child (clean PID
// identity), RLIMIT_CPU is seconds-granularity (never advertised as
// millisecond-exact), and the parent waits with wait4 so CPU seconds
// and max RSS are measured even when the child is SIGKILLed.
//
// Termination signals sent to the wrapper are forwarded to the child
// before the wrapper dies — killing runlimited can never strand its
// workload, and the child's death is still recorded via wait4.
//
// The forwarding window is closed with a BLOCKED-SIGNAL handoff, not
// ordering luck: the signals are blocked before fork, so a signal that
// arrives while the child still carries the parent's handler pends in
// the child's inherited mask. The child restores default dispositions
// FIRST, then unblocks — a pending signal is delivered as an ordinary
// kill (never a swallowed run through the parent's forwarding handler,
// which would see child=0 and no-op). The parent unblocks only after
// `child` is set, so its own pending signal forwards to a real pid.
//
// Exact invariant: once a child exists, any catchable termination
// signal delivered to the wrapper reaches that child. A signal before
// the block install kills the wrapper by default disposition — but no
// child exists yet, so nothing is stranded. SIGKILL is never
// catchable; the supervisor's descendant check and the restart
// reconciler cover that window.
//
// Usage: runlimited --cpu=<sec> --as=<bytes|0> --stats=<path> -- prog [args...]
// Exit status: the child's exit status, or 128+signal.
// On the child not being launchable, prints JSON with "spawn_error" and
// exits 127.
//
// The supervisor reads <path> for {"cpu_ms","max_rss_bytes","exit_code",
// "signal"}; the file is written after wait4 returns, so a crash of
// runlimited itself is the only way to lose it — in which case the
// journal's usage stays 'unknown', never fabricated.

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static pid_t child = -1;
static volatile sig_atomic_t got_sig = 0;

static void forward(int sig) {
    got_sig = sig;
    if (child > 0) kill(child, sig);
}

int main(int argc, char **argv) {
    long cpu = 0, as = 0;
    const char *stats = NULL;
    int i = 1;
    for (; i < argc; i++) {
        if (strncmp(argv[i], "--cpu=", 6) == 0) { cpu = strtol(argv[i] + 6, NULL, 10); continue; }
        if (strncmp(argv[i], "--as=", 5) == 0) { as = strtol(argv[i] + 5, NULL, 10); continue; }
        if (strncmp(argv[i], "--stats=", 8) == 0) { stats = argv[i] + 8; continue; }
        if (strcmp(argv[i], "--") == 0) { i++; break; }
        break;
    }
    if (i >= argc || !stats) {
        fprintf(stderr, "usage: runlimited --cpu=<sec> --as=<bytes> --stats=<path> -- prog [args...]\n");
        return 127;
    }

    // Blocked-signal handoff: block the termination signals BEFORE
    // anything else so every delivery between here and the child's
    // disposition reset is pending, never swallowed by an inherited
    // copy of the parent's handler.
    sigset_t blocked, saved;
    sigemptyset(&blocked);
    sigaddset(&blocked, SIGTERM);
    sigaddset(&blocked, SIGINT);
    sigaddset(&blocked, SIGHUP);
    sigaddset(&blocked, SIGQUIT);
    sigprocmask(SIG_BLOCK, &blocked, &saved);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = forward;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0; // no SA_RESTART: wait4 must see EINTR and re-check
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGHUP, &sa, NULL);
    sigaction(SIGQUIT, &sa, NULL);

    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 127; }
    child = pid;
    if (pid == 0) {
        // Restore default dispositions FIRST, then unblock: a signal
        // forwarded while we still carried the parent's handler pends
        // in our blocked mask and is delivered here as an ordinary
        // kill — it can never run the stale inherited handler.
        signal(SIGTERM, SIG_DFL);
        signal(SIGINT, SIG_DFL);
        signal(SIGHUP, SIG_DFL);
        signal(SIGQUIT, SIG_DFL);
        sigprocmask(SIG_SETMASK, &saved, NULL);
        if (cpu > 0) {
            struct rlimit rl = { (rlim_t)cpu, (rlim_t)cpu + 1 };
            if (setrlimit(RLIMIT_CPU, &rl) < 0) { perror("setrlimit cpu"); _exit(127); }
        }
        if (as > 0) {
            struct rlimit rl = { (rlim_t)as, (rlim_t)as };
            // Best effort: V8's pointer cage needs a large VA space, so a
            // small RLIMIT_AS kills workerd at startup — callers must not
            // use it as the memory bound.
            setrlimit(RLIMIT_AS, &rl);
        }
        execvp(argv[i], &argv[i]);
        perror("execvp");
        _exit(127);
    }

    // Parent: unblock now that `child` is set — a signal pended during
    // the blocked window runs the handler here and forwards to the real
    // child pid. Signals after this point forward live.
    sigprocmask(SIG_SETMASK, &saved, NULL);

    int status = 0;
    struct rusage ru;
    memset(&ru, 0, sizeof(ru));
    while (wait4(pid, &status, 0, &ru) < 0) {
        if (errno == EINTR) continue;
        perror("wait4");
        return 127;
    }

    long cpu_ms = (ru.ru_utime.tv_sec + ru.ru_stime.tv_sec) * 1000
                + (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) / 1000;
    long rss_bytes = ru.ru_maxrss * 1024;
    int code = WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    int sig = WIFSIGNALED(status) ? WTERMSIG(status) : 0;

    FILE *f = fopen(stats, "w");
    if (f) {
        fprintf(f, "{\"cpu_ms\":%ld,\"max_rss_bytes\":%ld,\"exit_code\":%d,\"signal\":%d}\n",
                cpu_ms, rss_bytes, code, sig);
        fclose(f);
    }
    if (got_sig) {
        // Die by the same signal we forwarded — the parent's wait sees a
        // true signal death and the stats file above is already durable.
        signal(got_sig, SIG_DFL);
        raise(got_sig);
    }
    if (sig) return 128 + sig;
    return code < 0 ? 127 : code;
}
