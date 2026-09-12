# Browser Push notifications

Messaging notification settings include **この端末への通知**. Enabling it asks
the browser for notification permission and registers that browser's endpoint.
Disabling removes its server registration and browser subscription. The choice
to disable is retained for that Human on that browser, including across tabs.
Conversation notification settings separately select which messages notify.

The web manifest and home-screen icons support installing Sumi as a web app.
Permission requests originate from an explicit button gesture. Unsupported or
denied permission and registration failures have visible recovery guidance.

## Lifetime and account changes

- HTTP login retains its existing 15-minute lifetime. It does not authorize
  delivery while the application is closed.
- A separate HttpOnly, SameSite=Lax, application-scoped `sumi_push_device`
  cookie identifies a notification device. It lasts 30 days and is renewed by
  an authenticated session exchange. Only a hash is persisted in `push_devices`.
  The cookie cannot authenticate application requests or read conversations.
- Same-Human login refresh preserves the device. Switching Human replaces it
  and removes its old subscriptions. Logout revokes it even when the short
  HTTP session cookie has expired. Failed revocation retains the authenticated
  UI and cookie so logout can be retried.
- Every send rechecks current Workspace/app authority, exact recipient
  membership tenure, device lifetime, and endpoint ownership. Revocation waits
  for an already-started send; once it succeeds, queued sends cannot start for
  that device. A notification already accepted by a push service cannot be
  recalled by logout.
- Expired registrations are cleaned in bounded batches during later logins.
  Housekeeping failure does not undo a successful login.

## Deployment and validation

Configure `SUMI_MESSAGING_PUSH_SUBJECT` to a contact URL or email address. VAPID
keys remain persisted in PostgreSQL. Migration 0046 retires old session-bound
subscriptions, which have no device cookie; browsers register again after the
next login. Rolling back also requires registration renewal. VAPID keys and
message/notification history are preserved.

Local and Workers origins have separate cookies and browser subscriptions.
The service worker sends a generic notification and a route pointer; message
bodies, attachment names, and participant display names are not in the Push
payload. Dispatch remains best-effort after message commit, without a durable
retry queue.

Integration tests cover actual session signing/rotation, cookie routing and
database persistence, account switching, logout failure and retry, queued and
in-flight sends, and current membership/device authority. Browser tests cover
permission and device-control state transitions. Actual platform acceptance
also requires enabling notifications on a real browser/PWA, closing the app,
receiving a new message, opening its destination, and verifying notification
off and logout on that same device. Controlled transport tests do not prove
APNs/FCM or OS notification delivery.
