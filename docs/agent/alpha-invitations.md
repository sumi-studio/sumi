# Alpha invitations

Alpha enrollment is limited to invited developers and testers. A Sumi account
and membership of a Workspace are separate grants. A combined invitation can
carry both, but account creation never joins the Workspace automatically.

## Operator setup

Set `SUMI_ENROLLMENT_ADMIN_HUMAN_IDS` on the API to a comma-separated list of
existing Human UUIDs. Confirm each ID against the intended person's account;
display names are not identifiers. An empty value grants nobody permission to
issue enrollment invitations. Invalid or nonexistent IDs prevent startup rather
than silently creating a different policy. Keep the list in deployment
configuration, not in a shared browser build.

Use the normal migration runner before starting the new API. This feature adds
enrollment invitations and their Workspace relationships. The release contract permits one new migration version at a time: this feature
uses the next version, 0043, for enrollment and Workspace reservation together.
Extend FROZEN.sha256 using the migration-freeze script; never modify a sealed
migration or insert a version before already released migrations.

Existing users can still sign in. A new account requires a valid enrollment
invitation, including when an unknown identity chooses the sign-in flow. The
invitation check and account creation share a transaction. A failed registration
does not consume the invitation.

## Sending an invitation

An enrollment administrator can create an invitation from the account settings
menu. The raw link is shown when created; only metadata is available later.
Copy the link and send it through the intended channel. Creating an invitation
does not send an email or message automatically.

A Workspace member with `manage_members` can create an ordinary Workspace
invitation. If that person also has enrollment-admin authority, the Workspace
invitation form offers a combined Sumi registration link. Issuing a combined
link requires both permissions on the server. It is for one recipient, and both
grants share the expiry.

An optional email address binds enrollment to a verified identity. For an
existing account accepting an email-bound combined invitation, the API also
checks that the verified Firebase identity belongs to the signed-in Human.

## Following a combined link

1. The browser captures both invitation values from the URL fragment and removes
   them from the visible URL. They are submitted to the relevant APIs as needed.
2. A new user registers; an existing user signs in or continues with their current
   account. Authentication redirects retain the pending Workspace invitation.
3. Sumi names the destination Workspace and asks whether to participate. Joining
   requires an explicit action. The person can decline or switch accounts.

After registration, the Workspace grant is reserved for that new Human. Another
person cannot use the remaining half of the link. An existing user's explicit
join retires the unused enrollment grant, including when that user is already a
Workspace member. Revoking either part also invalidates the other uncompleted
part. Revocation after registration does not delete the new account.

Pending values normally stay in the tab's session storage. Email-link
authentication also carries the Workspace code in its existing, expiring
pending-flow record so that the email can be opened in another tab of the same
browser. This does not establish cross-device continuation. No invitation token
is added to the email callback URL.

## Acceptance and limits

Check both the public Workers origin and the local Web/API origin: existing-user
sign-in, rejection of uninvited registration, one-use enrollment, explicit
Workspace join, cancellation, expiry, revocation and account switching. API and
fixture-browser tests do not prove an interactive Firebase sign-in succeeds on
the public hostname; verify that separately with its authorized-domain settings.

Authentication allocation is rate limited before database allocation. The
current limiter is local to the API process and uses the actual network peer.
VPC users may share that peer. It is not distributed per-user throttling or a
replacement for the hosting platform's traffic controls.
