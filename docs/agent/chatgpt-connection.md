# ChatGPT connection

Sumi can use a person's ChatGPT account for `gpt-6-astra`, while keeping Sumi's
own conversation history, tools, approvals, memory and execution loop. It does
not launch a Codex agent inside Sumi. This inference connection is separate from
Google/GitHub sign-in to Sumi.

## Connect

1. Open Sumi's model settings and choose **ChatGPTを接続してAstraを使う**.
2. Open the displayed ChatGPT login page and explicitly enter the displayed
   device code. Opening the link alone does not authorize the connection.
   If device login is unavailable, enable device-code login in ChatGPT settings
   and retry.
3. Complete authorization. Sumi saves the connection and selects **Astra /
   Medium**. The same settings screen can change effort or disconnect.

A connection belongs to the signed-in Human. Using it from a PA requires that
Human to remain the PA's current employer; the API rechecks this relationship.
The runtime binds requests to the selected connection and actual ChatGPT account
ID. Tokens are not returned to the browser or placed in runtime environment
variables.

Changes apply automatically when the current PA is safely idle. Active work is
not interrupted to change models; the runtime restarts the same PA with its
persisted state. The UI explains this delay; it does not yet report the running
model's activation status.

## Server setup

Use the control-plane PostgreSQL database with migration
`0037_chatgpt_connections`, and configure `SUMI_CHATGPT_CREDENTIAL_KEY` on the API
process. It must be a base64-encoded, random 32-byte key. Generate a new value with:

```sh
openssl rand -base64 32
```

Store the resulting value in the deployment's secret configuration. Keep the key
stable across API restarts: it encrypts the stored OAuth credentials with
AES-GCM. Do not put it in source control, the frontend, or agent containers.
Without this configuration, the connection service is unavailable.

Conversation activation selects `chatgpt-responses`, the model and effort, the
connection ID (`SUMI_CHATGPT_CONNECTION_ID`) and actual account ID
(`SUMI_MODEL_ACCOUNT_SCOPE`). Its requests use the native ChatGPT Responses Lite
backend. Credential refresh happens through the PA-authorized API control route.
No conversation API key is used by this native backend.

Execution and escalation reviewers keep their independently configured models
and API keys. Connecting ChatGPT does not silently replace configured Kimi
reviewers. A reviewer explicitly configured with the same ChatGPT preset uses
the native connection; reviewer account overrides must match that connection.
An API-key conversation backend still requires its own configured key.

Pending device-login flows and pending idle-switch work currently live in one
API process. Run this feature with a single API process. Restarting that process
loses pending login flows; start login again. Completed connections persist in
the database, and their selection is used when the PA next starts. Pending
switch work is not a durable cross-process queue.

## Validation boundary

Synthetic OAuth, credential-refresh, native HTTP, Responses Lite, configuration
and bootstrap tests cover the implementation. A real ChatGPT login followed by
a real Astra response has not yet been accepted end to end. Native steering and
asynchronous tool execution have not been accepted or enabled by this connection
work; a model name or successful login alone does not establish those features.
