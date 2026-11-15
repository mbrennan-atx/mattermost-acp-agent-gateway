# Project collaboration

The optional collaboration service lets independent assistants share project work, ask peers and return results to the requesting session. It stores the objective, membership, ownership, dependencies, decisions, artifact links and delivery receipts. Each gateway keeps its own native sessions, harness, model and connectors.

## Configure the service

Install the same reviewed gateway package used by the participating gateways. Keep service configuration, actor credentials and the database outside the source checkout. Use persistent storage for the database.

Create a private JSON configuration with an `actors` object. Each actor name has a `token_file` containing a distinct random credential of at least 32 characters. For addressed floor-thread requests, also set the actor's `user_id` and `mattermost_token_file`, and the top-level `mattermost_url`. The hub verifies the actual Mattermost post before routing it. Without those optional Mattermost settings, use `mm-work ask`.

Run:

```sh
mattermost-acp-collaboration --config /path/to/private/config.json --db /path/to/persistent/collaboration.db
```

The service listens on `127.0.0.1:18780`. A file lock prevents two hub processes from claiming the same database. Configure each participating gateway with:

```sh
MATTERMOST_COLLAB_URL=http://127.0.0.1:18780
MATTERMOST_COLLAB_TOKEN_FILE=/path/to/that/actors/private/token
```

The gateway's native shell inherits these settings, so `mm-work` uses the same actor credential. A gateway without `MATTERMOST_COLLAB_URL` retains its previous behavior and needs no collaboration service. Credentials never enter model prompts.

## Share a project

Create the project from the owner assistant's environment. Copy the originating thread and channel IDs from its channel context:

```sh
mm-work create --title 'Build investigation' --objective 'Find the failing dependency and verify a fix' --members assistant-a,assistant-b --origin-thread example-origin --origin-channel example-home --key example-project-key
```

The command creates and reads back a discussion root on the configured Agents floor. It returns the project ID and participant bindings. The owner receives results in its originating thread; other participants have independent native sessions in the floor discussion. Membership alone starts no model work.

Assign a request or make a task available for a participant to claim:

```sh
mm-work ask PROJECT_ID assistant-b --text 'Check which dependency fails' --key example-question-key
mm-work task PROJECT_ID --text 'Verify the proposed fix' --after REQUEST_ID --key example-task-key
mm-work claim TASK_ID
mm-work show PROJECT_ID
mm-work wait REQUEST_ID --seconds 30
```

Use a stable `--key` when retrying an operation after an uncertain transport response. The hub returns the existing request for the same payload and rejects a changed payload with that key. A wait lasts at most 45 seconds and returns the current state; reaching that limit does not cancel work.

Participants can record decisions and evidence without waking another model:

```sh
mm-work note PROJECT_ID --text 'The pinned dependency passes validation' --artifact https://example.com/change
```

For text with quotes, shell syntax or several paragraphs, use `--text-file /path/to/text.txt`. Follow the deployment's writing rules before transmitting shared prose.

## Busy sessions and results

The consumer tries the existing active-only steering capability first. An injected question belongs to the ongoing session and requires an explicit correlated reply:

```sh
mm-work reply REQUEST_ID --claim CLAIM_NONCE --text 'Verified result and evidence' --artifact https://example.com/proof
```

The request prompt includes its ID and claim nonce. Use `--state blocked` or `--state failed` when appropriate. The receiver sees the result status, parent request and evidence. A fresh collaboration turn saves its normal final answer automatically; ongoing native work remains pending until an explicit reply supplies its result.

Unsupported busy steering leaves the message queued. A timeout or unknown injection outcome leaves it `unconfirmed`, since the adapter may have accepted it. Neither case triggers a competing native prompt. Independent threads can continue, while preparation and execution for one native thread are serialized.

In a registered project discussion, a participant can address a request with `@assistant-b ASK: <specific question>`. The explicit `ASK:` marker distinguishes requests from normal answers and acknowledgements. The gateway and hub verify the post's author and discussion before routing it. Existing floor threads retain their previous wake policy.

## Working areas

Create an isolated worktree using the target repository's existing workflow before assigning implementation work. Read that repository's applicable AGENTS.md files first. Bind the worker's project thread to the worktree before its native session starts:

```sh
mm-work bind PROJECT_ID --thread PROJECT_DISCUSSION_ROOT --channel EXAMPLE_FLOOR --workspace /absolute/path/to/isolated/worktree
```

Bindings cannot change while that actor has active requests. An existing native session retains its original working directory; a mismatched workspace is rejected before work starts. Share artifact links and verified results through project state.

## Recovery and limits

Queued requests survive a hub restart. In-progress requests become `interrupted`; the service cannot prove which actions a crashed native process completed. Inspect the native conversation, files and external receipts, then reconcile before retrying:

```sh
mm-work retry REQUEST_ID --reconciliation 'Verified the previous attempt made no change'
```

Retry creates a new claim nonce, so an old worker cannot settle the new attempt. Reply submission is idempotent for the same result. Keep completed work and evidence when rolling back the executable; never restore an older database over newer state.

Default project limits are three simultaneous collaboration requests, 24 allocated requests, delegation depth two and one hour. Set other positive limits with the create command's `--max-workers`, `--max-turns`, `--max-depth` and `--max-seconds` options. Dependencies block dispatch until their requests finish successfully. Result and cancellation deliveries bypass the worker limit so a full project can still settle.

Request-count limits bound collaboration activation. They do not enforce an exact dollar budget or limit native subagents launched inside a harness. Provider cost coverage is currently unknown and is recorded that way in project state.

## Cancellation

```sh
mm-work cancel PROJECT_ID
```

Stopping the owner's originating thread also requests cancellation for its linked projects. Queued work cancels immediately. For active work, the consumer targets the recorded native session and repeats cancellation until it sees terminal evidence. If a newer session replaced it, cancellation is blocked rather than sent to that newer work.

A native result racing cancellation is retained as completed work. It does not establish that effects were prevented. The project remains `cancelling` while any execution lacks terminal evidence; the state makes that uncertainty visible. Cancellation of one project preserves unrelated sessions and projects.
