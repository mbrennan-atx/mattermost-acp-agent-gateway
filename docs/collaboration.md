# Project collaboration

The optional collaboration service lets independent assistants share project work, ask peers and return results to the requesting session. It stores the objective, membership, ownership, dependencies, decisions, artifact links and delivery receipts. Each gateway keeps its own native sessions, harness, model and connectors.

## Configure the service

Install the same reviewed gateway package used by the participating gateways. Keep service configuration, actor credentials and the database outside the source checkout. Use persistent storage for the database.

Create a private JSON configuration with an `actors` object. Each actor name has a `token_file` containing a distinct random credential of at least 32 characters. Automatic discussion creation and addressed floor requests also require the actor's `user_id` and `mattermost_token_file`, and the top-level `mattermost_url`. The hub verifies the actual Mattermost post before routing it. A token-only hub can use an existing root with `mm-work create --discussion-thread EXISTING_ROOT`, then `mm-work ask`.

Run:

```sh
mattermost-acp-collaboration --config /path/to/private/config.json --db /path/to/persistent/collaboration.db
```

The service listens on `127.0.0.1:18780`. A file lock prevents two hub processes from claiming the same database. Configure each participating gateway with:

```sh
MATTERMOST_COLLAB_URL=http://127.0.0.1:18780
MATTERMOST_COLLAB_TOKEN_FILE=/path/to/that/actors/private/token
```

The gateway's native shell inherits these settings, so `mm-work` uses the same actor credential. Request prompts invoke the CLI module through the gateway’s absolute Python interpreter, including virtual-environment and user-site installs. A gateway without `MATTERMOST_COLLAB_URL` retains its previous behavior and needs no collaboration service. Credentials never enter model prompts. Choose another free port with `--port` and use that port in each gateway's URL when the default is occupied.

## Share a project

Create the project from the owner assistant's environment. Copy the originating thread and channel IDs from its channel context:

```sh
mm-work create --title 'Build investigation' --objective 'Find the failing dependency and verify a fix' --members assistant-a,assistant-b --origin-thread example-origin --origin-channel example-home --key example-project-key
```

The hub persists creation intent before creating and reading back a discussion root on the configured Agents floor. Concurrent retries share the same intent. A saved post ID survives read-back or project-persistence failure. If posting itself is uncertain, creation stays unconfirmed instead of posting another root; inspect the floor and repeat the same key with `--discussion-thread EXISTING_ROOT` to reconcile. A changed explicit root is rejected. The command returns the project ID and participant bindings. The owner receives results in its originating thread; other participants have independent native sessions in the floor discussion. Membership alone starts no model work.

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

The consumer tries the existing active-only steering capability first. Every question asks for an explicit correlated reply, including work that continues in background tools or workers:

```sh
mm-work reply REQUEST_ID --claim CLAIM_NONCE --text 'Verified result and evidence' --artifact https://example.com/proof
```

Further delegation requires `mm-work ask PROJECT_ID RECIPIENT --parent REQUEST_ID --text 'Specific request'`. Without an explicit parent, the request is independent. Sharing an actor credential between a home gateway and its project sidecar does not connect their native work.

The request prompt includes its ID and claim nonce. Use `--state blocked` or `--state failed` when appropriate. The receiver sees the result status, parent request and evidence. Every successful work request remains pending until its explicit correlated reply. An early reply retains native ownership while observed tools or workers remain active. A foreground end_turn is not proof that a native goal finished. The gateway records foreground output as evidence, with long text retained in a private runtime file. Result notifications preserve the peer result and artifact evidence independently of delivery retries.

Unsupported busy steering leaves the message queued. A timeout or unknown injection outcome leaves it `unconfirmed`, since the adapter may have accepted it. Neither case triggers a competing native prompt. A correlated child result can continue its parent in the same native session after the parent foreground turn ends and its tracked tools and workers settle. Parent ownership transfers to the continuation; unrelated work stays queued. Late explicit replies after ambiguous delivery release ownership only after matching native terminal evidence recorded after dispatch. Independent threads can continue, while preparation and execution for one native thread are serialized.

Deployments can set `AGENT_WORKSPACE_GUARD_COMMAND` to one trusted executable path. Before native startup and session creation or resume, it receives the thread ID, canonical workspace and prior session ID (empty if there is no stored session). A failed check prevents native work. The private sidecar helper uses it to record selected workspace configuration before work and detect later changes without adopting missing evidence. The executable runs directly with literal arguments; it supplies no native goal or queue idle proof.

In a registered project discussion, a participant can address a request with `@assistant-b ASK: <specific question>`. The explicit `ASK:` marker distinguishes requests from normal answers and acknowledgements. The gateway and hub verify the post's author and discussion before routing it. Existing floor threads retain their previous wake policy.

## Working areas

Create an isolated worktree using the target repository's existing workflow before assigning implementation work. Read that repository's applicable AGENTS.md files first. Bind the worker's project thread to the worktree before its native session starts:

```sh
mm-work bind PROJECT_ID --thread PROJECT_DISCUSSION_ROOT --channel EXAMPLE_FLOOR --workspace /absolute/path/to/isolated/worktree
```

Bindings cannot change while that actor has active requests. Worker threads belong to one project until cancellation settles. Multiple projects may return results to the same owner's originating conversation. An existing native session retains its original working directory; a mismatched workspace is rejected before work starts. Share artifact links and verified results through project state.

## Recovery and limits

Queued requests survive a hub restart. Unconfirmed or interrupted work retains its worker slot and blocks new requests to that participant until reconciliation. In-progress requests become `interrupted`; the service cannot prove which actions a crashed native process completed. Inspect the native conversation, files and external receipts, then reconcile before retrying:

```sh
mm-work retry REQUEST_ID --reconciliation 'Verified the previous attempt made no change'
```

Retry retains old dispatch evidence as history and clears it for the new attempt. It creates a new claim nonce, so an old worker cannot settle the new attempt. Reply submission is idempotent for the same result. Keep completed work and evidence when rolling back the executable; never restore an older database over newer state.

Default project limits are three worker slots, 24 allocated requests, delegation depth two and one hour. A running parent may borrow one descendant slot when the worker limit is full, so a parent waiting for its peer does not deadlock. Active sibling loans serialize. The hard total ceiling is workers multiplied by depth plus one, nine requests at defaults. Set other positive limits with the create command's `--max-workers`, `--max-turns`, `--max-depth` and `--max-seconds` options. Dependencies block dispatch until their requests finish successfully. Result and cancellation deliveries bypass the worker limit so a full project can still settle.

Request-count limits bound collaboration activation. They do not enforce an exact dollar budget or limit native subagents launched inside a harness. Provider cost coverage is currently unknown and is recorded that way in project state.

## Cancellation

```sh
mm-work cancel PROJECT_ID
```

Stopping the owner's originating thread in an upgraded home gateway also requests cancellation for its linked projects created by that stop time. Durable retries retain that boundary and preserve later projects. Creation intent retains its admission timestamp, so a stop during discussion posting or read-back also cancels that project when it finishes. The hub persists the latest origin stop boundary across restart. A legacy home gateway with separate project sidecars requires `mm-work cancel`; its `/stop` does not propagate. Queued work cancels immediately. Result deliveries and requests addressed to the owner stop routing without cancelling the owner's ongoing conversation. For active worker work, the consumer targets the recorded native session and repeats cancellation until it sees terminal evidence. If a newer session or native turn replaced it, or the gateway cannot prove turn ownership after a restart, cancellation stays blocked to preserve that work. Reconcile its native outcome before proceeding.

A native result racing cancellation is retained as completed work. Conflicting repeated results are rejected. It does not establish that effects were prevented. Cancellation controls requeue after transport interruption. Session and execution guards are checked immediately before writing native controls, so session replacement cannot redirect a reserved cancellation or injection. The project remains `cancelling` while any execution lacks terminal evidence; the state makes that uncertainty visible. Cancellation of one project preserves unrelated sessions and projects.

## Gateway maintenance

Participating gateways expose a loopback-only `POST /maintenance` on their slash listener, authenticated with that gateway's collaboration actor token. `state` returns foreground, full response delivery, inbound, command, native RPC, background and collaboration counters. Unconfirmed native ownership remains counted until terminal evidence or explicit reconciliation settles it. `drain` pauses new model work while allowing stop commands. `seal` requires drained idle state and atomically blocks every new native control, including stop. `resume` clears both flags. Maintenance also reports `native_boundary_unknown`. A warmed adapter without authoritative native goal and queue idle evidence counts as pending background work and refuses sealing, retirement or refresh, even after foreground and correlated answers finish. Leave these clients running; normal collaboration remains available. Native process exit alone does not prove descendant work finished; warmed uncertainty remains. Automatic harness restarts also defer when that boundary is unknown or project work is pending. Cold sidecars can be retired before their first native task. Deployment tooling can seal admission before freezing and replacing a verified idle process; it must preserve the current native databases and provider configuration.

## Collaboration beside an existing gateway

A deployment can add a project worker beside an older home gateway when that gateway cannot prove a safe restart boundary. Configure the worker with `MATTERMOST_COLLAB_SIDECAR=1`, `MATTERMOST_COLLAB_ORIGIN_ACTOR=ACTOR`, a separate project database and loopback maintenance port. Keep its native harness, provider, model, effort and connector settings identical to the original. Set `sidecar: true` for that actor in the hub configuration. Startup verifies the actor and real Mattermost user before consuming work.

This worker handles registered floor requests and project mailboxes, and ignores ordinary home messages and slash commands. Every native project binding, including the owner's, stays in the project discussion; workspace selection cannot move it to a home root. Results for requests made by the original owner are posted and read back in the originating thread and returned by `mm-work wait` or `show`; the worker never loads the original owner's native session. Nested results return to the requesting project-native session. The originating agent supervises its delegated work through those commands. Existing home conversations keep running. Shared-work cancellation uses `mm-work cancel`; an older home gateway's `/stop` retains its original behavior and does not gain automatic project cancellation through this mode.

Mailbox claims remain counted until their responses are parsed and workers are registered. Drain fences new work at the hub for that actor and consumer instance, settles outstanding claims, and requeues only undispatched claims with no local owner. Seal also fences cancellation claims before its final idle check. Failed or ambiguous claim responses remain visible and prevent retirement until fenced reconciliation succeeds. Settled previous progress reports are discarded once no pending work or ownership needs them.
