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

The hub persists creation intent and a stable creation ID before posting a discussion root on the configured Agents floor. The post metadata carries that ID and the creating actor. Concurrent retries share the same intent. A saved post ID survives read-back or project-persistence failure. When a reply addresses a visible root before its POST response is confirmed, the hub independently verifies the root, creator and metadata before accepting a matching root as a pending project discussion. Replies there stay blocked until creation is reconciled. Independently verified unrelated roots retain ordinary floor behavior.

An uncertain POST is never repeated automatically. Repeat the same key to reuse a root already recovered by independent verification. Otherwise inspect the floor and repeat the same key with `--discussion-thread EXISTING_ROOT`; reconciliation of an uncertain automatic post requires matching creation proof. A changed explicit root is rejected. Fresh explicit creation can still use an existing unmarked discussion. The command returns the project ID and participant bindings. The owner receives results in its originating thread; other participants have independent native sessions in the floor discussion. Membership alone starts no model work.

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

The consumer tries active-only steering when the loaded session has a recorded execution report and execution ID. Missing reports prevent steering. The existing busy-work guards keep the request queued or allow serialized normal preparation to create a new report. Every question asks for an explicit correlated reply, including work that continues in background tools or workers:

```sh
mm-work reply REQUEST_ID --claim CLAIM_NONCE --text 'Verified result and evidence' --artifact https://example.com/proof
```

Further delegation requires `mm-work ask PROJECT_ID RECIPIENT --parent REQUEST_ID --text 'Specific request'`. Without an explicit parent, the request is independent. Sharing an actor credential between a home gateway and its project sidecar does not connect their native work.

Explicit delegation uses a bounded DAG of participants. A child cannot address anyone already on its parent chain, including the original requester and its own actor. Unassigned delegated tasks enforce the same rule when claimed. Callback questions to an ancestor are not supported. Return a correlated blocked result that identifies the missing information:

```sh
mm-work reply CHILD_REQUEST_ID --claim CHILD_CLAIM --state blocked --text 'Need the parent to supply the commit hash before verification can continue'
```

The parent handles that result and supplies the information in a new child request with a new key. This returns control through the result path without leaving both participants waiting for each other.

The request prompt includes its ID and claim nonce. Use `--state blocked` or `--state failed` when appropriate. The receiver sees the result status, parent request and evidence. Every successful work request remains pending until its explicit correlated reply. An early reply retains native ownership while observed tools or workers remain active. A foreground end_turn is not proof that a native goal finished. The gateway records foreground output as evidence, with long text retained in a private runtime file. Result notifications preserve the peer result and artifact evidence independently of delivery retries.

Terminal evidence remains available when long-answer artifact storage fails. The receipt retains a bounded preview and a separate storage error while the complete answer still posts. A confirmed dispatch rejection releases only its provisional local ownership; settled historical evidence and genuinely unresolved earlier execution remain distinct. Mailbox consumption starts after the peer-floor join attempt. Failed workspace guards terminate their own process group, including children that survive an exited guard.

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

A correlated answer can arrive while its native execution remains pending. After a gateway restart, that old execution blocks new asks and tasks to the participant in that project. Child results remain queued when their parent is unresolved and the receiving gateway cannot verify continuation ownership. Restarting a gateway does not authorize a new native turn for those results.

Inspect an actor's retained claims with authenticated `GET /claims`. This read includes earlier gateway instances and does not change their leases or ownership. When a terminal request still has pending execution, the recorded recipient can settle it through `POST /messages/REQUEST_ID/update`, even after its old gateway instance has disappeared. Supply the stored `instance` and `claim`, `state: "execution_settled"`, the matching `expected_execution_id`, and verified native `evidence`: matching `session_id` and `execution_id`, `native_dispatched: true`, a terminal `stop_reason`, `pending: false`, and finite `completed_at` at or after the recorded dispatch time. Matching identity alone is insufficient; obtain the evidence from the native execution before submitting it. Verified settlement preserves the answer and makes new project requests admissible. It does not replay the old request.

Default project limits are three worker slots, 24 allocated requests, delegation depth two and one hour. A running parent may borrow one descendant slot when the worker limit is full. Child work must use a participant outside that explicit chain. Active sibling loans serialize. The hard total ceiling is workers multiplied by depth plus one, nine requests at defaults. Set other positive limits with the create command's `--max-workers`, `--max-turns`, `--max-depth` and `--max-seconds` options. Dependencies block dispatch until their requests finish successfully. Result and cancellation deliveries bypass the worker limit so a full project can still settle.

Request-count limits bound collaboration activation. They do not enforce an exact dollar budget or limit native subagents launched inside a harness. Provider cost coverage is currently unknown and is recorded that way in project state.

## Cancellation

```sh
mm-work cancel PROJECT_ID
```

Stopping the owner's originating thread in an upgraded home gateway also requests cancellation for its linked projects created by that stop time. Durable retries retain that boundary and preserve later projects. Creation intent retains its admission timestamp, so a stop during discussion posting or read-back also cancels that project when it finishes. The hub persists the latest origin stop boundary across restart. A legacy home gateway with separate project sidecars requires `mm-work cancel`; its `/stop` does not propagate. Queued work cancels immediately. In normal gateway mode, requests and result deliveries bound to the owner's original thread and channel stop routing without cancelling that conversation. An owner rebound to a separate worker thread receives the same native cancellation controls as other workers, including early results whose executions remain pending. For active worker work, the consumer targets the recorded native session and repeats cancellation until it sees terminal evidence. If a newer session or native turn replaced it, or the gateway cannot prove turn ownership after a restart, cancellation stays blocked to preserve that work. Reconcile its native outcome before proceeding.

A native result racing cancellation is retained as completed work. Conflicting repeated results are rejected. It does not establish that effects were prevented. Cancellation controls requeue after transport interruption. Session and execution guards are checked immediately before writing native controls, so session replacement cannot redirect a reserved cancellation or injection. The project remains `cancelling` while any execution lacks terminal evidence; the state makes that uncertainty visible. Cancellation of one project preserves unrelated sessions and projects.

## Gateway maintenance

Participating gateways expose a loopback-only `POST /maintenance` on their slash listener, authenticated with that gateway's collaboration actor token. `state` returns foreground, full response delivery, inbound, command, native RPC, background and collaboration counters. Unconfirmed native ownership remains counted until terminal evidence or explicit reconciliation settles it. `drain` pauses new model work while allowing stop commands. `seal` requires drained idle state and atomically blocks every new native control, including stop. `resume` clears both flags. Maintenance also reports `native_boundary_unknown`. A warmed adapter without authoritative native goal and queue idle evidence counts as pending background work and refuses sealing, retirement or refresh, even after foreground and correlated answers finish. Leave these clients running; normal collaboration remains available. Native process exit alone does not prove descendant work finished; warmed uncertainty remains. Automatic harness restarts also defer when that boundary is unknown or project work is pending. Cold sidecars can be retired before their first native task. Deployment tooling can seal admission before freezing and replacing a verified idle process; it must preserve the current native databases and provider configuration.

Native boundary uncertainty is committed to the gateway database before spawning an adapter, including a launch before its first thread mapping is saved. Existing thread history also preserves uncertainty across restarts and harness changes. A restarted consumer reads its actor's retained claims before mailbox consumption or maintenance admission; older claims count as collaboration debt without being reassigned or requeued. Failed or missing recovery evidence keeps maintenance blocked. Admission fences and their claim snapshots remain scoped to the current instance. Drain refreshes inherited debt after verified settlement. Settling an individual request does not clear the persistent warmed boundary: the gateway currently has no authoritative verifier for the complete native goal and queue lifecycle.

## Collaboration beside an existing gateway

A deployment can add a project worker beside an older home gateway when that gateway cannot prove a safe restart boundary. Configure the worker with `MATTERMOST_COLLAB_SIDECAR=1`, `MATTERMOST_COLLAB_ORIGIN_ACTOR=ACTOR`, a separate project database and loopback maintenance port. Keep its native harness, provider, model, effort and connector settings identical to the original. Set `sidecar: true` for that actor in the hub configuration. Startup verifies the actor and real Mattermost user before consuming work.

This worker handles registered floor requests and project mailboxes, and ignores ordinary home messages and slash commands. Every native project binding, including the owner's, stays in the project discussion; workspace selection cannot move it to a home root. Results for requests made by the original owner are posted and read back in the originating thread and returned by `mm-work wait` or `show`; the worker never loads the original owner's native session. Nested results return to the requesting project-native session. The originating agent supervises its delegated work through those commands. Existing home conversations keep running. Shared-work cancellation uses `mm-work cancel`; an older home gateway's `/stop` retains its original behavior and does not gain automatic project cancellation through this mode.

Mailbox claims remain counted until their responses are parsed and workers are registered. Drain fences new work at the hub for that actor and consumer instance, settles outstanding claims, and requeues only undispatched claims with no local owner. Seal also fences cancellation claims before its final idle check. Failed or ambiguous claim responses remain visible and prevent retirement until fenced reconciliation succeeds. Settled previous progress reports are discarded once no pending work or ownership needs them.

Inherited execution debt also fences independent peer results and ordinary native prompts in the same thread. Missing legacy thread bindings conservatively retain admission. Recovery arriving during native preparation refuses only that provisional turn. Cancellation can settle a retained, claim/execution/epoch-matched non-dispatch proof without cancelling its predecessor. Missing or stale proof remains unresolved.
