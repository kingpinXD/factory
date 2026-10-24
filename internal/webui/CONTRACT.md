# Local browser contract

Same origin, instance `local` only. All JSON keys snake_case. Errors are `{error: string}`.
POST `/session` with `{token}` exchanges the one-use URL fragment for a cookie and returns `{csrf}`.
GET `/session` with the cookie returns `{csrf}` for browser reloads. The fragment is removed before fetching.
Every API request requires the cookie. Every POST requires the exact Origin and `X-CSRF-Token` header.
No credentials or private content belong in browser storage. A request id alone can use sessionStorage.

Base `/api/v1/instances/local`:
- GET `/overview`, `/jobs`, `/jobs/{id}` use reconcile projection DTOs. Jobs returns `{jobs,next_offset,total}`; query `offset`/`limit` bounds 0+/1..100, `state=active|completed`, `q` search.
- GET `/entities/{id}/events?after_seq=0&limit=100`, `/entities/{id}/inspection`, `/entities/{id}/artifacts/{artifact_id}` use projection DTOs.
- GET `/health` returns `{instance_id,brain,executable,version,read_only,answer_supported,prerequisites:[{name,available,detail}]}`. Prerequisites are passive local observations, never login probes.
- GET `/entities/{id}/cancel-preview` returns `{target_id,expected,confirmation,descendants:[{id,state,pr,adopted,preserve_pr}],message}`. Send its exact confirmation and expected back.
- GET `/logs/tick` or `/logs/repo-worker` returns `{source,text,truncated,source_at}`. Fixed instance log root; no browser path.
- POST `/uploads` takes `{name,text}`, bounded UTF-8 text, returns `{upload_id,sha256,name}`. Uploaded plan id is opaque and durable.
- POST `/jobs` takes `{request_id,input:{kind,value,upload_id}}`, kinds `text`, `issue`, `epic`, `plan_upload`.
- POST `/actions` takes `{request_id,type,target_id,expected,issue,value,confirmation}`. Types `answer`, `retry`, `replan`, `message`, `deployed`, `review_asked`, `cancel`. `value` is answer/note/message/reason or PR URL for deployed/review_asked. Answer issue identifies the current question's issue; target may be its epic or item.
- POST writes return a receipt. GET `/actions/{request_id}` returns the same durable receipt.

Receipt: `{request_id,state,entity_id,events:[{entity_id,seq}],message,updated_at}`. States `pending`, `queued`, `applied`, `refused`, `failed`, `outcome_unknown`. A reused id with different canonical body returns 409. Incomplete intents after server restart become outcome_unknown and are never replayed.

The backend redacts all serialized strings, including default, private debug, errors, logs, receipt messages and exported evidence. Browser renders text only. A queued action is not completed. New bound answers require a fresh completed consuming-manager tick with `ui_action_protocol=1`; an older installed manager cannot safely consume them. Stop UI mutations before downgrading the manager.
