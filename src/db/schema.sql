-- Runs at every start, so every statement stays IF NOT EXISTS.
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS miners (
  id INTEGER PRIMARY KEY,
  address TEXT NOT NULL UNIQUE,
  first_seen INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
  id INTEGER PRIMARY KEY CHECK(typeof(id)='integer' AND id BETWEEN 1 AND 9007199254740991),
  miner_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  first_seen INTEGER NOT NULL,
  last_seen INTEGER NOT NULL,
  accepted INTEGER NOT NULL DEFAULT 0,
  rejected INTEGER NOT NULL DEFAULT 0,
  stale INTEGER NOT NULL DEFAULT 0,
  UNIQUE (miner_id, name)
);

-- Accepted PPLNS shares in arrival order; the id is the share's sequence number.
CREATE TABLE IF NOT EXISTS shares (
  id INTEGER PRIMARY KEY,
  time INTEGER NOT NULL,
  miner_id INTEGER NOT NULL,
  worker_id INTEGER NOT NULL CHECK(typeof(worker_id)='integer' AND worker_id BETWEEN 1 AND 9007199254740991),
  weight INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS shares_time ON shares (time);

-- status: 0 found, 1 pending, 2 confirmed, 3 expired, 4 rejected
CREATE TABLE IF NOT EXISTS blocks (
  id INTEGER PRIMARY KEY,
  hash TEXT NOT NULL,
  mode INTEGER NOT NULL,
  status INTEGER NOT NULL,
  progress INTEGER NOT NULL DEFAULT 0 CHECK(progress IN (0,1,2)),
  miner_id INTEGER NOT NULL,
  worker_id INTEGER NOT NULL CHECK(typeof(worker_id)='integer' AND worker_id BETWEEN 1 AND 9007199254740991),
  height INTEGER NOT NULL,
  window_end INTEGER NOT NULL,
  found_at INTEGER NOT NULL,
  network_difficulty INTEGER NOT NULL,
  round_work INTEGER NOT NULL,
  fee_bps INTEGER NOT NULL,
  share_id INTEGER,
  window_work INTEGER,
  window_shares INTEGER,
  split BLOB,
  reward INTEGER,
  fee INTEGER,
  cut_height INTEGER,
  done_at INTEGER,
  reason TEXT
);
CREATE INDEX IF NOT EXISTS blocks_open ON blocks (status) WHERE status < 2;
CREATE INDEX IF NOT EXISTS blocks_miner ON blocks (miner_id, id);
CREATE INDEX IF NOT EXISTS blocks_mode ON blocks (mode, id);
CREATE INDEX IF NOT EXISTS blocks_found ON blocks (found_at);
CREATE INDEX IF NOT EXISTS blocks_split ON blocks (COALESCE(done_at, found_at)) WHERE split IS NOT NULL;

CREATE TABLE IF NOT EXISTS balances (
  miner_id INTEGER PRIMARY KEY,
  unpaid INTEGER NOT NULL DEFAULT 0 CHECK (typeof(unpaid)='integer' AND unpaid>=0),
  pending INTEGER NOT NULL DEFAULT 0 CHECK (typeof(pending)='integer' AND pending>=0),
  paid TEXT NOT NULL DEFAULT '0' CHECK (typeof(paid)='text'
    AND length(paid) BETWEEN 1 AND 39 AND paid NOT GLOB '*[^0-9]*'
    AND (paid='0' OR substr(paid,1,1)!='0')
    AND (length(paid)<39 OR paid<='340282366920938463463374607431768211455')),
  earned TEXT NOT NULL DEFAULT '0' CHECK (typeof(earned)='text'
    AND length(earned) BETWEEN 1 AND 39 AND earned NOT GLOB '*[^0-9]*'
    AND (earned='0' OR substr(earned,1,1)!='0')
    AND (length(earned)<39 OR earned<='340282366920938463463374607431768211455'))
);
CREATE INDEX IF NOT EXISTS balances_unpaid ON balances (unpaid) WHERE unpaid > 0;

CREATE TABLE IF NOT EXISTS earnings (
  miner_id INTEGER NOT NULL,
  day INTEGER NOT NULL,
  amount TEXT NOT NULL DEFAULT '0' CHECK (typeof(amount)='text'
    AND length(amount) BETWEEN 1 AND 39 AND amount NOT GLOB '*[^0-9]*'
    AND (amount='0' OR substr(amount,1,1)!='0')
    AND (length(amount)<39 OR amount<='340282366920938463463374607431768211455')),
  blocks INTEGER NOT NULL CHECK (typeof(blocks)='integer' AND blocks>=0),
  PRIMARY KEY (miner_id, day)
) WITHOUT ROWID;

-- status: 0 prepared, 1 sent, 2 paid, 3 failed
CREATE TABLE IF NOT EXISTS payouts (
  id INTEGER PRIMARY KEY,
  request_id TEXT NOT NULL UNIQUE,
  status INTEGER NOT NULL,
  created_at INTEGER NOT NULL,
  sent_at INTEGER,
  done_at INTEGER,
  total INTEGER NOT NULL,
  fee INTEGER,
  recipients INTEGER NOT NULL,
  transaction_id TEXT,
  valid_until INTEGER,
  error TEXT
);
CREATE INDEX IF NOT EXISTS payouts_open ON payouts (status) WHERE status < 2;
CREATE INDEX IF NOT EXISTS payouts_open_id ON payouts (id) WHERE status < 2;

-- Only newly created payouts get an unattempted marker. Older ambiguous rows stay unbound.
CREATE TABLE IF NOT EXISTS payout_attempts (
  payout_id INTEGER PRIMARY KEY,
  attempted INTEGER NOT NULL DEFAULT 0 CHECK (attempted IN (0,1)),
  archive_id TEXT,
  account INTEGER,
  chain_id TEXT,
  CHECK ((attempted=0 AND archive_id IS NULL AND account IS NULL AND chain_id IS NULL)
    OR (attempted=1 AND archive_id IS NOT NULL AND account IS NOT NULL AND chain_id IS NOT NULL
      AND length(archive_id)=64 AND archive_id NOT GLOB '*[^0-9a-f]*'
      AND account BETWEEN 0 AND 4294967295
      AND length(chain_id)=96 AND chain_id NOT GLOB '*[^0-9a-f]*'))
);

CREATE TABLE IF NOT EXISTS payout_items (
  payout_id INTEGER NOT NULL,
  miner_id INTEGER NOT NULL,
  amount INTEGER NOT NULL,
  fee_share INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (payout_id, miner_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS payout_items_miner ON payout_items (miner_id, payout_id);

-- Share work per ten minutes; miner_id 0 holds the pool totals.
CREATE TABLE IF NOT EXISTS hashrate (
  miner_id INTEGER NOT NULL,
  bucket INTEGER NOT NULL,
  mode INTEGER NOT NULL,
  work INTEGER NOT NULL,
  PRIMARY KEY (miner_id, bucket, mode)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS hashrate_bucket ON hashrate (bucket);

-- Canonical proof fingerprints exclude payout identity and mining mode. Sequences never reuse
-- old accepted-work ids when their rolling PPLNS rows are pruned.
CREATE TABLE IF NOT EXISTS accepted_work (
  job_id BLOB NOT NULL CHECK(typeof(job_id)='blob' AND length(job_id)=48),
    nonce BLOB NOT NULL CHECK(typeof(nonce)='blob' AND length(nonce)=32),
    fingerprint BLOB PRIMARY KEY CHECK(typeof(fingerprint)='blob' AND length(fingerprint)=48),
  id INTEGER NOT NULL UNIQUE CHECK(typeof(id)='integer' AND id>0),
  time INTEGER NOT NULL CHECK(typeof(time)='integer' AND time>=0),
  miner_id INTEGER NOT NULL CHECK(miner_id>0),
  worker_id INTEGER NOT NULL CHECK(typeof(worker_id)='integer' AND worker_id BETWEEN 1 AND 9007199254740991),
  mode INTEGER NOT NULL CHECK(mode IN (0,1)),
  weight INTEGER NOT NULL CHECK(typeof(weight)='integer' AND weight>0),
  window_end INTEGER NOT NULL CHECK(typeof(window_end)='integer' AND window_end>0),
  block_id INTEGER
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS accepted_work_retirement ON accepted_work(time,window_end);
CREATE TABLE IF NOT EXISTS accepted_participants (
  miner_id INTEGER PRIMARY KEY CHECK(miner_id>0),
  last_seen INTEGER NOT NULL CHECK(typeof(last_seen)='integer' AND last_seen>=0)
);
CREATE INDEX IF NOT EXISTS accepted_participants_seen ON accepted_participants(last_seen);
-- A committed found block remains submit-able after a queue fills or the process restarts.
CREATE TABLE IF NOT EXISTS found_payload (
  block_id INTEGER PRIMARY KEY,
  retry_height INTEGER NOT NULL DEFAULT 0 CHECK(typeof(retry_height)='integer' AND retry_height>=0),
  retry_at INTEGER NOT NULL DEFAULT 0 CHECK(typeof(retry_at)='integer' AND retry_at>=0),
  header BLOB NOT NULL CHECK(typeof(header)='blob' AND length(header)=312),
  body_hex TEXT NOT NULL CHECK(typeof(body_hex)='text' AND length(body_hex)<=94208 AND length(body_hex)%2=0)
);
CREATE UNIQUE INDEX IF NOT EXISTS blocks_canonical_hash ON blocks(hash);

CREATE TABLE IF NOT EXISTS share_sources (
  miner_id INTEGER NOT NULL, source BLOB NOT NULL, last_seen INTEGER NOT NULL,
  PRIMARY KEY(miner_id,source)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS share_sources_seen ON share_sources(last_seen);

CREATE UNIQUE INDEX IF NOT EXISTS accepted_work_replay ON accepted_work(job_id,nonce);
CREATE INDEX IF NOT EXISTS blocks_open_id ON blocks(id) WHERE status < 2;
CREATE INDEX IF NOT EXISTS found_payload_due ON found_payload(block_id);

CREATE INDEX IF NOT EXISTS accepted_work_block ON accepted_work(block_id) WHERE block_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS blocks_submission ON blocks(window_end,id) WHERE status < 2;

CREATE INDEX IF NOT EXISTS found_payload_retry ON found_payload(retry_at,block_id);


-- Local worker aliases are disposable only after every monetary reference has retired.
CREATE TABLE IF NOT EXISTS worker_catalogue (
  id INTEGER PRIMARY KEY CHECK(id=1),
  live_count INTEGER NOT NULL CHECK(typeof(live_count)='integer' AND live_count BETWEEN 0 AND 262144),
  last_id INTEGER NOT NULL CHECK(typeof(last_id)='integer' AND last_id BETWEEN 0 AND 9007199254740991),
  gc_after INTEGER NOT NULL CHECK(typeof(gc_after)='integer' AND gc_after BETWEEN 0 AND last_id),
  gc_through INTEGER NOT NULL CHECK(typeof(gc_through)='integer' AND gc_through BETWEEN gc_after AND last_id)
);
INSERT OR IGNORE INTO worker_catalogue VALUES(1,0,0,0,0);
CREATE INDEX IF NOT EXISTS shares_worker ON shares(worker_id);
CREATE INDEX IF NOT EXISTS accepted_work_worker ON accepted_work(worker_id);
CREATE INDEX IF NOT EXISTS blocks_worker ON blocks(worker_id);
