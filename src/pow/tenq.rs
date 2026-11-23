pub const POW_PREIMAGE_BYTES: usize = 323;
pub const OFF_CHAIN: usize = 11;
pub const OFF_ALGO: usize = 61;
pub const OFF_WORKREF: usize = 62;
pub const OFF_NONCE: usize = 259;
pub const OFF_EXTRANONCE: usize = 291;
pub const ALGORITHM_ID: u8 = 0x03;

pub const SPEC_REVISION: u32 = 9;

pub const LANES: usize = 32;
pub const REGS: usize = 16;
pub const NODE_WORDS: usize = 32;
pub const ITEM_NODES: usize = 8;
pub const ITEM_WORDS: usize = NODE_WORDS * ITEM_NODES;
pub const WORDS_PER_LANE: usize = ITEM_WORDS / LANES;
pub const SCRATCH_WORDS: usize = 1024;
pub const SCRATCH_LANE_WORDS: u32 = 32;
pub const TEMPLATES: usize = 16;
pub const TEMPLATE_OPS: usize = 12;
pub const EPOCH_HEIGHTS: u64 = 34560;

pub const PROFILE_TEST: u32 = 0x5445_4E01;
pub const PROFILE_BENCH: u32 = 0x5445_4E02;
pub const PROFILE_PROD2G: u32 = 0x5445_4E10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Profile {
    pub id: u32,
    pub cache_log2_bytes: u32,
    pub cache_mix_passes: u32,
    pub node_reads: u32,
    pub dataset_log2_bytes: u32,
    pub rounds: u32,
}

pub fn profile(id: u32) -> Option<Profile> {
    match id {
        PROFILE_TEST => Some(Profile {
            id,
            cache_log2_bytes: 20,
            cache_mix_passes: 1,
            node_reads: 32,
            dataset_log2_bytes: 22,
            rounds: 64,
        }),
        PROFILE_BENCH => Some(Profile {
            id,
            cache_log2_bytes: 24,
            cache_mix_passes: 1,
            node_reads: 32,
            dataset_log2_bytes: 26,
            rounds: 256,
        }),
        PROFILE_PROD2G => Some(Profile {
            id,
            cache_log2_bytes: 28,
            cache_mix_passes: 1,
            node_reads: 352,
            dataset_log2_bytes: 31,
            rounds: 128,
        }),
        _ => None,
    }
}

impl Profile {
    pub const fn cache_words(&self) -> usize {
        (1usize << self.cache_log2_bytes) / 4
    }

    pub const fn dataset_words(&self) -> usize {
        (1usize << self.dataset_log2_bytes) / 4
    }

    /// Mask of the 1 KiB dataset items a round picks from.
    pub const fn item_mask(&self) -> u32 {
        ((1usize << self.dataset_log2_bytes) / (ITEM_WORDS * 4) - 1) as u32
    }
}

pub fn cache_words(id: u32) -> Option<usize> {
    profile(id).map(|p| p.cache_words())
}
pub fn dataset_words(id: u32) -> Option<usize> {
    profile(id).map(|p| p.dataset_words())
}
pub fn epoch_of_anchor_height(h: u64) -> u32 {
    (h / EPOCH_HEIGHTS) as u32
}
pub fn epoch_of_anchor_height_checked(h: u64) -> Option<u32> {
    u32::try_from(h / EPOCH_HEIGHTS).ok()
}

#[inline]
fn rotl32(x: u32, n: u32) -> u32 {
    x.rotate_left(n & 31)
}
#[inline]
fn rotl64(x: u64, n: u32) -> u64 {
    x.rotate_left(n & 63)
}
#[inline]
fn mulhi32(a: u32, b: u32) -> u32 {
    (((a as u64) * (b as u64)) >> 32) as u32
}
#[inline]
pub(crate) fn mix32(mut x: u32) -> u32 {
    x ^= x >> 16;
    x = x.wrapping_mul(0x7feb_352d);
    x ^= x >> 15;
    x = x.wrapping_mul(0x846c_a68b);
    x ^ (x >> 16)
}
#[inline]
pub(crate) fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}
#[inline]
fn fold64(v: u64) -> u32 {
    (v as u32) ^ ((v >> 32) as u32)
}
#[inline]
fn le32(p: &[u8]) -> u32 {
    u32::from_le_bytes([p[0], p[1], p[2], p[3]])
}
#[inline]
fn le64(p: &[u8]) -> u64 {
    u64::from_le_bytes([p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]])
}

pub(crate) fn keccakf(a: &mut [u64; 25]) {
    const RC: [u64; 24] = [
        0x0000_0000_0000_0001,
        0x0000_0000_0000_8082,
        0x8000_0000_0000_808a,
        0x8000_0000_8000_8000,
        0x0000_0000_0000_808b,
        0x0000_0000_8000_0001,
        0x8000_0000_8000_8081,
        0x8000_0000_0000_8009,
        0x0000_0000_0000_008a,
        0x0000_0000_0000_0088,
        0x0000_0000_8000_8009,
        0x0000_0000_8000_000a,
        0x0000_0000_8000_808b,
        0x8000_0000_0000_008b,
        0x8000_0000_0000_8089,
        0x8000_0000_0000_8003,
        0x8000_0000_0000_8002,
        0x8000_0000_0000_0080,
        0x0000_0000_0000_800a,
        0x8000_0000_8000_000a,
        0x8000_0000_8000_8081,
        0x8000_0000_0000_8080,
        0x0000_0000_8000_0001,
        0x8000_0000_8000_8008,
    ];
    // rho offsets along the pi walk that starts at lane 1
    const RHO: [u32; 24] = [
        1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14, 27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44,
    ];
    const PI: [usize; 24] = [
        10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4, 15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1,
    ];
    for &rc in RC.iter() {
        let mut c = [0u64; 5];
        for x in 0..5 {
            c[x] = a[x] ^ a[x + 5] ^ a[x + 10] ^ a[x + 15] ^ a[x + 20];
        }
        for x in 0..5 {
            let d = c[(x + 4) % 5] ^ rotl64(c[(x + 1) % 5], 1);
            for y in 0..5 {
                a[5 * y + x] ^= d;
            }
        }
        let mut last = a[1];
        for k in 0..24 {
            let next = a[PI[k]];
            a[PI[k]] = rotl64(last, RHO[k]);
            last = next;
        }
        for y in 0..5 {
            let row = [
                a[5 * y],
                a[5 * y + 1],
                a[5 * y + 2],
                a[5 * y + 3],
                a[5 * y + 4],
            ];
            for x in 0..5 {
                a[5 * y + x] = row[x] ^ ((!row[(x + 1) % 5]) & row[(x + 2) % 5]);
            }
        }
        a[0] ^= rc;
    }
}

#[derive(Clone)]
pub(crate) struct Xof {
    a: [u64; 25],
    p: usize,
    finalized: bool,
    suffix: u64,
}

fn left_encode(x: u64) -> Vec<u8> {
    let bytes = x.to_be_bytes();
    let skip = bytes.iter().take_while(|&&b| b == 0).count().min(7);
    let mut out = vec![(8 - skip) as u8];
    out.extend_from_slice(&bytes[skip..]);
    out
}

impl Xof {
    /// SHAKE256 with a tag in front of the data.
    pub(crate) fn new(tag: &[u8]) -> Self {
        let mut s = Xof {
            a: [0; 25],
            p: 0,
            finalized: false,
            suffix: 0x1f,
        };
        s.absorb(tag);
        s
    }
    /// cSHAKE256 of NIST SP 800-185 with function name `n` and customization `s`.
    pub(crate) fn cshake(n: &[u8], s: &[u8]) -> Self {
        let mut x = Xof {
            a: [0; 25],
            p: 0,
            finalized: false,
            suffix: 0x04,
        };
        x.absorb(&left_encode(136));
        x.absorb(&left_encode(8 * n.len() as u64));
        x.absorb(n);
        x.absorb(&left_encode(8 * s.len() as u64));
        x.absorb(s);
        while x.p != 0 {
            x.byte(0);
        }
        x
    }
    fn byte(&mut self, b: u8) {
        self.a[self.p >> 3] ^= (b as u64) << (8 * (self.p & 7));
        self.p += 1;
        if self.p == 136 {
            keccakf(&mut self.a);
            self.p = 0;
        }
    }
    pub(crate) fn absorb(&mut self, p: &[u8]) {
        for &b in p {
            self.byte(b)
        }
    }
    fn u32v(&mut self, x: u32) {
        self.absorb(&x.to_le_bytes())
    }
    fn finalize(&mut self) {
        if !self.finalized {
            self.a[self.p >> 3] ^= self.suffix << (8 * (self.p & 7));
            self.a[16] ^= 0x8000_0000_0000_0000;
            keccakf(&mut self.a);
            self.p = 0;
            self.finalized = true;
        }
    }
    fn squeeze_byte(&mut self) -> u8 {
        self.finalize();
        if self.p == 136 {
            keccakf(&mut self.a);
            self.p = 0;
        }
        let b = (self.a[self.p >> 3] >> (8 * (self.p & 7))) as u8;
        self.p += 1;
        b
    }
    fn squeeze_u32(&mut self) -> u32 {
        let mut b = [0u8; 4];
        for x in b.iter_mut() {
            *x = self.squeeze_byte();
        }
        u32::from_le_bytes(b)
    }
    pub(crate) fn squeeze<const N: usize>(&mut self) -> [u8; N] {
        let mut o = [0u8; N];
        for b in o.iter_mut() {
            *b = self.squeeze_byte();
        }
        o
    }
}

fn absorb_words(s: &mut Xof, w: &[u32]) {
    for &x in w {
        s.u32v(x)
    }
}

const ATTEMPT_TAG: &[u8] = b"TENRAI/TENQ/ATTEMPT";
const LANE_TAG: &[u8] = b"TENRAI/TENQ/LANE";
const FINAL_TAG: &[u8] = b"TENRAI/TENQ/FINAL";
static LANE_PREFIX: std::sync::OnceLock<Xof> = std::sync::OnceLock::new();
static FINAL_PREFIX: std::sync::OnceLock<Xof> = std::sync::OnceLock::new();

/// Keccak states after cSHAKE256's constant first block: lane digests, then the final value.
pub fn cshake_prefix_states() -> [[u64; 25]; 2] {
    [Xof::cshake(b"", LANE_TAG).a, Xof::cshake(b"", FINAL_TAG).a]
}

/// The attempt seed's SHAKE256 state after two blocks, and the padded third block. GPUs XOR the
/// counter into bytes 6..14 of it.
pub fn attempt_midstate(pre: &[u8; POW_PREIMAGE_BYTES]) -> ([u64; 25], [u64; 17]) {
    let mut stream = [0u8; 342];
    stream[..ATTEMPT_TAG.len()].copy_from_slice(ATTEMPT_TAG);
    stream[ATTEMPT_TAG.len()..].copy_from_slice(pre);
    let mut mid = [0u64; 25];
    for block in stream[..272].chunks(136) {
        for (i, &b) in block.iter().enumerate() {
            mid[i >> 3] ^= (b as u64) << (8 * (i & 7));
        }
        keccakf(&mut mid);
    }
    let mut last = [0u64; 17];
    for (i, &b) in stream[272..].iter().enumerate() {
        last[i >> 3] ^= (b as u64) << (8 * (i & 7));
    }
    last[70 >> 3] ^= 0x1f << (8 * (70 & 7));
    last[16] ^= 0x8000_0000_0000_0000;
    (mid, last)
}

/// cSHAKE256 of one lane's number and its 32 scratchpad words: one permutation after the prefix block.
fn lane_digest(lane: u32, words: &[u32]) -> [u8; 32] {
    let mut s = LANE_PREFIX
        .get_or_init(|| Xof::cshake(b"", LANE_TAG))
        .clone();
    s.u32v(lane);
    absorb_words(&mut s, words);
    s.squeeze::<32>()
}

/// SHAKE256 of the words, for vector files and dataset checks.
pub fn words_digest(words: &[u32]) -> [u8; 32] {
    let mut s = Xof::new(b"TENRAI/TENQ/DIGEST");
    absorb_words(&mut s, words);
    s.squeeze::<32>()
}

pub(crate) fn compress32(inw: &[u32; 32], d0: u64, d1: u64, out: &mut [u32; 32]) {
    let mut a = [0u64; 25];
    for i in 0..16 {
        a[i] = (inw[2 * i] as u64) | ((inw[2 * i + 1] as u64) << 32);
    }
    a[16] = d0;
    a[17] = d1;
    for i in 18..25 {
        a[i] = 0x9e37_79b9_7f4a_7c15u64.wrapping_mul((i + 1) as u64);
    }
    keccakf(&mut a);
    for i in 0..16 {
        out[2 * i] = a[i] as u32;
        out[2 * i + 1] = (a[i] >> 32) as u32;
    }
}

pub fn epoch_seed(profile_id: u32, chain: &[u8; 48], epoch: u32) -> Option<[u8; 64]> {
    profile(profile_id)?;
    let mut s = Xof::new(b"TENRAI/TENQ/EPOCH");
    s.absorb(chain);
    s.u32v(profile_id);
    s.u32v(SPEC_REVISION);
    s.u32v(epoch);
    Some(s.squeeze::<64>())
}

pub(crate) fn zeroed_words(words: usize) -> Option<Vec<u32>> {
    let mut buffer = Vec::new();
    buffer.try_reserve_exact(words).ok()?;
    buffer.resize(words, 0);
    Some(buffer)
}

/// Sequential cache: row i mixes row i-1 with two earlier rows picked by row i-1.
pub fn make_cache(profile_id: u32, chain: &[u8; 48], epoch: u32) -> Option<Vec<u32>> {
    let p = profile(profile_id)?;
    let eseed = epoch_seed(profile_id, chain, epoch)?;
    let rows = (1usize << p.cache_log2_bytes) / 128;
    let mut c = zeroed_words(rows.checked_mul(NODE_WORDS)?)?;
    let mut s = Xof::new(b"TENRAI/TENQ/CACHE");
    s.absorb(&eseed);
    s.u32v(profile_id);
    for w in c[..NODE_WORDS].iter_mut() {
        *w = s.squeeze_u32();
    }
    for pass in 0..p.cache_mix_passes {
        for i in (if pass == 0 { 1 } else { 0 })..rows {
            let prev = if i == 0 { rows - 1 } else { i - 1 };
            let span = if pass == 0 { i } else { rows } as u64;
            let j1 = ((u64::from(c[prev * 32]) * span) >> 32) as usize;
            let j2 = ((u64::from(c[prev * 32 + 1]) * span) >> 32) as usize;
            let salt = (i as u32).wrapping_mul(0x85eb_ca6b) ^ pass.wrapping_mul(0x9e37_79b9);
            for w in 0..NODE_WORDS {
                let a = c[prev * 32 + w];
                let b = c[j1 * 32 + w];
                let d = c[j2 * 32 + ((w + 13) & 31)];
                let old = if pass == 0 { 0 } else { c[i * 32 + w] };
                c[i * 32 + w] =
                    (mix32(a.wrapping_add(b) ^ salt ^ w as u32) ^ rotl32(d, w as u32)) ^ old;
            }
        }
    }
    Some(c)
}

/// Start of one of the node's two chains; each chain reads half of the node's rows. The link holds
/// the next row in its low bits and the block rotation in its top three bits.
fn node_start(eseed: &[u8; 64], index: u32, chain: usize) -> ([u32; 32], u32) {
    let sa = le64(&eseed[0..8]);
    let sb = le64(&eseed[8..16]);
    let (key, salt) = if chain == 0 {
        (sa, sb as u32)
    } else {
        (sb, (sa as u32) ^ 0x6a09_e667)
    };
    let mut acc = [0u32; 32];
    let mut x = key ^ (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    for w in 0..32 {
        x = splitmix64(x.wrapping_add(w as u64));
        acc[w] = fold64(x);
    }
    (acc, mix32(index ^ salt))
}

/// Asks for a row early; only speed, never values.
#[inline(always)]
fn prefetch_row(cache: &[u32], j: u32) {
    #[cfg(target_arch = "x86_64")]
    {
        use core::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
        let row = &cache[j as usize * 32..j as usize * 32 + 32];
        // SAFETY: both addresses lie inside `row`; a prefetch never faults.
        unsafe {
            _mm_prefetch::<_MM_HINT_T0>(row.as_ptr().cast::<i8>());
            _mm_prefetch::<_MM_HINT_T0>(row.as_ptr().add(16).cast::<i8>());
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = (cache, j);
}

#[inline(always)]
fn cache_row(cache: &[u32], j: u32) -> &[u32; 32] {
    let base = (j as usize) * 32;
    cache[base..base + 32].try_into().unwrap()
}

/// One read step: add the row with its 16-byte blocks shifted by the link's top bits, then rotate.
/// The shift forces a word-sliced SRAM to move whole rows.
#[inline(always)]
fn node_step(acc: &mut [u32; 32], row: &[u32; 32], t: u32, link: u32) -> u32 {
    let rot = 1 + (t.wrapping_mul(7) % 31);
    let shift = (link >> 29) as usize;
    for v in 0..8 {
        let src = 4 * ((v + shift) & 7);
        for i in 0..4 {
            acc[4 * v + i] = acc[4 * v + i].wrapping_add(row[src + i]).rotate_left(rot);
        }
    }
    mix32(acc[(t & 31) as usize] ^ acc[((t * 5 + 3) & 31) as usize] ^ t.wrapping_mul(0x85eb_ca6b))
}

fn node_finish(acc: &[[u32; 32]; 2], eseed: &[u8; 64], index: u32, out: &mut [u32; 32]) {
    let sa = le64(&eseed[0..8]);
    let sb = le64(&eseed[8..16]);
    let mut both = [0u32; 32];
    for w in 0..32 {
        both[w] = acc[0][w] ^ acc[1][w].rotate_left(16);
    }
    compress32(
        &both,
        sa ^ (index as u64),
        sb.wrapping_add(index as u64),
        out,
    );
}

fn cache_mask(cache: &[u32]) -> u32 {
    assert!(
        cache.len() >= 32 && cache.len() % 32 == 0 && (cache.len() / 32).is_power_of_two(),
        "invalid cache length"
    );
    ((cache.len() / 32) - 1) as u32
}

pub(crate) fn dataset_node_reads(
    cache: &[u32],
    eseed: &[u8; 64],
    index: u32,
    reads: u32,
    out: &mut [u32; 32],
) {
    let mask = cache_mask(cache);
    let mut acc = [[0u32; 32]; 2];
    for (chain, a) in acc.iter_mut().enumerate() {
        let (start, mut link) = node_start(eseed, index, chain);
        *a = start;
        for t in 0..reads / 2 {
            link = node_step(a, cache_row(cache, link & mask), t, link);
        }
    }
    node_finish(&acc, eseed, index, out);
}

/// All 16 chains of an item's eight nodes in lockstep, so their cache misses overlap.
pub(crate) fn item_reads(
    cache: &[u32],
    eseed: &[u8; 64],
    item: u32,
    reads: u32,
    out: &mut [u32; ITEM_WORDS],
) {
    let mask = cache_mask(cache);
    let mut acc = [[[0u32; 32]; 2]; ITEM_NODES];
    let mut link = [[0u32; 2]; ITEM_NODES];
    for k in 0..ITEM_NODES {
        for c in 0..2 {
            (acc[k][c], link[k][c]) = node_start(eseed, item * ITEM_NODES as u32 + k as u32, c);
            prefetch_row(cache, link[k][c] & mask);
        }
    }
    for t in 0..reads / 2 {
        for k in 0..ITEM_NODES {
            for c in 0..2 {
                let l = link[k][c];
                link[k][c] = node_step(&mut acc[k][c], cache_row(cache, l & mask), t, l);
                prefetch_row(cache, link[k][c] & mask);
            }
        }
    }
    let mut node = [0u32; 32];
    for k in 0..ITEM_NODES {
        node_finish(
            &acc[k],
            eseed,
            item * ITEM_NODES as u32 + k as u32,
            &mut node,
        );
        out[k * NODE_WORDS..(k + 1) * NODE_WORDS].copy_from_slice(&node);
    }
}

/// Dataset node `index` from the cache; `cache` must be the profile's.
pub fn dataset_node(p: &Profile, cache: &[u32], eseed: &[u8; 64], index: u32, out: &mut [u32; 32]) {
    dataset_node_reads(cache, eseed, index, p.node_reads, out);
}

/// The eight nodes of item `item`, for about the cost of one.
pub fn dataset_item(
    p: &Profile,
    cache: &[u32],
    eseed: &[u8; 64],
    item: u32,
    out: &mut [u32; ITEM_WORDS],
) {
    item_reads(cache, eseed, item, p.node_reads, out);
}

pub fn dataset_node_checked(
    profile_id: u32,
    cache: &[u32],
    eseed: &[u8; 64],
    index: u32,
) -> Option<[u32; 32]> {
    let p = profile(profile_id)?;
    if cache.len() != p.cache_words() || index as usize >= p.dataset_words() / NODE_WORDS {
        return None;
    }
    let mut out = [0u32; 32];
    dataset_node(&p, cache, eseed, index, &mut out);
    Some(out)
}

pub fn make_dataset(profile_id: u32, cache: &[u32], eseed: &[u8; 64]) -> Option<Vec<u32>> {
    let p = profile(profile_id)?;
    if cache.len() != p.cache_words() {
        return None;
    }
    let mut d = zeroed_words(p.dataset_words())?;
    let (items, _) = d.as_chunks_mut::<ITEM_WORDS>();
    for (i, item) in items.iter_mut().enumerate() {
        dataset_item(&p, cache, eseed, i as u32, item);
    }
    Some(d)
}

include!("tenq_templates.rs");

#[inline(always)]
fn contrib(r: &[u32; REGS], lane: u32) -> u32 {
    mix32(r[12] ^ rotl32(r[13], lane) ^ r[14] ^ (lane + 1).wrapping_mul(0x9e37_79b9))
}

fn preimage_valid(p: &[u8; 323]) -> bool {
    &p[0..11] == b"TRAI_POW_V1" && p[OFF_ALGO] == ALGORITHM_ID
}

pub(crate) struct State {
    pub(crate) seed: [u8; 64],
    pub(crate) r: [[u32; REGS]; LANES],
    pub(crate) scratch: Vec<u32>,
}

pub(crate) fn run_rounds<F>(
    rounds: u32,
    pre: &[u8; 323],
    eseed: &[u8; 64],
    item_mask: u32,
    mut fetch_item: F,
) -> State
where
    F: FnMut(u32, &mut [u32; ITEM_WORDS]),
{
    let mut s = Xof::new(ATTEMPT_TAG);
    s.absorb(pre);
    let seed = s.squeeze::<64>();
    let tpl = templates(eseed);
    let sa = le64(&seed[0..8]);
    let sb = le64(&seed[8..16]);
    let mut sw = [0u32; 16];
    for (i, w) in sw.iter_mut().enumerate() {
        *w = le32(&seed[4 * i..4 * i + 4]);
    }
    let mut scratch = vec![0u32; SCRATCH_WORDS];
    for (i, w) in scratch.iter_mut().enumerate() {
        *w = fold64(
            splitmix64(sa.wrapping_add((i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)))
                ^ splitmix64(sb ^ (i as u64).wrapping_mul(0xff51_afd7_ed55_8ccd)),
        );
    }
    let mut r = [[0u32; REGS]; LANES];
    for (lane, regs) in r.iter_mut().enumerate() {
        let mut x = sa ^ splitmix64(sb.wrapping_add((lane as u64) << 32));
        for (j, reg) in regs.iter_mut().enumerate() {
            x = splitmix64(
                x.wrapping_add(0x9e37_79b9_7f4a_7c15)
                    .wrapping_add(j as u64)
                    .wrapping_add((lane as u64).wrapping_mul(0x1_0000_0001b3)),
            );
            *reg = mix32(
                fold64(x)
                    ^ sw[(lane + j) & 15]
                    ^ (lane as u32).wrapping_mul(0x85eb_ca6b)
                    ^ (j as u32).wrapping_mul(0xc2b2_ae35),
            );
        }
    }
    let mut prev = 0u32;
    for (lane, regs) in r.iter().enumerate() {
        prev ^= contrib(regs, lane as u32);
    }
    let mut idx = mix32(prev ^ sw[0]) & item_mask;
    let mut item = [0u32; ITEM_WORDS];
    for t in 0..rounds {
        fetch_item(idx, &mut item);
        let ops = &tpl[((prev >> 20) & (TEMPLATES as u32 - 1)) as usize];
        let mut sread = [0u32; LANES];
        for (l, regs) in r.iter_mut().enumerate() {
            let w = &item[l * WORDS_PER_LANE..(l + 1) * WORDS_PER_LANE];
            for j in 0..WORDS_PER_LANE {
                regs[j] ^= w[j];
            }
            sread[l] = scratch[((regs[8] ^ w[0]) & (SCRATCH_WORDS as u32 - 1)) as usize];
        }
        let mut con = [0u32; LANES];
        for l in 0..LANES {
            let li = l as u32;
            let regs = &mut r[l];
            regs[9] = regs[9].wrapping_add(sread[l]);
            let salt = t.wrapping_mul(0x9e37_79b9) ^ li.wrapping_mul(0x85eb_ca6b);
            for ins in ops {
                let (d, a, b) = (ins.dst as usize, ins.a as usize, ins.b as usize);
                regs[d] = op(ins.op, regs[d], regs[a], regs[b], ins.imm ^ salt, ins.rot);
            }
            let wa = li * SCRATCH_LANE_WORDS + (regs[10] & (SCRATCH_LANE_WORDS - 1));
            scratch[wa as usize] ^= regs[11];
            con[l] = contrib(regs, li);
        }
        let mut next = 0u32;
        for c in con {
            next ^= c;
        }
        let o1 = 1 + (next & 15) as usize;
        let o2 = 16 + ((next >> 4) & 15) as usize;
        for l in 0..LANES {
            let (n1, n2) = (con[(l + o1) & 31], con[(l + o2) & 31]);
            r[l][15] = rotl32(r[l][15] ^ n1, 13).wrapping_add(n2);
        }
        idx = mix32(next ^ sw[(t & 15) as usize] ^ t ^ 0x6a09_e667) & item_mask;
        prev = next;
    }
    State { seed, r, scratch }
}

/// The work value: cSHAKE256 over the seed, the profile, every register in lane order and the 32 lane
/// digests of the scratchpad, all of fixed length.
pub(crate) fn finish(profile_id: u32, st: &State) -> [u8; 32] {
    let mut f = FINAL_PREFIX
        .get_or_init(|| Xof::cshake(b"", FINAL_TAG))
        .clone();
    f.absorb(&st.seed);
    f.u32v(profile_id);
    for regs in &st.r {
        absorb_words(&mut f, regs);
    }
    for c in 0..LANES {
        f.absorb(&lane_digest(c as u32, &st.scratch[c * 32..c * 32 + 32]));
    }
    f.squeeze::<32>()
}

fn evaluate_items<F>(
    profile_id: u32,
    pre: &[u8; 323],
    eseed: &[u8; 64],
    fetch_item: F,
) -> Option<[u8; 32]>
where
    F: FnMut(u32, &mut [u32; ITEM_WORDS]),
{
    let p = profile(profile_id)?;
    if !preimage_valid(pre) {
        return None;
    }
    let st = run_rounds(p.rounds, pre, eseed, p.item_mask(), fetch_item);
    Some(finish(profile_id, &st))
}

pub fn evaluate(profile_id: u32, pre: &[u8; 323], eseed: &[u8; 64], d: &[u32]) -> Option<[u8; 32]> {
    if d.len() != dataset_words(profile_id)? {
        return None;
    }
    evaluate_items(profile_id, pre, eseed, |idx, item| {
        let b = (idx as usize) * ITEM_WORDS;
        item.copy_from_slice(&d[b..b + ITEM_WORDS]);
    })
}

/// `evaluate` with the dataset in parts, as wasm32 cannot hold 2 GiB in one slice: `fetch` copies a node.
pub fn evaluate_nodes<F>(
    profile_id: u32,
    pre: &[u8; 323],
    eseed: &[u8; 64],
    mut fetch: F,
) -> Option<[u8; 32]>
where
    F: FnMut(u32, &mut [u32; 32]),
{
    let mut node = [0u32; NODE_WORDS];
    evaluate_items(profile_id, pre, eseed, |idx, item| {
        for k in 0..ITEM_NODES {
            fetch(idx * ITEM_NODES as u32 + k as u32, &mut node);
            item[k * NODE_WORDS..(k + 1) * NODE_WORDS].copy_from_slice(&node);
        }
    })
}

pub fn evaluate_light(
    profile_id: u32,
    pre: &[u8; 323],
    eseed: &[u8; 64],
    cache: &[u32],
) -> Option<[u8; 32]> {
    let p = profile(profile_id)?;
    if cache.len() != p.cache_words() {
        return None;
    }
    evaluate_items(profile_id, pre, eseed, |idx, item| {
        dataset_item(&p, cache, eseed, idx, item)
    })
}

pub fn meets_target(v: &[u8; 32], t: &[u8; 32]) -> bool {
    v <= t
}
