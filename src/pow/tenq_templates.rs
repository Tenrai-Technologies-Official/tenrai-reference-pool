// Normative template operations and bounded epoch program generation.

#[inline(always)]
pub(crate) fn op(code: u32, x: u32, y: u32, z: u32, m: u32, rot: u32) -> u32 {
    match code {
        0 => x.wrapping_add(y ^ m),
        1 => x.wrapping_sub(z.wrapping_add(m)),
        2 => x ^ rotl32(y, rot) ^ m,
        3 => x.wrapping_mul(y | 1),
        4 => rotl32(x.wrapping_add(y), z.wrapping_add(rot)) ^ m,
        5 => mix32(x ^ y ^ m).wrapping_add(z),
        6 => x.wrapping_add(mulhi32(y | 1, z ^ m)),
        7 => x.swap_bytes().wrapping_add(y ^ m),
        8 => x ^ if y < z { m } else { !m },
        9 => x.wrapping_add(y >> (z & 31)).wrapping_add(m),
        10 => rotl32(x ^ z, rot).wrapping_add(mix32(y.wrapping_add(m))),
        11 => x.wrapping_add(y).wrapping_add(z).wrapping_add(m),
        12 => x ^ y.min(z).wrapping_add(m),
        13 => x.wrapping_mul(z | 1) ^ rotl32(y, rot),
        14 => x.wrapping_add(y.max(z ^ m) ^ rotl32(y, rot)),
        _ => rotl32(x, rot) ^ y.wrapping_add(z) ^ m,
    }
}

// Input bits name the old destination, a and b, in that order.
pub(crate) const TEMPLATE_INPUT_READS: [u8; 16] = [3, 5, 3, 3, 7, 7, 7, 3, 7, 7, 7, 7, 7, 7, 7, 7];
pub(crate) const TEMPLATE_INPUT_BIJECTIVE: [u8; 16] =
    [3, 5, 3, 1, 3, 7, 1, 3, 1, 1, 7, 7, 1, 3, 1, 7];
pub(crate) const TEMPLATE_DRAW_LIMIT: usize = 64;

#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct Ins {
    pub op: u32,
    pub dst: u32,
    pub a: u32,
    pub b: u32,
    pub rot: u32,
    pub imm: u32,
}

#[derive(Clone, Copy, Default)]
struct ItemDependency {
    deps: u8,
    bijective: u8,
}

fn instruction_dependency(code: u32, inputs: [ItemDependency; 3]) -> ItemDependency {
    let reads = TEMPLATE_INPUT_READS[code as usize];
    let invertible = TEMPLATE_INPUT_BIJECTIVE[code as usize];
    let mut deps = [0u8; 3];
    let mut bijective = [0u8; 3];
    for index in 0..3 {
        let flag = 1 << index;
        if reads & flag != 0 {
            deps[index] = inputs[index].deps;
            if invertible & flag != 0 {
                bijective[index] = inputs[index].bijective;
            }
        }
    }
    ItemDependency {
        deps: deps[0] | deps[1] | deps[2],
        bijective: (bijective[0] & !(deps[1] | deps[2]))
            | (bijective[1] & !(deps[0] | deps[2]))
            | (bijective[2] & !(deps[0] | deps[1])),
    }
}

/// A proven full-word bijection in exactly one of the address inputs r12..r14.
/// Other item words and the pre-round state are held fixed. This is not a work/memory lower bound.
pub(crate) fn template_item_proof(tpl: &[Ins; TEMPLATE_OPS]) -> u8 {
    let mut flow = [ItemDependency::default(); REGS];
    for (index, entry) in flow.iter_mut().enumerate().take(8) {
        entry.deps = 1 << index;
        entry.bijective = 1 << index;
    }
    // This lane's w[0] selects a scratch read. Reading it is not a full-word bijection.
    flow[9].deps = 1;
    for ins in tpl {
        if ins.op >= 16 || ins.dst >= REGS as u32 || ins.a >= REGS as u32 || ins.b >= REGS as u32 {
            return 0;
        }
        let (dst, a, b) = (ins.dst as usize, ins.a as usize, ins.b as usize);
        flow[dst] = instruction_dependency(ins.op, [flow[dst], flow[a], flow[b]]);
    }
    (flow[12].bijective & !(flow[13].deps | flow[14].deps))
        | (flow[13].bijective & !(flow[12].deps | flow[14].deps))
        | (flow[14].bijective & !(flow[12].deps | flow[13].deps))
}

pub(crate) fn reads_item(tpl: &[Ins; TEMPLATE_OPS]) -> bool {
    template_item_proof(tpl) != 0
}

fn source_bijective(code: u32) -> bool {
    TEMPLATE_INPUT_BIJECTIVE[code as usize] & 6 != 0
}

/// Every slice needs a source-bijective instruction before register redraws can succeed.
/// The fixed multiset has 120 such opcodes for 16 slices, so a donor always exists.
pub(crate) fn repair_template_codes(codes: &mut [u32; TEMPLATES * TEMPLATE_OPS]) {
    let mut counts = [0u8; TEMPLATES];
    for (index, &code) in codes.iter().enumerate() {
        counts[index / TEMPLATE_OPS] += u8::from(source_bijective(code));
    }
    for recipient in 0..TEMPLATES {
        if counts[recipient] != 0 {
            continue;
        }
        let donor = codes
            .iter()
            .enumerate()
            .position(|(index, &code)| source_bijective(code) && counts[index / TEMPLATE_OPS] > 1)
            .expect("the fixed opcode multiset has a source-bijective donor");
        codes.swap(recipient * TEMPLATE_OPS, donor);
        counts[recipient] += 1;
        counts[donor / TEMPLATE_OPS] -= 1;
    }
}

/// Read-only r2 enters only r12; fillers read r0/r1 and cannot carry item word 2 elsewhere.
pub(crate) fn fallback_template(codes: &[u32; TEMPLATE_OPS]) -> [Ins; TEMPLATE_OPS] {
    let injection = codes
        .iter()
        .position(|&code| source_bijective(code))
        .expect("opcode slices are repaired before any template is built");
    let mut next_destination = 4;
    std::array::from_fn(|index| {
        let dst = if index == injection {
            12
        } else {
            if next_destination == 12 {
                next_destination += 1;
            }
            let dst = next_destination;
            next_destination += 1;
            dst
        };
        let (a, b) = if index == injection {
            if codes[index] == 1 { (0, 2) } else { (2, 0) }
        } else {
            (0, 1)
        };
        Ins {
            op: codes[index],
            dst,
            a,
            b,
            rot: 1,
            imm: 1,
        }
    })
}

/// Fixed opcode counts, bounded redraws, and a proven item-word influence in every template.
pub fn templates(eseed: &[u8; 64]) -> [[Ins; TEMPLATE_OPS]; TEMPLATES] {
    templates_with_draw_limit(eseed, TEMPLATE_DRAW_LIMIT)
}

// The consensus entry point always uses 64; tests can force the deterministic fallback with zero.
pub(crate) fn templates_with_draw_limit(
    eseed: &[u8; 64],
    draw_limit: usize,
) -> [[Ins; TEMPLATE_OPS]; TEMPLATES] {
    let mut s = Xof::new(b"TENRAI/TENQ/TEMPLATES");
    s.absorb(eseed);
    let mut x = le64(&s.squeeze::<8>());
    let mut rand = move || {
        x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
        splitmix64(x)
    };
    const TOTAL: usize = TEMPLATES * TEMPLATE_OPS;
    let mut codes = [0u32; TOTAL];
    for (i, code) in codes.iter_mut().enumerate() {
        *code = (i * 16 / TOTAL) as u32;
    }
    for i in (1..TOTAL).rev() {
        let j = ((u128::from(rand()) * (i as u128 + 1)) >> 64) as usize;
        codes.swap(i, j);
    }
    // All swaps precede materialization, including changes to donor slices.
    repair_template_codes(&mut codes);
    let mut out = [[Ins::default(); TEMPLATE_OPS]; TEMPLATES];
    for (t, tpl) in out.iter_mut().enumerate() {
        let mut accepted = false;
        for _ in 0..draw_limit {
            let mut dsts = [4u32, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
            for i in (1..dsts.len()).rev() {
                let j = ((u128::from(rand()) * (i as u128 + 1)) >> 64) as usize;
                dsts.swap(i, j);
            }
            for (k, slot) in tpl.iter_mut().enumerate() {
                let h = rand();
                let dst = dsts[k];
                let mut a = (h & 15) as u32;
                while a == dst {
                    a = (a + 1) & 15;
                }
                let mut b = ((h >> 4) & 15) as u32;
                while b == dst || b == a {
                    b = (b + 1) & 15;
                }
                *slot = Ins {
                    op: codes[t * TEMPLATE_OPS + k],
                    dst,
                    a,
                    b,
                    rot: 1 + (((h >> 8) % 31) as u32),
                    imm: ((h >> 32) as u32) | 1,
                };
            }
            if reads_item(tpl) {
                accepted = true;
                break;
            }
        }
        if !accepted {
            let slice: &[u32; TEMPLATE_OPS] = (&codes[t * TEMPLATE_OPS..(t + 1) * TEMPLATE_OPS])
                .try_into()
                .expect("fixed opcode slice");
            *tpl = fallback_template(slice);
        }
    }
    out
}
