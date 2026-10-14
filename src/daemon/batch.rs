use crate::config::Shared;
use crate::index::Entry;

/// Estimated size at which a message is full, kept below the frame limit.
const BATCH_LIMIT: usize = 40_000;

/// Split entries into messages that fit in one frame.
pub(super) fn batches(entries: Vec<Entry>) -> Vec<Vec<Entry>> {
    let (mut out, mut cur, mut size) = (Vec::new(), Vec::new(), 0);
    for e in entries {
        let s = e.path.len() + 100 + e.meta.vv.0.len() * 20;
        if size + s > BATCH_LIMIT && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
            size = 0;
        }
        size += s;
        cur.push(e);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Split the shared config into parts that each fit in one message.
pub(super) fn config_batches(shared: &Shared) -> Vec<Shared> {
    let mut out = vec![Shared::default()];
    let mut size = 0;
    let mut add = |s: usize, put: &mut dyn FnMut(&mut Shared)| {
        if size + s > BATCH_LIMIT && out.last().is_some_and(|c| !c.nodes.is_empty() || !c.roots.is_empty()) {
            out.push(Shared::default());
            size = 0;
        }
        size += s;
        put(out.last_mut().unwrap());
    };
    for (id, n) in &shared.nodes {
        let s = postcard::to_stdvec(&(id, n)).map_or(0, |b| b.len());
        add(s, &mut |c| drop(c.nodes.insert(id.clone(), n.clone())));
    }
    for (path, r) in &shared.roots {
        let s = postcard::to_stdvec(&(path, r)).map_or(0, |b| b.len());
        add(s, &mut |c| drop(c.roots.insert(path.clone(), r.clone())));
    }
    out
}

#[cfg(test)]
mod tests;
