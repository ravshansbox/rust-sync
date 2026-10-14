use crate::config::Shared;
use crate::index::Entry;

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
mod tests {
    use super::*;
    use crate::index::{FileMeta, Vv};
    use crate::net::{self, Msg};

    fn entry(path: String, nodes: u64) -> Entry {
        let vv = Vv((0..nodes).map(|n| (u64::MAX - n, u64::MAX)).collect());
        Entry { path, meta: FileMeta { hash: Some([1; 32]), size: 1, mtime: 0, mode: 0o644, vv, by: 0 } }
    }

    #[test]
    fn batches_keeps_order_and_skips_empty() {
        assert!(batches(Vec::new()).is_empty());
        let entries: Vec<_> = (0..1000).map(|i| entry(format!("~/{i}"), 1)).collect();
        let out = batches(entries);
        assert!(out.len() > 1);
        assert!(out.iter().all(|b| !b.is_empty()));
        let paths: Vec<_> = out.iter().flatten().map(|e| e.path.clone()).collect();
        assert_eq!(paths, (0..1000).map(|i| format!("~/{i}")).collect::<Vec<_>>());
    }

    #[test]
    fn batches_fit_in_one_message() {
        let long_paths = (0..500).map(|i| entry(format!("~/{}/{i}", "x".repeat(200)), 8));
        let long_histories = (0..50).map(|i| entry(format!("~/{i}"), 100));
        for b in batches(long_paths.chain(long_histories).collect()) {
            let len = postcard::to_stdvec(&Msg::Index(b)).unwrap().len();
            assert!(len <= net::MAX_MSG, "{len}");
        }
    }

    #[test]
    fn batches_keeps_entries_that_reach_the_limit_exactly_together() {
        let half = || entry("x".repeat(20_000 - 100), 0);
        assert_eq!(batches(vec![half(), half()]).len(), 1);
        assert_eq!(batches(vec![half(), half(), entry("y".into(), 0)]).len(), 2);
    }

    #[test]
    fn batches_gives_an_oversized_entry_its_own_batch_even_when_first() {
        let big = entry(format!("~/{}", "b".repeat(50_000)), 1);
        let out = batches(vec![entry("~/a".into(), 1), big.clone(), entry("~/c".into(), 1)]);
        assert_eq!(out.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1, 1]);
        let out = batches(vec![big, entry("~/c".into(), 1)]);
        assert_eq!(out.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1]);
    }
}
