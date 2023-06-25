// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use engine_traits::SstFileInfo;

enum SstStatus {
    NotUpload,
    Uploading,
    Uploaded,
}

type SegmentMap = Vec<BTreeMap<Vec<u8>, SstFileInfo>>;
pub struct SegmentMapRouter(HashMap<String, Arc<SegmentMapManager>>);

impl SegmentMapRouter {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn register(&mut self, d: SegmentMap, w: SegmentMap) -> String {
        let (id, manager) = SegmentMapManager::register(d, w);
        self.0.insert(id.clone(), Arc::new(manager));
        id
    }

    pub fn route(&self, id: &str) -> Option<Arc<SegmentMapManager>> {
        self.0.get(id).cloned()
    }
}

pub struct SegmentMapManager {
    map: (SegmentMap, SegmentMap),
    // TODO: directly update the uploaded flag in hashmap
    index_d: Mutex<Vec<Vec<SstStatus>>>,
    index_w: Mutex<Vec<Vec<SstStatus>>>,
}

impl SegmentMapManager {
    fn new(mut d: SegmentMap, mut w: SegmentMap) -> Self {
        let index_d_raw = Self::generate_index(&mut d);
        let index_w_raw = Self::generate_index(&mut w);
        Self {
            map: (d, w),

            index_d: Mutex::new(index_d_raw),
            index_w: Mutex::new(index_w_raw),
        }
    }

    pub fn register(d: SegmentMap, w: SegmentMap) -> (String, Self) {
        let id = uuid::Uuid::new_v4().to_string();

        (id, Self::new(d, w))
    }

    fn generate_index(map: &mut SegmentMap) -> Vec<Vec<SstStatus>> {
        let mut index = Vec::new();
        for tree in map {
            let mut lvl_idx = Vec::new();
            for (idx, (_, info)) in tree.iter_mut().enumerate() {
                info.idx = idx;
                lvl_idx.push(SstStatus::NotUpload);
            }

            index.push(lvl_idx);
        }
        index
    }

    pub fn find_ssts(
        &self,
        start_key: &Vec<u8>,
        end_key: &Vec<u8>,
    ) -> (Vec<Vec<(String, usize)>>, Vec<Vec<(String, usize)>>, usize) {
        let (d, d_cnt) = {
            let mut index_d = self.index_d.lock().unwrap();
            find_ssts_internal(&mut index_d, &self.map.0, start_key, end_key)
        };
        let (w, w_cnt) = {
            let mut index_w = self.index_w.lock().unwrap();
            find_ssts_internal(&mut index_w, &self.map.1, start_key, end_key)
        };
        (d, w, d_cnt + w_cnt)
    }

    pub fn release_index(
        &self,
        d: Vec<Vec<(String, usize)>>,
        d_progress_l: usize,
        d_progress_f: usize,
        w: Vec<Vec<(String, usize)>>,
        w_progress_l: usize,
        w_progress_f: usize,
    ) {
        {
            let mut index_d = self.index_d.lock().unwrap();
            release_index_internal(&mut index_d, d, d_progress_l, d_progress_f);
        }
        {
            let mut index_w = self.index_w.lock().unwrap();
            release_index_internal(&mut index_w, w, w_progress_l, w_progress_f);
        }
    }
}

fn find_ssts_internal(
    index: &mut [Vec<SstStatus>],
    map: &SegmentMap,
    start_key: &Vec<u8>,
    end_key: &Vec<u8>,
) -> (Vec<Vec<(String, usize)>>, usize) {
    let mut res = Vec::new();
    let mut count = 0;
    for (level, tree) in map.iter().enumerate() {
        let lvl_index = &mut index[level];
        let mut fs = Vec::new();
        for f in tree.iter().filter(|info| {
            let idx = info.1.idx;
            if !matches!(lvl_index[idx], SstStatus::NotUpload) {
                return false;
            }

            let sk = info.0;
            let ek = &info.1.end_key;
            if (end_key.is_empty() || sk < end_key) && (ek.is_empty() || ek > start_key) {
                lvl_index[idx] = SstStatus::Uploading;
                return true;
            }
            false
        }) {
            count += 1;
            fs.push((f.1.file_name.clone(), f.1.idx));
        }
        res.push(fs);
    }
    (res, count)
}

fn release_index_internal(
    index: &mut [Vec<SstStatus>],
    findex: Vec<Vec<(String, usize)>>,
    progress_l: usize,
    progress_f: usize,
) {
    for (level, sst_index) in findex.iter().enumerate() {
        for (_, idx) in sst_index {
            index[level][*idx] =
                if level < progress_l || (level == progress_l && *idx <= progress_f) {
                    SstStatus::Uploaded
                } else {
                    SstStatus::NotUpload
                };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use engine_traits::SstFileInfo;

    use super::SegmentMap;

    #[test]
    fn test() {
        use super::SegmentMapManager;

        let (id, manager) =
            SegmentMapManager::register(generate_segment_map(), generate_segment_map());
        println!("{id}");
        let sk = "1_1".as_bytes().to_vec();
        let ek = "2_1".as_bytes().to_vec();
        let (d, _, cnt) = manager.find_ssts(&sk, &ek);
        println!("{:?}", d);
        assert!(cnt > 0);
    }

    fn generate_segment_map() -> SegmentMap {
        let mut map = vec![BTreeMap::new(), BTreeMap::new(), BTreeMap::new()];
        for (i, m) in map.iter_mut().enumerate() {
            for j in 0..3 {
                let sk = format!("{i}_{j}").into_bytes();
                let ek = format!("{}_{}", i, j + 1).into_bytes();
                m.insert(
                    sk,
                    SstFileInfo {
                        end_key: ek,
                        file_name: String::from("/asdfg.sst"),
                        idx: 0,
                    },
                );
            }
        }

        map
    }
}
