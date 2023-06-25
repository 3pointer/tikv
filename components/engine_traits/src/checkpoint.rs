// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeMap, fmt::Debug, path::Path};

use crate::{CfName, Result};

pub trait Checkpointable {
    type Checkpointer: Checkpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer>;

    fn merge(&self, dbs: &[&Self]) -> Result<()>;
}

pub trait Checkpointer {
    fn create_at(
        &mut self,
        db_out_dir: &Path,
        titan_out_dir: Option<&Path>,
        log_size_for_flush: u64,
    ) -> Result<()>;

    fn column_family_meta_data(&self, _cf: CfName) -> Result<ColumnFamilyMetadata> {
        unimplemented!()
    }
}

pub struct SstFileInfo {
    pub file_name: String,
    // pub start_key: Key,
    pub end_key: Vec<u8>,
    pub idx: usize,
}

pub struct ColumnFamilyMetadata {
    pub file_count: usize,
    pub file_size: usize,
    pub ssts: Vec<BTreeMap<Vec<u8>, SstFileInfo>>,
}

impl Debug for ColumnFamilyMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut binding = f.debug_struct("ColumnFamilyMetadata");
        binding.field("file_count", &self.file_count);
        binding.field("file_size", &self.file_size);

        for (level, ssts) in self.ssts.iter().enumerate() {
            let mut ss = String::new();
            for SstFileInfo { file_name, .. } in ssts.values() {
                let str = format!("name: {file_name}");
                ss = ss + &str
            }
            binding.field(&format!("level: {level}"), &ss);
            for sst in ssts {
                binding.field("sk", sst.0);
            }
        }

        binding.finish()
    }
}
