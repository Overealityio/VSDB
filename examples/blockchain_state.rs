//!
//! An example for stateless functions.
//!

use once_cell::sync::Lazy;
use ruc::*;
use serde::{Deserialize, Serialize};
use std::{
    mem,
    sync::{mpsc::channel, Mutex},
    thread,
};
use vsdb::{
    vsdb_set_base_dir, BranchName, MapxVs, OrphanVs, ValueEnDe, VecxVs, VersionName,
    VersionNameOwned, Vs, VsMgmt,
};

type Amount = u64;
type Address = Vec<u8>;
type ConsensusInt = i128;

#[derive(Vs, Clone, Debug, Serialize, Deserialize)]
struct WorldState {
    transactions: VecxVs<Transaction>,
    balances: MapxVs<Address, Amount>,
    a_consensus_int: OrphanVs<ConsensusInt>,
}

const MASTER_BRANCH: &str = "master";
const PRE_CHECK_BRANCH: &str = "pre_check";
const FORMAL_CHECK_BRANCH: &str = "formal_check";

static WORLD_STATE: Lazy<Mutex<WorldState>> =
    Lazy::new(|| Mutex::new(WorldState::load_or_init().unwrap()));

static WORLD_STATE_SNAP_0: Lazy<Mutex<WorldState>> = Lazy::new(|| {
    let mut snap0 = WORLD_STATE.lock().unwrap().clone();
    pnk!(snap0.reset_branch(PRE_CHECK_BRANCH));
    Mutex::new(snap0)
});

static WORLD_STATE_SNAP_1: Lazy<Mutex<WorldState>> = Lazy::new(|| {
    let mut snap1 = WORLD_STATE.lock().unwrap().clone();
    pnk!(snap1.reset_branch(FORMAL_CHECK_BRANCH));
    Mutex::new(snap1)
});

static MEM_POOL: Lazy<Mutex<Vec<Transaction>>> = Lazy::new(|| Mutex::new(vec![]));

fn random_version() -> VersionNameOwned {
    VersionNameOwned(
        (1_0000_0000 + rand::random::<u64>() / 2)
            .to_be_bytes()
            .to_vec(),
    )
}

fn transaction_pre_check(tx: Transaction) {
    let mut snap0 = WORLD_STATE_SNAP_0.lock().unwrap();
    snap0.push_version(&tx.hash()).unwrap();
    if snap0.apply_transaction(&tx).is_ok() {
        MEM_POOL.lock().unwrap().push(tx);
    } else {
        snap0.version_pop().unwrap();
    }
}

fn begin_block() {
    let mut snap0 = WORLD_STATE_SNAP_0.lock().unwrap();
    pnk!(snap0.reset_branch(PRE_CHECK_BRANCH));
    let mut snap1 = WORLD_STATE_SNAP_1.lock().unwrap();
    pnk!(snap1.reset_branch(FORMAL_CHECK_BRANCH));
}

fn transaction_formal_check_all() {
    let mut snap1 = WORLD_STATE_SNAP_1.lock().unwrap().clone();
    for tx in mem::take(&mut *MEM_POOL.lock().unwrap()).into_iter() {
        snap1.push_version(&tx.hash()).unwrap();
        if snap1.apply_transaction(&tx).is_err() {
            snap1.version_pop().unwrap();
        }
        unreachable!(); // all testing transactions are failed
    }
}

fn end_block() {
    let mut snap1 = WORLD_STATE_SNAP_1.lock().unwrap();
    snap1.merge_branch(FORMAL_CHECK_BRANCH).unwrap();
}

impl WorldState {
    // sample code
    fn load_or_init() -> Result<Self> {
        let mut ws = WorldState {
            transactions: VecxVs::new(),
            balances: MapxVs::new(),
            a_consensus_int: OrphanVs::new(),
        };

        if !ws.branch_is_found(MASTER_BRANCH) {
            ws.push_version(b"init version").c(d!())?;
            ws.new_branch(MASTER_BRANCH).c(d!())?;
        }
        ws.set_default_branch(MASTER_BRANCH).c(d!())?;
        ws.push_version(b"init version 2").c(d!())?;

        ws.new_branch(PRE_CHECK_BRANCH).c(d!())?;
        ws.new_branch(FORMAL_CHECK_BRANCH).c(d!())?;

        Ok(ws)
    }

    fn apply_transaction(&mut self, tx: &Transaction) -> Result<()> {
        self.a_very_complex_function_will_change_state(tx).c(d!())
    }

    // sample code
    fn a_very_complex_function_will_change_state(
        &mut self,
        tx: &Transaction,
    ) -> Result<()> {
        if tx.from.get(0).is_some() {
            Ok(())
        } else {
            // ..........
            Err(eg!("error occur"))
        }
    }

    fn branch_is_found(&self, branch: &str) -> bool {
        let br = BranchName(branch.as_bytes());
        self.branch_exists(br)
    }

    fn new_branch(&mut self, branch: &str) -> Result<()> {
        let br = BranchName(branch.as_bytes());
        self.branch_create(br, random_version().as_deref(), false)
            .c(d!())
    }

    fn delete_branch(&mut self, branch: &str) -> Result<()> {
        let br = BranchName(branch.as_bytes());
        self.branch_remove(br).c(d!())
    }

    fn merge_branch(&mut self, branch: &str) -> Result<()> {
        let br = BranchName(branch.as_bytes());
        self.branch_merge_to(br, BranchName(MASTER_BRANCH.as_bytes()))
            .c(d!())
    }

    fn set_default_branch(&mut self, branch: &str) -> Result<()> {
        let br = BranchName(branch.as_bytes());
        self.branch_set_default(br).c(d!())
    }

    fn reset_branch(&mut self, branch: &str) -> Result<()> {
        self.set_default_branch(MASTER_BRANCH)
            .c(d!())
            .and_then(|_| self.delete_branch(branch).c(d!()))
            .and_then(|_| self.new_branch(branch).c(d!()))
            .and_then(|_| self.set_default_branch(branch).c(d!()))
    }

    fn push_version(&mut self, version: &[u8]) -> Result<()> {
        let ver = VersionName(version);
        self.version_create(ver).c(d!())
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
struct Transaction {
    from: Address,
    to: Address,
    amount: Amount,
}

impl Transaction {
    fn hash(&self) -> Vec<u8> {
        // assume this is a hash function
        self.encode().to_vec()
    }
}

impl Transaction {
    fn new(amount: Amount) -> Self {
        Self {
            from: vec![],
            to: vec![],
            amount,
        }
    }
}

fn main() {
    pnk!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let (sender, reveiver) = channel();

    thread::spawn(move || {
        loop {
            for tx in reveiver.iter() {
                transaction_pre_check(tx);
            }
        }
    });

    (0..10).for_each(|i| sender.send(Transaction::new(i)).unwrap());

    sleep_ms!(60);

    begin_block();
    transaction_formal_check_all();
    end_block();
}
