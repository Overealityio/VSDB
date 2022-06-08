//!
//! An example for stateless functions.
//!

use ruc::*;
use serde::{Deserialize, Serialize};
use std::{sync::mpsc::channel, thread};
use vsdb::{vsdb_set_base_dir, Mapx, ValueEnDe, Vecx};

#[derive(Debug, Deserialize, Serialize)]
struct Animal {
    id: u64,
    kind: AnimalKind,
    owner: AnimalOwner,
}

#[derive(Debug, Deserialize, Serialize)]
enum AnimalKind {
    Dog,
    Cat,
    Pig,
    Monkey,
    Donkey,
    Unknown,
}

type AnimalID = u64;
type AnimalOwner = String;

#[derive(Default, Debug, Deserialize, Serialize)]
struct AnimalHospital {
    owners: Mapx<AnimalOwner, Mapx<AnimalID, Animal>>,
    discharge_history: Vecx<AnimalID>,
    id_alloctor: u64,
}

impl AnimalHospital {
    fn hospitalized(
        &mut self,
        kind: AnimalKind,
        owner: AnimalOwner,
    ) -> Result<AnimalID> {
        if matches!(kind, AnimalKind::Unknown) {
            return Err(eg!("unsupported animal"));
        }

        let new_id = self.alloc_id();
        let animal = Animal {
            id: new_id,
            kind,
            owner: owner.clone(),
        };
        self.owners
            .entry(owner)
            .or_insert(Mapx::new())
            .insert(new_id, animal);

        Ok(new_id)
    }

    fn discharged(&mut self, owner: AnimalOwner, id: AnimalID) -> Result<()> {
        if let Some(animals) = self.owners.get_mut(&owner) {
            if animals.remove(&id).is_none() {
                return Err(eg!("animal ID not found"));
            }
            self.discharge_history.push(id);

            Ok(())
        } else {
            Err(eg!("owner not found"))
        }
    }

    fn alloc_id(&mut self) -> AnimalID {
        self.id_alloctor += 1;
        self.id_alloctor
    }
}

fn main() {
    pnk!(vsdb_set_base_dir(&format!(
        "/tmp/vsdb_testing/{}",
        rand::random::<u64>()
    )));

    let (sender, receiver) = channel();

    thread::spawn(move || {
        let mut ah = AnimalHospital::default();
        let ids = (0..100)
            .map(|i| ah.hospitalized(AnimalKind::Cat, format!("owner-{}", i)))
            .collect::<Result<Vec<_>>>()
            .unwrap();
        sender.send((ah.encode(), ids)).unwrap();
    });

    let (ah_bytes, ids) = receiver.recv().unwrap();

    // will be re-initilized when serializing
    let mut ah: AnimalHospital = ValueEnDe::decode(&ah_bytes).unwrap();

    (0..100)
        .zip(ids.iter())
        .map(|(i, id)| ah.discharged(format!("owner-{}", i), *id))
        .collect::<Result<Vec<_>>>()
        .unwrap();
}
