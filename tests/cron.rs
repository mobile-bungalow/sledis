mod common;
use common::TempDb;
use sledis::JobScheduler;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn can_construct_scheduler() {
    let db = Arc::new(TempDb::new());
    let _ = JobScheduler::new(db.clone()).unwrap();
}

#[test]
fn schedule_deletion() {
    let db = Arc::new(TempDb::new());
    let mut sched = JobScheduler::new(db.clone()).unwrap();

    let (k, v) = (b"key_1", b"val_1");
    db.blob_set(k, v.into()).unwrap();

    // the key was inserted
    let ret_v = db.blob_get(k).unwrap().unwrap();
    assert_eq!(&v, &ret_v.as_ref(), "Value was not inserted.");

    let dur = Duration::from_secs(2);
    sched.expire_blob_in(k.into(), dur).unwrap();
    std::thread::sleep(dur);

    let ret_v = db.blob_get(k).unwrap();
    assert_eq!(ret_v, None, "Value Was not Deleted.");

    // the key was inserted
}
