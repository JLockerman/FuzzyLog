
use super::*;


fn new_log() -> ServerLog<(), VecDeque<ToWorker<()>>> {
    ServerLog::new(0, 1, Default::default())
}
