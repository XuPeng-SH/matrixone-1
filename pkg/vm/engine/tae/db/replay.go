package db

func (db *DB) ReplayDDL() {
	db.Wal.Replay(db.Catalog.OnReplayWal)
}
