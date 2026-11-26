-- sqlite
CREATE TABLE media_backfills (
	miroir_id VARCHAR(24) NOT NULL,
	connection_id VARCHAR(24) NOT NULL,
	kind VARCHAR(40) NOT NULL,
	"after" TEXT NOT NULL,
	updated_at DATETIME NOT NULL,
	PRIMARY KEY (miroir_id, connection_id, kind)
);
CREATE TABLE media_pending (
	miroir_id VARCHAR(24) NOT NULL,
	connection_id VARCHAR(24) NOT NULL,
	kind VARCHAR(40) NOT NULL,
	"key" VARCHAR(512) NOT NULL,
	size BIGINT NOT NULL,
	stored_at DATETIME NOT NULL,
	attempts INTEGER NOT NULL,
	retry BOOLEAN NOT NULL,
	PRIMARY KEY (miroir_id, connection_id, kind, "key")
);
CREATE INDEX ix_media_pending_stored ON media_pending (miroir_id, connection_id, kind, stored_at);
-- postgresql
CREATE TABLE media_backfills (
	miroir_id VARCHAR(24) NOT NULL,
	connection_id VARCHAR(24) NOT NULL,
	kind VARCHAR(40) NOT NULL,
	after TEXT NOT NULL,
	updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
	PRIMARY KEY (miroir_id, connection_id, kind)
);
CREATE TABLE media_pending (
	miroir_id VARCHAR(24) NOT NULL,
	connection_id VARCHAR(24) NOT NULL,
	kind VARCHAR(40) NOT NULL,
	key VARCHAR(512) NOT NULL,
	size BIGINT NOT NULL,
	stored_at TIMESTAMP WITH TIME ZONE NOT NULL,
	attempts INTEGER NOT NULL,
	retry BOOLEAN NOT NULL,
	PRIMARY KEY (miroir_id, connection_id, kind, key)
);
CREATE INDEX ix_media_pending_stored ON media_pending (miroir_id, connection_id, kind, stored_at);
