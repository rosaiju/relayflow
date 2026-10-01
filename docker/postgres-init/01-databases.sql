-- Extra databases created on first initialization of the volume.
-- relayflow_test: integration tests. mocknotify: used only by local, non-Docker runs of the
-- mock service; in Docker Compose the mock service has its own server (mocknotify-db).
CREATE DATABASE relayflow_test OWNER relayflow;
CREATE DATABASE mocknotify OWNER relayflow;
