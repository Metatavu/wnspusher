#
# file: migrations/20180131_initial.py
#
from yoyo import step

initial = """
CREATE TABLE "clientapplication" (
  "id" SERIAL PRIMARY KEY,
  "app_name" TEXT NOT NULL,
  "client_id" TEXT NOT NULL,
  "client_secret" TEXT NOT NULL
);
CREATE TABLE "apikey" (
  "id" SERIAL PRIMARY KEY,
  "app" INTEGER NOT NULL,
  "permission_level" INTEGER NOT NULL,
  "issued" TIMESTAMP NOT NULL,
  "key" TEXT UNIQUE NOT NULL
);
CREATE INDEX "idx_apikey__app" ON "apikey" ("app");
ALTER TABLE "apikey" ADD CONSTRAINT "fk_apikey__app" FOREIGN KEY ("app") REFERENCES "clientapplication" ("id");
CREATE TABLE "message" (
  "id" SERIAL PRIMARY KEY,
  "app" INTEGER NOT NULL,
  "content" TEXT NOT NULL,
  "content_type" TEXT NOT NULL,
  "wns_type" TEXT NOT NULL
);
CREATE INDEX "idx_message__app" ON "message" ("app");
ALTER TABLE "message" ADD CONSTRAINT "fk_message__app" FOREIGN KEY ("app") REFERENCES "clientapplication" ("id");
CREATE TABLE "subscriber" (
  "id" SERIAL PRIMARY KEY,
  "app" INTEGER NOT NULL,
  "channel_url" TEXT UNIQUE NOT NULL,
  "subscribed" TIMESTAMP NOT NULL
);
CREATE INDEX "idx_subscriber__app" ON "subscriber" ("app");
ALTER TABLE "subscriber" ADD CONSTRAINT "fk_subscriber__app" FOREIGN KEY ("app") REFERENCES "clientapplication" ("id");
CREATE TABLE "pendingnotification" (
  "id" SERIAL PRIMARY KEY,
  "subscriber" INTEGER NOT NULL,
  "message" INTEGER NOT NULL,
  "issued" TIMESTAMP NOT NULL
);
CREATE INDEX "idx_pendingnotification__issued" ON "pendingnotification" ("issued");
CREATE INDEX "idx_pendingnotification__message" ON "pendingnotification" ("message");
CREATE INDEX "idx_pendingnotification__subscriber" ON "pendingnotification" ("subscriber");
ALTER TABLE "pendingnotification" ADD CONSTRAINT "fk_pendingnotification__message" FOREIGN KEY ("message") REFERENCES "message" ("id");
ALTER TABLE "pendingnotification" ADD CONSTRAINT "fk_pendingnotification__subscriber" FOREIGN KEY ("subscriber") REFERENCES "subscriber" ("id");
CREATE TABLE "wnsaccesstoken" (
  "id" SERIAL PRIMARY KEY,
  "app" INTEGER NOT NULL,
  "issued" TIMESTAMP NOT NULL,
  "content" TEXT NOT NULL
);
CREATE INDEX "idx_wnsaccesstoken__app" ON "wnsaccesstoken" ("app");
ALTER TABLE "wnsaccesstoken" ADD CONSTRAINT "fk_wnsaccesstoken__app" FOREIGN KEY ("app") REFERENCES "clientapplication" ("id");
"""

step(initial)