-- AlterTable
ALTER TABLE "CodeEntry" ADD COLUMN     "name" TEXT NOT NULL DEFAULT 'Untitled';

-- Backfill any existing rows just in case
UPDATE "CodeEntry" SET "name" = 'Untitled' WHERE "name" IS NULL;

-- CreateIndex
CREATE INDEX "CodeEntry_userId_name_idx" ON "CodeEntry"("userId", "name");
