-- CreateEnum
CREATE TYPE "ConversionLanguage" AS ENUM ('PYTHON', 'R');

-- AlterTable
ALTER TABLE "CodeEntry" ADD COLUMN     "language" "ConversionLanguage" NOT NULL DEFAULT 'PYTHON',
ALTER COLUMN "name" DROP DEFAULT;
