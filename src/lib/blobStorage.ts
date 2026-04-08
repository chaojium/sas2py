import "server-only";
import {
  BlobSASPermissions,
  BlobServiceClient,
  StorageSharedKeyCredential,
  generateBlobSASQueryParameters,
} from "@azure/storage-blob";
import { randomUUID } from "node:crypto";

type UploadedExecutionInput = {
  name: string;
  url: string;
};

function getContainerName() {
  return process.env.AZURE_STORAGE_CONTAINER?.trim() || "sas2py-inputs";
}

function getBlobPrefix() {
  return (
    process.env.AZURE_STORAGE_BLOB_PREFIX?.trim().replace(/^\/+|\/+$/g, "") ||
    "execution-inputs"
  );
}

function getAccountConfig() {
  const accountName = process.env.AZURE_STORAGE_ACCOUNT_NAME?.trim();
  const accountKey = process.env.AZURE_STORAGE_ACCOUNT_KEY?.trim();
  if (!accountName || !accountKey) {
    return null;
  }
  return { accountName, accountKey };
}

export function isAzureBlobUploadConfigured() {
  return Boolean(
    process.env.AZURE_STORAGE_CONNECTION_STRING?.trim() || getAccountConfig(),
  );
}

function getBlobServiceClient() {
  const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING?.trim();
  if (connectionString) {
    return BlobServiceClient.fromConnectionString(connectionString);
  }

  const account = getAccountConfig();
  if (!account) {
    throw new Error(
      "Azure Blob Storage is not configured. Set AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY.",
    );
  }

  return new BlobServiceClient(
    `https://${account.accountName}.blob.core.windows.net`,
    new StorageSharedKeyCredential(account.accountName, account.accountKey),
  );
}

function buildBlobName(fileName: string) {
  const prefix = getBlobPrefix();
  const stamp = new Date().toISOString().slice(0, 10);
  return `${prefix}/${stamp}/${randomUUID()}-${fileName}`;
}

export async function uploadExecutionInputsToAzure(
  files: { name: string; content: Buffer }[],
  expiresInMinutes = 60,
) {
  const serviceClient = getBlobServiceClient();
  const containerName = getContainerName();
  const containerClient = serviceClient.getContainerClient(containerName);
  await containerClient.createIfNotExists();

  const account = getAccountConfig();
  const uploads = await Promise.all(
    files.map(async (file) => {
      const blobName = buildBlobName(file.name);
      const blobClient = containerClient.getBlockBlobClient(blobName);
      await blobClient.uploadData(file.content, {
        blobHTTPHeaders: {
          blobContentType: "application/octet-stream",
        },
      });

      if (!account) {
        throw new Error(
          "AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY are required to generate SAS URLs for Databricks input files.",
        );
      }

      const sas = generateBlobSASQueryParameters(
        {
          containerName,
          blobName,
          permissions: BlobSASPermissions.parse("r"),
          startsOn: new Date(Date.now() - 5 * 60 * 1000),
          expiresOn: new Date(Date.now() + expiresInMinutes * 60 * 1000),
          protocol: "https",
        },
        new StorageSharedKeyCredential(account.accountName, account.accountKey),
      ).toString();

      return {
        name: file.name,
        url: `${blobClient.url}?${sas}`,
      } satisfies UploadedExecutionInput;
    }),
  );

  return uploads;
}
