import "server-only";
import { DefaultAzureCredential } from "@azure/identity";
import {
  BlockBlobClient,
  BlobSASPermissions,
  BlobServiceClient,
  SASProtocol,
  StorageSharedKeyCredential,
  generateBlobSASQueryParameters,
} from "@azure/storage-blob";
import { randomUUID } from "node:crypto";

type UploadedExecutionInput = {
  name: string;
  url: string;
};

type UploadedBlobReference = {
  blobName: string;
  url: string;
};

type KeyAuthConfig = {
  kind: "shared-key";
  accountName: string;
  accountKey: string;
};

type IdentityAuthConfig = {
  kind: "identity";
  accountName: string;
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
  return { kind: "shared-key", accountName, accountKey } satisfies KeyAuthConfig;
}

function getIdentityConfig() {
  const accountName = process.env.AZURE_STORAGE_ACCOUNT_NAME?.trim();
  if (!accountName) {
    return null;
  }
  return { kind: "identity", accountName } satisfies IdentityAuthConfig;
}

export function isAzureBlobUploadConfigured() {
  return Boolean(
    process.env.AZURE_STORAGE_CONNECTION_STRING?.trim() ||
      getAccountConfig() ||
      getIdentityConfig(),
  );
}

function getBlobServiceClient() {
  const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING?.trim();
  if (connectionString) {
    return BlobServiceClient.fromConnectionString(connectionString);
  }

  const account = getAccountConfig();
  if (account) {
    return new BlobServiceClient(
      `https://${account.accountName}.blob.core.windows.net`,
      new StorageSharedKeyCredential(account.accountName, account.accountKey),
    );
  }

  const identity = getIdentityConfig();
  if (!identity) {
    throw new Error(
      "Azure Blob Storage is not configured. Set AZURE_STORAGE_CONNECTION_STRING, or AZURE_STORAGE_ACCOUNT_NAME with either AZURE_STORAGE_ACCOUNT_KEY or Azure managed identity / Entra ID.",
    );
  }

  return new BlobServiceClient(
    `https://${identity.accountName}.blob.core.windows.net`,
    new DefaultAzureCredential(),
  );
}

async function buildReadSasUrl(params: {
  blobClient: BlockBlobClient;
  containerName: string;
  blobName: string;
  expiresInMinutes: number;
}) {
  const account = getAccountConfig();
  if (account) {
    const sas = generateBlobSASQueryParameters(
      {
        containerName: params.containerName,
        blobName: params.blobName,
        permissions: BlobSASPermissions.parse("r"),
        startsOn: new Date(Date.now() - 5 * 60 * 1000),
        expiresOn: new Date(Date.now() + params.expiresInMinutes * 60 * 1000),
        protocol: SASProtocol.Https,
      },
      new StorageSharedKeyCredential(account.accountName, account.accountKey),
    ).toString();
    return `${params.blobClient.url}?${sas}`;
  }

  const identity = getIdentityConfig();
  if (!identity) {
    throw new Error(
      "Azure Blob Storage is not configured for SAS URL generation. Provide AZURE_STORAGE_ACCOUNT_NAME with managed identity / Entra ID, or AZURE_STORAGE_ACCOUNT_KEY.",
    );
  }

  const serviceClient = getBlobServiceClient();
  const delegationKey = await serviceClient.getUserDelegationKey(
    new Date(Date.now() - 5 * 60 * 1000),
    new Date(Date.now() + params.expiresInMinutes * 60 * 1000),
  );
  const sas = generateBlobSASQueryParameters(
    {
      containerName: params.containerName,
      blobName: params.blobName,
      permissions: BlobSASPermissions.parse("r"),
      startsOn: new Date(Date.now() - 5 * 60 * 1000),
      expiresOn: new Date(Date.now() + params.expiresInMinutes * 60 * 1000),
      protocol: SASProtocol.Https,
    },
    delegationKey,
    identity.accountName,
  ).toString();
  return `${params.blobClient.url}?${sas}`;
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

  const uploads = await Promise.all(
    files.map(async (file) => {
      const blobName = buildBlobName(file.name);
      const blobClient = containerClient.getBlockBlobClient(blobName);
      await blobClient.uploadData(file.content, {
        blobHTTPHeaders: {
          blobContentType: "application/octet-stream",
        },
      });

      return {
        name: file.name,
        url: await buildReadSasUrl({
          blobClient,
          containerName,
          blobName,
          expiresInMinutes,
        }),
      } satisfies UploadedExecutionInput;
    }),
  );

  return uploads;
}

export async function uploadTextToAzureAndGetSasUrl(params: {
  fileName: string;
  content: string;
  expiresInMinutes?: number;
  contentType?: string;
}) {
  const serviceClient = getBlobServiceClient();
  const containerName = getContainerName();
  const containerClient = serviceClient.getContainerClient(containerName);
  await containerClient.createIfNotExists();

  const blobName = buildBlobName(params.fileName);
  const blobClient = containerClient.getBlockBlobClient(blobName);
  await blobClient.uploadData(Buffer.from(params.content, "utf8"), {
    blobHTTPHeaders: {
      blobContentType: params.contentType || "text/plain; charset=utf-8",
    },
  });

  return {
    blobName,
    url: await buildReadSasUrl({
      blobClient,
      containerName,
      blobName,
      expiresInMinutes: params.expiresInMinutes || 60,
    }),
  } satisfies UploadedBlobReference;
}

export async function uploadBinaryToAzureAndGetSasUrl(params: {
  fileName: string;
  content: Buffer;
  expiresInMinutes?: number;
  contentType?: string;
}) {
  const serviceClient = getBlobServiceClient();
  const containerName = getContainerName();
  const containerClient = serviceClient.getContainerClient(containerName);
  await containerClient.createIfNotExists();

  const blobName = buildBlobName(params.fileName);
  const blobClient = containerClient.getBlockBlobClient(blobName);
  await blobClient.uploadData(params.content, {
    blobHTTPHeaders: {
      blobContentType: params.contentType || "application/octet-stream",
    },
  });

  return {
    blobName,
    url: await buildReadSasUrl({
      blobClient,
      containerName,
      blobName,
      expiresInMinutes: params.expiresInMinutes || 60,
    }),
  } satisfies UploadedBlobReference;
}
