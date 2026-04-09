import "server-only";
import { DBSQLClient, DBSQLParameter } from "@databricks/sql";

const host = process.env.DATABRICKS_SERVER_HOSTNAME?.trim();
const path = process.env.DATABRICKS_HTTP_PATH?.trim();
const token = process.env.DATABRICKS_ACCESS_TOKEN?.trim();
const catalog = process.env.DATABRICKS_CATALOG || "sas2py";
const schema = process.env.DATABRICKS_SCHEMA || "prod";

if (!host || !path || !token) {
  throw new Error(
    "Databricks connection is missing. Set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_ACCESS_TOKEN.",
  );
}

const databricksHost = host;
const databricksPath = path;
const databricksToken = token;

const client = new DBSQLClient();

let connectionPromise: ReturnType<typeof client.connect> | null = null;

function getHttpStatusCode(error: unknown) {
  if (
    error &&
    typeof error === "object" &&
    "statusCode" in error &&
    typeof error.statusCode === "number"
  ) {
    return error.statusCode;
  }
  return undefined;
}

function normalizeDatabricksError(error: unknown, context: string) {
  const statusCode = getHttpStatusCode(error);

  if (statusCode === 403) {
    return new Error(
      `${context}: Databricks rejected the request with 403 Forbidden. Check DATABRICKS_ACCESS_TOKEN, DATABRICKS_HTTP_PATH, warehouse permissions, and that the same environment variables are configured in Vercel.`,
      { cause: error },
    );
  }

  if (error instanceof Error) {
    return new Error(`${context}: ${error.message}`, { cause: error });
  }

  return new Error(context, { cause: error });
}

async function getConnection() {
  if (!connectionPromise) {
    connectionPromise = client.connect({
      host: databricksHost,
      path: databricksPath,
      token: databricksToken,
    });
  }
  return connectionPromise;
}

export function table(name: string) {
  return `${catalog}.${schema}.${name}`;
}

export async function execute<T = Record<string, unknown>>(
  statement: string,
  params?: unknown[],
) {
  let session: Awaited<ReturnType<Awaited<ReturnType<typeof getConnection>>["openSession"]>> | null =
    null;
  try {
    const connection = await getConnection();
    session = await connection.openSession();
    const options = params?.length
      ? {
          ordinalParameters: params.map((value) => new DBSQLParameter({ value })),
        }
      : undefined;
    const operation = await session.executeStatement(statement, options);
    try {
      const rows = (await operation.fetchAll()) as T[];
      return rows;
    } finally {
      await operation.close();
    }
  } catch (error) {
    throw normalizeDatabricksError(error, "Databricks query failed");
  } finally {
    if (session) {
      await session.close();
    }
  }
}
