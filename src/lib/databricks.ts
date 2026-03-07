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

const client = new DBSQLClient();

let connectionPromise: ReturnType<typeof client.connect> | null = null;

async function getConnection() {
  if (!connectionPromise) {
    connectionPromise = client.connect({
      host,
      path,
      token,
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
  const connection = await getConnection();
  const session = await connection.openSession();
  try {
    const options = params?.length
      ? {
          ordinalParameters: params.map((value) => new DBSQLParameter({ value })),
        }
      : undefined;
    const operation = await session.executeStatement(statement, options);
    const rows = (await operation.fetchAll()) as T[];
    await operation.close();
    return rows;
  } finally {
    await session.close();
  }
}
