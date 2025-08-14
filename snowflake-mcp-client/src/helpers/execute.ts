import * as snowflake from 'snowflake-sdk';

/**
 * Promisified wrapper for Snowflake connection.execute()
 * CORRECTNESS: Properly handles the callback-based Node SDK
 */
export function exec(
  conn: snowflake.Connection, 
  sqlText: string, 
  binds: any[] = []
): Promise<any[]> {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      binds,
      complete: (err: any, _stmt: any, rows: any) => {
        if (err) {
          reject(new Error(`SQL execution failed: ${err.message}`));
        } else {
          resolve(rows || []);
        }
      }
    });
  });
}

/**
 * Execute with retry logic for transient failures
 */
export async function execWithRetry(
  conn: snowflake.Connection,
  sqlText: string,
  binds: any[] = [],
  maxRetries: number = 3
): Promise<any[]> {
  let lastError: Error;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await exec(conn, sqlText, binds);
    } catch (error) {
      lastError = error as Error;
      
      // Check if this is a transient error worth retrying
      if (isTransientError(error as Error) && attempt < maxRetries) {
        const backoffMs = Math.min(1000 * Math.pow(2, attempt - 1), 10000);
        await new Promise(resolve => setTimeout(resolve, backoffMs));
        continue;
      }
      
      throw error;
    }
  }
  
  throw lastError!;
}

/**
 * Check if an error is transient and worth retrying
 */
function isTransientError(error: Error): boolean {
  const message = error.message.toLowerCase();
  
  return (
    message.includes('connection') ||
    message.includes('timeout') ||
    message.includes('warehouse') ||
    message.includes('network') ||
    message.includes('502') ||
    message.includes('503') ||
    message.includes('504')
  );
}

/**
 * Set query tag for observability
 */
export async function setQueryTag(
  conn: snowflake.Connection,
  tag: Record<string, any>
): Promise<void> {
  await exec(conn, "ALTER SESSION SET QUERY_TAG = ?", [JSON.stringify(tag)]);
}

/**
 * Configure session settings for optimal performance and security
 */
export async function configureSession(conn: snowflake.Connection): Promise<void> {
  await exec(conn, `
    ALTER SESSION SET 
      CLIENT_SESSION_KEEP_ALIVE = TRUE,
      STATEMENT_TIMEOUT_IN_SECONDS = 60,
      AUTOCOMMIT = TRUE
  `);
}