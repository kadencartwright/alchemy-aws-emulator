import { Effect, Layer, Context } from 'effect';
import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import { Agent, request } from 'node:https';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { S3Client, ListBucketsCommand } from '@aws-sdk/client-s3';
import { SQSClient } from '@aws-sdk/client-sqs';

export interface Session {
  readonly id: string; readonly endpoint: string; readonly region: string; readonly account: string;
  readonly credentials: { accessKeyId: string; secretAccessKey: string };
  readonly ca: string; readonly s3: S3Client; readonly sqs: SQSClient;
  /** Explicit config for a newly spawned process; never mutate the parent's env. */
  readonly childEnv: NodeJS.ProcessEnv;
}
export class AwsEmulatorSession extends Context.Service<AwsEmulatorSession, Session>()('AwsEmulatorSession') {}
export function assertLocalEndpoint(endpoint: string) {
  const u = new URL(endpoint);
  if (u.protocol !== 'https:' || u.hostname !== '127.0.0.1' || !u.port || u.username || u.password || u.pathname !== '/' || u.search || u.hash)
    throw new Error('Refusing non-local AWS endpoint: expected https://127.0.0.1:<port>');
  return endpoint;
}
export interface SessionOptions { binary?: string; port?: number; region?: string }
const pause = (ms: number) => new Promise(r => setTimeout(r, ms));
async function start(options: SessionOptions) {
  const home = await mkdtemp(join(tmpdir(), 'alchemy-aws-emulator-'));
  const id = randomUUID();
  let child: ChildProcess | undefined;
  let s3: S3Client | undefined, sqs: SQSClient | undefined, agent: Agent | undefined;
  const stop = async () => {
    s3?.destroy(); sqs?.destroy(); agent?.destroy();
    if (child && child.exitCode === null && child.signalCode === null) {
      const exited = new Promise<void>(r => child!.once('exit', () => r()));
      child.kill('SIGTERM');
      const timer = setTimeout(() => child?.kill('SIGKILL'), 3000);
      await exited; clearTimeout(timer);
    }
    await rm(home, { recursive: true, force: true });
  };
  try {
    child = spawn(resolve(options.binary ?? '.local/emulator/opt/aws-emulator/bin/aws-emulator'), ['--port', String(options.port ?? 0), '--bind', '127.0.0.1', '--auth', 'on', '--telemetry', 'off'], {
      env: { PATH: process.env.PATH, HOME: home }, stdio: ['ignore', 'pipe', 'pipe'],
    });
    let endpoint: string | undefined, failure: Error | undefined;
    child.on('error', e => { failure = e; });
    // Consume logs without exposing generated credentials or local machine paths.
    let lines = '';
    const parse = (chunk: Buffer) => {
      lines += chunk.toString();
      let pos: number;
      while ((pos = lines.indexOf('\n')) >= 0) {
        const line = lines.slice(0, pos); lines = lines.slice(pos + 1);
        try { const fields = JSON.parse(line).fields; if (fields?.endpoint_url) endpoint = fields.endpoint_url; } catch {}
      }
    };
    child.stdout!.on('data', parse); child.stderr!.on('data', parse);
    const deadline = Date.now() + 20000;
    while (!endpoint) {
      if (failure) throw failure;
      if (child.exitCode !== null || Date.now() > deadline) throw new Error('Emulator did not start');
      await pause(30);
    }
    assertLocalEndpoint(endpoint);
    const dir = join(home, '.aws/emulator', new URL(endpoint).port);
    const text = await readFile(join(dir, 'emulator_credentials'), 'utf8');
    const value = (name: string) => { const v = text.match(new RegExp(`${name}\\s*=\\s*(\\S+)`))?.[1]; if (!v) throw new Error('Missing generated credentials'); return v; };
    const credentials = { accessKeyId: value('aws_access_key_id'), secretAccessKey: value('aws_secret_access_key') };
    const caPath = join(dir, 'ca.pem'); const ca = await readFile(caPath, 'utf8');
    agent = new Agent({ ca, rejectUnauthorized: true });
    await new Promise<void>((ok, fail) => {
      const req = request(`${endpoint}/__admin/health`, { agent }, res => { res.resume(); res.statusCode === 200 ? ok() : fail(new Error('Health check failed')); });
      req.on('error', fail); req.end();
    });
    const region = options.region ?? 'us-east-1';
    const config = { endpoint, region, credentials, disableHostPrefix: true, maxAttempts: 1, requestHandler: new NodeHttpHandler({ httpsAgent: agent }) };
    s3 = new S3Client({ ...config, forcePathStyle: true }); sqs = new SQSClient({ ...config, useQueueUrlAsEndpoint: false });
    await s3.send(new ListBucketsCommand({}));
    const session: Session = { id, endpoint, region, account: '000000000000', credentials, ca, s3, sqs,
      childEnv: { PATH: process.env.PATH, AWS_ENDPOINT_URL: endpoint, AWS_REGION: region, AWS_DEFAULT_REGION: region,
        AWS_ACCESS_KEY_ID: credentials.accessKeyId, AWS_SECRET_ACCESS_KEY: credentials.secretAccessKey,
        AWS_SHARED_CREDENTIALS_FILE: join(dir, 'emulator_credentials'), AWS_CONFIG_FILE: join(dir, 'config'),
        AWS_CA_BUNDLE: caPath, NODE_EXTRA_CA_CERTS: caPath, AWS_EC2_METADATA_DISABLED: 'true', AWS_DISABLE_HOST_PREFIX_INJECTION: 'true' } };
    return { session, stop };
  } catch (e) { await stop(); throw e; }
}
export const sessionLayer = (options: SessionOptions = {}) => Layer.effect(AwsEmulatorSession,
  Effect.acquireRelease(Effect.tryPromise(() => start(options)), owned => Effect.promise(owned.stop)).pipe(Effect.map(x => x.session)));
