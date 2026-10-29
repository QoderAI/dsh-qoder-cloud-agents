/**
 * Qoder Cloud Agents Managed API (`https://api.qoder.com/api/v1/cloud`) through
 * the official Qoder Cloud Agents TypeScript SDK's `ManagedClient`: environments,
 * agents, sessions, session events and the session event stream. This module
 * narrows the SDK to the calls the plugin makes and keeps one error shape for
 * the rest of it.
 *
 * @module dsh-qoder-cloud-agents/client
 */

import { randomUUID } from 'node:crypto'
import {
  APIError,
  ManagedClient,
  type AgentCreateParams,
  type ManagedAgentsEventParamsUnion,
  type SessionCreateParams,
  type SessionNewParamsResourceUnion,
} from 'qoder-cloud-agents-sdk/managed'

/** Default public gateway. */
export const DEFAULT_BASE_URL = 'https://api.qoder.com/api/v1/cloud'

/** Built-in sandbox toolset (Bash, Read, Write, Edit, Glob, Grep, WebFetch, …). */
export const AGENT_TOOLSET = 'agent_toolset_20260401'

/** `{type, message}` from the platform error envelope, with the HTTP status and request id. */
export class QoderCloudError extends Error {
  constructor(
    readonly status: number,
    readonly errorType: string,
    message: string,
    readonly requestId?: string,
    options?: ErrorOptions,
  ) {
    super(`Qoder Cloud ${status} ${errorType}: ${message}${requestId === undefined ? '' : ` (request_id ${requestId})`}`, options)
    this.name = 'QoderCloudError'
  }
}

export interface CursorPage<T> {
  readonly data: T[]
  readonly has_more?: boolean
}

export interface EnvironmentView {
  readonly id: string
  readonly name: string
  readonly archived_at?: string | null
  /** `{type: 'cloud' | 'self_hosted', networking?, packages?, setup_script?}`. */
  readonly config?: { readonly type?: string } | null
}

export interface AgentView {
  readonly id: string
  readonly name: string
  /** A model id string or `{id, effort?, speed?}`. */
  readonly model?: unknown
  readonly version?: number
  readonly archived_at?: string | null
}

export interface SessionView {
  readonly id: string
  readonly status: 'idle' | 'running' | 'rescheduling' | 'terminated' | string
  readonly title?: string | null
}

/** One public session event; only the fields this plugin reads are typed. */
export interface SessionEvent {
  readonly id: string
  readonly type: string
  readonly content?: unknown
  readonly stop_reason?: { readonly type?: string; readonly event_ids?: readonly string[] } | null
  readonly error?: { readonly type?: string; readonly message?: string; readonly retry_status?: { readonly type?: string } } | null
  readonly name?: string
  readonly file_id?: string
  readonly original_filename?: string
  readonly size?: number
  readonly [key: string]: unknown
}

/** `github_repository` carries the token as `authorization_token`; `git_repository` puts the account in the URL and the token in `password`. */
export type SessionResource = SessionNewParamsResourceUnion
export type CreateSessionRequest = SessionCreateParams
export type CreateAgentRequest = AgentCreateParams

export interface QoderCloudClientOptions {
  baseURL: string
  token: string
  headers?: Record<string, string>
  /** Injected for tests. */
  fetch?: typeof fetch
  /** Retries for 408 / 429 / 5xx and connection errors on GETs and idempotency-keyed POSTs. */
  maxRetries?: number
}

export class QoderCloudClient {
  private readonly sdk: ManagedClient

  constructor(options: QoderCloudClientOptions) {
    this.sdk = new ManagedClient({
      pat: options.token,
      baseURL: options.baseURL.replace(/\/+$/, ''),
      maxRetries: options.maxRetries ?? 3,
      ...options.headers === undefined ? {} : { defaultHeaders: options.headers },
      ...options.fetch === undefined ? {} : { fetch: options.fetch },
    })
  }

  listEnvironments(signal?: AbortSignal): Promise<CursorPage<EnvironmentView>> {
    return this.call(signal, async () => ({ data: await collect(this.sdk.environments.list({ limit: 100 }, { signal: signal ?? null })) }))
  }

  createEnvironment(name: string, signal?: AbortSignal): Promise<EnvironmentView> {
    return this.call(signal, () => this.sdk.environments.create({
      name,
      config: { type: 'cloud', networking: { type: 'unrestricted' } },
    }, post(signal)))
  }

  listAgents(signal?: AbortSignal): Promise<CursorPage<AgentView>> {
    return this.call(signal, async () => ({ data: await collect(this.sdk.agents.list({ limit: 100 }, { signal: signal ?? null })) }))
  }

  createAgent(body: CreateAgentRequest, signal?: AbortSignal): Promise<AgentView> {
    return this.call(signal, () => this.sdk.agents.create(body, post(signal)))
  }

  createSession(body: CreateSessionRequest, signal?: AbortSignal): Promise<SessionView> {
    return this.call(signal, () => this.sdk.sessions.create(body, post(signal)))
  }

  getSession(id: string, signal?: AbortSignal): Promise<SessionView> {
    return this.call(signal, () => this.sdk.sessions.retrieve(id, {}, { signal: signal ?? null }))
  }

  sendEvents(id: string, events: readonly Record<string, unknown>[], signal?: AbortSignal): Promise<{ data: SessionEvent[] }> {
    return this.call(signal, async () => {
      const sent = await this.sdk.sessions.events.send(id, { events: events as unknown as ManagedAgentsEventParamsUnion[] }, post(signal))
      return { data: (sent.data ?? []) as unknown as SessionEvent[] }
    })
  }

  cancelSession(id: string, signal?: AbortSignal): Promise<unknown> {
    return this.call(signal, () => this.sdk.sessions.cancel(id, {}, post(signal)))
  }

  archiveSession(id: string, signal?: AbortSignal): Promise<unknown> {
    return this.call(signal, () => this.sdk.sessions.archive(id, {}, post(signal)))
  }

  /**
   * Open the session event stream once. Without `lastEventId` the server
   * replays the whole history first; with it, delivery resumes strictly after
   * that event. The stream never ends on idle: the caller stops reading.
   * Reconnecting stays with the caller, which also re-checks the session.
   * @param id - session id.
   * @param lastEventId - resume point (`Last-Event-ID`).
   * @param signal - aborts the underlying request.
   */
  async *streamEvents(id: string, lastEventId: string | undefined, signal: AbortSignal): AsyncGenerator<SessionEvent> {
    const stream = await this.call(signal, () => this.sdk.sessions.events.streamEvents(
      id,
      lastEventId === undefined ? {} : { last_event_id: lastEventId },
      { signal, maxRetries: 0 },
    ))
    try {
      for await (const event of stream) {
        const view = event as unknown as SessionEvent
        yield { ...view, id: view.id ?? stream.lastEventID }
      }
    } catch (error: unknown) {
      throw failure(error, signal)
    } finally {
      await stream.close()
    }
  }

  /** Run one SDK call; caller aborts surface as the signal's reason, API errors as {@link QoderCloudError}. */
  private async call<T>(signal: AbortSignal | undefined, run: () => Promise<T>): Promise<T> {
    signal?.throwIfAborted()
    try {
      return await run()
    } catch (error: unknown) {
      throw failure(error, signal)
    }
  }
}

/** One idempotency key per logical POST, reused across the SDK's retries, so a retried POST that did land is not applied twice. */
function post(signal: AbortSignal | undefined): { signal: AbortSignal | null; idempotencyKey: string } {
  return { signal: signal ?? null, idempotencyKey: randomUUID() }
}

async function collect<T>(items: AsyncIterable<T>): Promise<T[]> {
  const all: T[] = []
  for await (const item of items) all.push(item)
  return all
}

function failure(error: unknown, signal: AbortSignal | undefined): unknown {
  if (signal?.aborted) return signal.reason
  if (!(error instanceof APIError) || error.status === undefined) return error
  return new QoderCloudError(error.status, error.type ?? 'api_error', error.message, error.request_id ?? undefined, { cause: error })
}

/** Abortable sleep. */
export function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(signal.reason)
      return
    }
    const onAbort = (): void => {
      clearTimeout(timer)
      reject(signal?.reason)
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}
