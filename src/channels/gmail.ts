import fs from 'fs';
import os from 'os';
import path from 'path';

import { google, gmail_v1 } from 'googleapis';
import { OAuth2Client } from 'google-auth-library';

import { logger } from '../logger.js';
import { registerChannel, ChannelOpts } from './registry.js';
import {
  Channel,
  OnChatMetadata,
  OnInboundMessage,
  RegisteredGroup,
} from '../types.js';

export interface GmailChannelOpts {
  onMessage: OnInboundMessage;
  onChatMetadata: OnChatMetadata;
  registeredGroups: () => Record<string, RegisteredGroup>;
}

interface ThreadMeta {
  sender: string;
  senderName: string;
  subject: string;
  messageId: string; // RFC 2822 Message-ID for In-Reply-To
  accountName: string; // which account owns this thread
}

/** Per-account Gmail poller */
class GmailAccount {
  readonly accountName: string;
  readonly gmail: gmail_v1.Gmail;
  readonly oauth2Client: OAuth2Client;
  userEmail = '';

  private pollIntervalMs: number;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private processedIds = new Set<string>();
  private consecutiveErrors = 0;

  constructor(
    accountName: string,
    oauth2Client: OAuth2Client,
    gmail: gmail_v1.Gmail,
    pollIntervalMs: number,
  ) {
    this.accountName = accountName;
    this.oauth2Client = oauth2Client;
    this.gmail = gmail;
    this.pollIntervalMs = pollIntervalMs;
  }

  startPolling(onPoll: () => Promise<void>): void {
    const schedulePoll = () => {
      const backoffMs =
        this.consecutiveErrors > 0
          ? Math.min(
              this.pollIntervalMs * Math.pow(2, this.consecutiveErrors),
              30 * 60 * 1000,
            )
          : this.pollIntervalMs;
      this.pollTimer = setTimeout(() => {
        onPoll()
          .catch((err) =>
            logger.error(
              { err, account: this.accountName },
              'Gmail poll error',
            ),
          )
          .finally(() => {
            if (this.gmail) schedulePoll();
          });
      }, backoffMs);
    };
    schedulePoll();
  }

  recordError(): void {
    this.consecutiveErrors++;
  }

  clearErrors(): void {
    this.consecutiveErrors = 0;
  }

  get currentConsecutiveErrors(): number {
    return this.consecutiveErrors;
  }

  addProcessedId(id: string): boolean {
    if (this.processedIds.has(id)) return false;
    this.processedIds.add(id);
    return true;
  }

  trimProcessedIds(): void {
    if (this.processedIds.size > 5000) {
      const ids = [...this.processedIds];
      this.processedIds = new Set(ids.slice(ids.length - 2500));
    }
  }

  stop(): void {
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
  }
}

export class GmailChannel implements Channel {
  name = 'gmail';

  private accounts: GmailAccount[] = [];
  private opts: GmailChannelOpts;
  private pollIntervalMs: number;
  private threadMeta = new Map<string, ThreadMeta>();

  constructor(opts: GmailChannelOpts, pollIntervalMs = 60000) {
    this.opts = opts;
    this.pollIntervalMs = pollIntervalMs;
  }

  async connect(): Promise<void> {
    const credDir = path.join(os.homedir(), '.gmail-mcp');
    const keysPath = path.join(credDir, 'gcp-oauth.keys.json');

    if (!fs.existsSync(keysPath)) {
      logger.warn(
        'Gmail OAuth keys not found in ~/.gmail-mcp/gcp-oauth.keys.json. Skipping Gmail channel.',
      );
      return;
    }

    const keys = JSON.parse(fs.readFileSync(keysPath, 'utf-8'));
    const clientConfig = keys.installed || keys.web || keys;
    const { client_id, client_secret, redirect_uris } = clientConfig;

    // Discover accounts: subdirectories with credentials.json, or root credentials.json
    const accountDirs = this.discoverAccounts(credDir);

    if (accountDirs.length === 0) {
      logger.warn(
        'No Gmail credentials.json found. Skipping Gmail channel. Run /add-gmail to set up.',
      );
      return;
    }

    for (const { name: accountName, tokensPath } of accountDirs) {
      try {
        const tokens = JSON.parse(fs.readFileSync(tokensPath, 'utf-8'));

        const oauth2Client = new google.auth.OAuth2(
          client_id,
          client_secret,
          redirect_uris?.[0],
        );
        oauth2Client.setCredentials(tokens);

        // Persist refreshed tokens
        const tokenFile = tokensPath;
        oauth2Client.on('tokens', (newTokens) => {
          try {
            const current = JSON.parse(fs.readFileSync(tokenFile, 'utf-8'));
            Object.assign(current, newTokens);
            fs.writeFileSync(tokenFile, JSON.stringify(current, null, 2));
            logger.debug(
              { account: accountName },
              'Gmail OAuth tokens refreshed',
            );
          } catch (err) {
            logger.warn(
              { err, account: accountName },
              'Failed to persist refreshed Gmail tokens',
            );
          }
        });

        const gmail = google.gmail({ version: 'v1', auth: oauth2Client });
        const profile = await gmail.users.getProfile({ userId: 'me' });

        const account = new GmailAccount(
          accountName,
          oauth2Client,
          gmail,
          this.pollIntervalMs,
        );
        account.userEmail = profile.data.emailAddress || '';

        logger.info(
          { account: accountName, email: account.userEmail },
          'Gmail account connected',
        );

        this.accounts.push(account);

        // Initial poll + start polling loop
        await this.pollAccount(account);
        account.startPolling(() => this.pollAccount(account));
      } catch (err) {
        logger.error(
          { err, account: accountName },
          'Failed to connect Gmail account',
        );
      }
    }

    if (this.accounts.length > 0) {
      logger.info(
        { count: this.accounts.length },
        'Gmail channel connected with %d account(s)',
        this.accounts.length,
      );
    }
  }

  async sendMessage(jid: string, text: string): Promise<void> {
    const threadId = jid.replace(/^gmail:/, '');
    const meta = this.threadMeta.get(threadId);

    if (!meta) {
      logger.warn({ jid }, 'No thread metadata for reply, cannot send');
      return;
    }

    const account = this.accounts.find(
      (a) => a.accountName === meta.accountName,
    );
    if (!account) {
      logger.warn(
        { jid, account: meta.accountName },
        'Account not found for reply',
      );
      return;
    }

    const subject = meta.subject.startsWith('Re:')
      ? meta.subject
      : `Re: ${meta.subject}`;

    const headers = [
      `To: ${meta.sender}`,
      `From: ${account.userEmail}`,
      `Subject: ${subject}`,
      `In-Reply-To: ${meta.messageId}`,
      `References: ${meta.messageId}`,
      'Content-Type: text/plain; charset=utf-8',
      '',
      text,
    ].join('\r\n');

    const encodedMessage = Buffer.from(headers)
      .toString('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');

    try {
      await account.gmail.users.messages.send({
        userId: 'me',
        requestBody: {
          raw: encodedMessage,
          threadId,
        },
      });
      logger.info(
        { to: meta.sender, threadId, account: account.accountName },
        'Gmail reply sent',
      );
    } catch (err) {
      logger.error({ jid, err }, 'Failed to send Gmail reply');
    }
  }

  isConnected(): boolean {
    return this.accounts.length > 0;
  }

  ownsJid(jid: string): boolean {
    return jid.startsWith('gmail:');
  }

  async disconnect(): Promise<void> {
    for (const account of this.accounts) {
      account.stop();
    }
    this.accounts = [];
    logger.info('Gmail channel stopped');
  }

  // --- Private ---

  private discoverAccounts(
    credDir: string,
  ): Array<{ name: string; tokensPath: string }> {
    const accounts: Array<{ name: string; tokensPath: string }> = [];

    // Check subdirectories for multi-account setup
    try {
      const entries = fs.readdirSync(credDir, { withFileTypes: true });
      for (const entry of entries) {
        if (!entry.isDirectory()) continue;
        const tokensPath = path.join(credDir, entry.name, 'credentials.json');
        if (fs.existsSync(tokensPath)) {
          accounts.push({ name: entry.name, tokensPath });
        }
      }
    } catch {
      // Directory read failed, fall through to legacy check
    }

    // Fall back to legacy single-account setup (credentials.json in root)
    if (accounts.length === 0) {
      const legacyTokens = path.join(credDir, 'credentials.json');
      if (fs.existsSync(legacyTokens)) {
        accounts.push({ name: 'default', tokensPath: legacyTokens });
      }
    }

    return accounts;
  }

  private buildQuery(): string {
    return 'is:unread category:primary';
  }

  private async pollAccount(account: GmailAccount): Promise<void> {
    try {
      const query = this.buildQuery();
      const res = await account.gmail.users.messages.list({
        userId: 'me',
        q: query,
        maxResults: 10,
      });

      const messages = res.data.messages || [];

      for (const stub of messages) {
        if (!stub.id || !account.addProcessedId(stub.id)) continue;
        await this.processMessage(account, stub.id);
      }

      account.trimProcessedIds();
      account.clearErrors();
    } catch (err) {
      account.recordError();
      logger.error(
        {
          err,
          account: account.accountName,
          consecutiveErrors: account.currentConsecutiveErrors,
        },
        'Gmail poll failed',
      );
    }
  }

  private async processMessage(
    account: GmailAccount,
    messageId: string,
  ): Promise<void> {
    const msg = await account.gmail.users.messages.get({
      userId: 'me',
      id: messageId,
      format: 'full',
    });

    const headers = msg.data.payload?.headers || [];
    const getHeader = (name: string) =>
      headers.find((h) => h.name?.toLowerCase() === name.toLowerCase())
        ?.value || '';

    const from = getHeader('From');
    const subject = getHeader('Subject');
    const rfc2822MessageId = getHeader('Message-ID');
    const threadId = msg.data.threadId || messageId;
    const timestamp = new Date(
      parseInt(msg.data.internalDate || '0', 10),
    ).toISOString();

    // Extract sender name and email
    const senderMatch = from.match(/^(.+?)\s*<(.+?)>$/);
    const senderName = senderMatch ? senderMatch[1].replace(/"/g, '') : from;
    const senderEmail = senderMatch ? senderMatch[2] : from;

    // Skip emails from self (our own replies)
    if (senderEmail === account.userEmail) return;

    // Extract body text
    const body = this.extractTextBody(msg.data.payload);

    if (!body) {
      logger.debug({ messageId, subject }, 'Skipping email with no text body');
      return;
    }

    const chatJid = `gmail:${threadId}`;

    // Cache thread metadata for replies
    this.threadMeta.set(threadId, {
      sender: senderEmail,
      senderName,
      subject,
      messageId: rfc2822MessageId,
      accountName: account.accountName,
    });

    // Store chat metadata for group discovery
    this.opts.onChatMetadata(chatJid, timestamp, subject, 'gmail', false);

    // Find the main group to deliver the email notification
    const groups = this.opts.registeredGroups();
    const mainEntry = Object.entries(groups).find(([, g]) => g.isMain === true);

    if (!mainEntry) {
      logger.debug(
        { chatJid, subject },
        'No main group registered, skipping email',
      );
      return;
    }

    const mainJid = mainEntry[0];
    const accountLabel =
      this.accounts.length > 1 ? ` (${account.userEmail})` : '';
    const content = `[Email from ${senderName} <${senderEmail}>${accountLabel}]\nSubject: ${subject}\n\n${body}`;

    this.opts.onMessage(mainJid, {
      id: messageId,
      chat_jid: mainJid,
      sender: senderEmail,
      sender_name: senderName,
      content,
      timestamp,
      is_from_me: false,
    });

    // Mark as read
    try {
      await account.gmail.users.messages.modify({
        userId: 'me',
        id: messageId,
        requestBody: { removeLabelIds: ['UNREAD'] },
      });
    } catch (err) {
      logger.warn({ messageId, err }, 'Failed to mark email as read');
    }

    logger.info(
      { mainJid, from: senderName, subject, account: account.accountName },
      'Gmail email delivered to main group',
    );
  }

  private extractTextBody(
    payload: gmail_v1.Schema$MessagePart | undefined,
  ): string {
    if (!payload) return '';

    // Direct text/plain body
    if (payload.mimeType === 'text/plain' && payload.body?.data) {
      return Buffer.from(payload.body.data, 'base64').toString('utf-8');
    }

    // Multipart: search parts recursively
    if (payload.parts) {
      // Prefer text/plain
      for (const part of payload.parts) {
        if (part.mimeType === 'text/plain' && part.body?.data) {
          return Buffer.from(part.body.data, 'base64').toString('utf-8');
        }
      }
      // Recurse into nested multipart
      for (const part of payload.parts) {
        const text = this.extractTextBody(part);
        if (text) return text;
      }
    }

    return '';
  }
}

registerChannel('gmail', (opts: ChannelOpts) => {
  const credDir = path.join(os.homedir(), '.gmail-mcp');
  if (!fs.existsSync(path.join(credDir, 'gcp-oauth.keys.json'))) {
    logger.warn('Gmail: OAuth keys not found in ~/.gmail-mcp/');
    return null;
  }
  return new GmailChannel(opts);
});
