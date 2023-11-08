/* eslint-disable max-lines */
// Import Node.js Dependencies
import { once, EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import timers from "node:timers/promises";

// Import Third-party Dependencies
import {
  Channel,
  getRedis
} from "@myunisoft/redis";
import { Logger, pino } from "pino";
import Ajv, { ValidateFunction } from "ajv";
import { match } from "ts-pattern";

// Import Internal Dependencies
import {
  channels
} from "../../utils/config";
import {
  PartialTransaction,
  Transaction,
  Transactions,
  TransactionStore
} from "../store/transaction.class";
import {
  Prefix,
  DispatcherChannelMessages,
  IncomerChannelMessages,
  DispatcherRegistrationMessage,
  IncomerRegistrationMessage,
  DispatcherPingMessage,
  DistributedEventMessage,
  EventMessage,
  GenericEvent
} from "../../types/eventManagement/index";
import * as eventsSchema from "../../schema/eventManagement/index";
import { CustomEventsValidationFunctions, defaultStandardLog, StandardLog } from "../../utils/index";
import { IncomerStore, RegisteredIncomer } from "../store/incomer.class";

// CONSTANTS
const ajv = new Ajv();
const kIdleTime = 60_000 * 10;
const kCheckLastActivityInterval = 60_000 * 2;
const kCheckRelatedTransactionInterval = 60_000 * 3;
const kBackupTransactionStoreName = "backup";
const kCancelTimeout = new AbortController();
const kCancelTask = new AbortController();
const kSilentLogger = process.env.MYUNISOFT_EVENTS_SILENT_LOGGER || false;
export const PING_INTERVAL = 60_000 * 5;

export type DispatcherOptions<T extends GenericEvent = GenericEvent> = {
  /* Prefix for the channel name, commonly used to distinguish envs */
  prefix?: Prefix;
  logger?: Partial<Logger> & Pick<Logger, "info" | "warn">;
  standardLog?: StandardLog<T>;
  eventsValidation?: {
    eventsValidationFn?: Map<string, ValidateFunction<Record<string, any>> | CustomEventsValidationFunctions>;
    validationCbFn?: (event: T) => void;
  };
  pingInterval?: number;
  checkLastActivityInterval?: number;
  checkTransactionInterval?: number;
  idleTime?: number;
  /** Used to avoid self ping & as discriminant for dispatcher instance that scale */
  incomerUUID?: string;
  /** Used as discriminant for dispatcher instance that scale */
  instanceName?: string;
};

type DispatcherChannelEvents = { name: "register" };

function isDispatcherChannelMessage<T extends GenericEvent = GenericEvent>(
  value: DispatcherChannelMessages["IncomerMessages"] |
  IncomerChannelMessages<T>["IncomerMessages"]
): value is DispatcherChannelMessages["IncomerMessages"] {
  return value.name === "register";
}

function isIncomerChannelMessage<T extends GenericEvent = GenericEvent>(
  value: DispatcherChannelMessages["IncomerMessages"] |
  IncomerChannelMessages<T>["IncomerMessages"]
): value is IncomerChannelMessages<T>["IncomerMessages"] {
  return value.name !== "register" && value.name !== "ping";
}

function isIncomerRegistrationMessage(
  value: DispatcherChannelMessages["IncomerMessages"]
): value is IncomerRegistrationMessage {
  return value.name === "register";
}

export class Dispatcher<T extends GenericEvent = GenericEvent> extends EventEmitter {
  readonly type = "dispatcher";
  readonly formattedPrefix: string;
  readonly prefix: string;
  readonly dispatcherChannelName: string;
  readonly privateUUID = randomUUID();

  private selfProvidedUUID: string;
  private instanceName: string | undefined;
  private isWorking = false;
  private dispatcherChannel: Channel<
    DispatcherChannelMessages["DispatcherMessages"] | { name: "OK", redisMetadata: { origin: string } }
  >;
  private incomerStore: IncomerStore;
  private dispatcherTransactionStore: TransactionStore<"dispatcher">;
  private backupIncomerTransactionStore: TransactionStore<"incomer">;
  private backupDispatcherTransactionStore: TransactionStore<"dispatcher">;

  private logger: Partial<Logger> & Pick<Logger, "info" | "warn">;
  private incomerChannels: Map<string,
    Channel<IncomerChannelMessages<T>["DispatcherMessages"] | DistributedEventMessage<T>>> = new Map();

  private pingInterval: number;
  private pingIntervalTimer: NodeJS.Timer;
  private checkLastActivityInterval: number;
  private checkLastActivityIntervalTimer: NodeJS.Timer;
  private checkRelatedTransactionInterval: number;
  private checkRelatedTransactionIntervalTimer: NodeJS.Timer;
  private checkDispatcherStateInterval: NodeJS.Timer;
  private resetCheckLastActivityTimeout: NodeJS.Timer;
  private idleTime: number;
  private minTimeout = 0;
  private maxTimeout = 60_000;

  private eventsValidationFn: Map<string, ValidateFunction<Record<string, any>> | CustomEventsValidationFunctions>;
  private validationCbFn: (event: T) => void = null;
  private standardLogFn: StandardLog<T>;

  constructor(options: DispatcherOptions<T>) {
    super();

    Object.assign(this, options);

    this.selfProvidedUUID = options.incomerUUID;
    this.prefix = options.prefix ?? "";
    this.formattedPrefix = options.prefix ? `${options.prefix}-` : "";
    this.dispatcherChannelName = this.formattedPrefix + channels.dispatcher;
    this.standardLogFn = options.standardLog ?? defaultStandardLog;
    this.idleTime = options.idleTime ?? kIdleTime;
    this.pingInterval = options.pingInterval ?? PING_INTERVAL;
    this.checkRelatedTransactionInterval = options.checkTransactionInterval ?? kCheckRelatedTransactionInterval;
    this.checkLastActivityInterval = options.checkLastActivityInterval ?? kCheckLastActivityInterval;

    this.eventsValidationFn = options?.eventsValidation?.eventsValidationFn ?? new Map();
    this.validationCbFn = options?.eventsValidation?.validationCbFn;

    for (const [name, validationSchema] of Object.entries(eventsSchema)) {
      this.eventsValidationFn.set(name, ajv.compile(validationSchema));
    }

    this.logger = options.logger ?? pino({
      name: this.formattedPrefix + this.type,
      level: kSilentLogger ? "silent" : "info",
      transport: {
        target: "pino-pretty"
      }
    });

    this.incomerStore = new IncomerStore({
      prefix: this.prefix
    });

    this.backupIncomerTransactionStore = new TransactionStore({
      prefix: this.formattedPrefix + kBackupTransactionStoreName,
      instance: "incomer"
    });

    this.backupDispatcherTransactionStore = new TransactionStore({
      prefix: this.formattedPrefix + kBackupTransactionStoreName,
      instance: "dispatcher"
    });

    this.dispatcherTransactionStore = new TransactionStore({
      prefix: this.prefix,
      instance: "dispatcher"
    });

    this.dispatcherChannel = new Channel({
      prefix: this.prefix,
      name: channels.dispatcher
    });

    this.pingIntervalTimer = setInterval(async() => {
      try {
        if (!this.isWorking) {
          return;
        }

        await this.ping();
      }
      catch (error) {
        this.logger.error(error.message);
      }
    }, this.pingInterval).unref();

    this.checkLastActivityIntervalTimer = this.checkLastActivityIntervalFn();

    this.checkRelatedTransactionIntervalTimer = setInterval(async() => {
      try {
        if (!this.isWorking) {
          return;
        }

        const dispatcherTransactions = await this.dispatcherTransactionStore.getTransactions();
        const backupDispatcherTransactions = await this.backupDispatcherTransactionStore.getTransactions();

        await this.resolveDispatcherTransactions(
          dispatcherTransactions,
          backupDispatcherTransactions
        );

        await this.resolveIncomerMainTransactions(
          dispatcherTransactions,
          backupDispatcherTransactions
        );
      }
      catch (error) {
        this.logger.error(error.message);
      }
    }, this.checkRelatedTransactionInterval).unref();
  }

  get subscriber() {
    return getRedis("subscriber");
  }

  private async takeRelay() {
    const incomers = await this.incomerStore.getIncomers();

    const now = Date.now();

    let toRemove: RegisteredIncomer;
    for (const incomer of incomers) {
      if (
        incomer.name === this.instanceName &&
        incomer.baseUUID !== this.selfProvidedUUID &&
        incomer.isDispatcherActiveInstance
      ) {
        if (now > incomer.lastActivity + this.idleTime) {
          toRemove = incomer;
        }
        else {
          return;
        }
      }
    }

    let aborted = false;
    kCancelTask.signal.addEventListener("abort", () => {
      this.logger.warn({ error: kCancelTask.signal.reason });

      aborted = true;
    }, { once: true });

    await Promise.race([
      this.updateDispatcherStateTimeout(),
      this.updateDispatcherState()
    ]);

    clearInterval(this.checkLastActivityIntervalTimer);

    setImmediate((async() => {
      if (aborted) {
        return;
      }

      this.isWorking = true;

      try {
        await Promise.all([this.ping(), toRemove && this.removeNonActives([toRemove])]);
      }
      catch (error) {
        this.logger.error(error.message);

        return;
      }

      this.resetCheckLastActivityTimeout = setTimeout(async() => {
        try {
          const dispatcherTransactions = await this.dispatcherTransactionStore.getTransactions();
          const backupDispatcherTransactions = await this.backupDispatcherTransactionStore.getTransactions();

          await this.resolveDispatcherTransactions(
            dispatcherTransactions,
            backupDispatcherTransactions
          );

          this.checkLastActivityIntervalTimer = this.checkLastActivityIntervalFn();
        }
        catch (error) {
          this.logger.error(error.message);
        }
      }, this.checkRelatedTransactionInterval).unref();

      if (this.checkDispatcherStateInterval) {
        clearInterval(this.checkDispatcherStateInterval);
        this.checkDispatcherStateInterval = undefined;
      }

      for (const { providedUUID, prefix } of incomers) {
        await this.subscriber.subscribe(`${prefix ? `${prefix}-` : ""}${providedUUID}`);
      }

      this.logger.info(`Dispatcher ${this.selfProvidedUUID} took relay on ${toRemove?.baseUUID}`);
    }));
  }

  public async initialize() {
    await this.subscriber.subscribe(this.dispatcherChannelName);
    this.subscriber.on("message", (channel, message) => this.handleMessages(channel, message));

    const incomers = await this.incomerStore.getIncomers();
    const now = Date.now();

    for (const incomer of incomers) {
      if (
        incomer.name === this.instanceName && (
          incomer.baseUUID !== this.selfProvidedUUID
        ) && !(
          now > incomer.lastActivity + this.idleTime
        )) {
        this.checkDispatcherStateInterval = setInterval(async() => await this.takeRelay(), this.pingInterval).unref();

        return;
      }
    }

    let aborted = false;
    kCancelTask.signal.addEventListener("abort", () => {
      this.logger.warn({ error: kCancelTask.signal.reason });

      aborted = true;
    }, { once: true });

    await Promise.race([
      this.updateDispatcherStateTimeout(),
      async() => {
        await timers.setTimeout(this.randomIntFromRange());
        await this.dispatcherChannel.publish({ name: "OK", redisMetadata: { origin: this.privateUUID } });
        kCancelTimeout.abort();
      }
    ]);

    setImmediate((async() => {
      if (!aborted) {
        this.isWorking = true;

        try {
          await this.ping();
        }
        catch (error) {
          this.logger.error(error);
        }

        return;
      }

      this.checkDispatcherStateInterval = setInterval(async() => await this.takeRelay(), this.pingInterval).unref();
    }));
  }

  public async close() {
    clearInterval(this.pingIntervalTimer);
    this.pingIntervalTimer = undefined;

    clearInterval(this.checkRelatedTransactionIntervalTimer);
    this.checkRelatedTransactionIntervalTimer = undefined;

    clearInterval(this.checkLastActivityIntervalTimer);
    this.checkLastActivityIntervalTimer = undefined;

    if (this.resetCheckLastActivityTimeout) {
      clearTimeout(this.resetCheckLastActivityTimeout);
      this.resetCheckLastActivityTimeout = undefined;
    }

    if (this.checkDispatcherStateInterval) {
      clearInterval(this.checkDispatcherStateInterval);
      this.checkDispatcherStateInterval = undefined;
    }

    await this.subscriber.unsubscribe(this.dispatcherChannelName, ...this.incomerChannels.keys());

    this.isWorking = false;
  }

  private randomIntFromRange() {
    return Math.floor((Math.random() * (this.maxTimeout - this.minTimeout)) + this.minTimeout);
  }

  private checkLastActivityIntervalFn() {
    return setInterval(async() => {
      try {
        if (!this.isWorking) {
          return;
        }

        await this.checkLastActivity();
      }
      catch (error) {
        this.logger.error(error);
      }
    }, this.checkLastActivityInterval).unref();
  }

  private async updateDispatcherState() {
    try {
      await timers.setTimeout(this.randomIntFromRange());
      await this.setAsActiveDispatcher();
      await this.dispatcherChannel.publish({ name: "OK", redisMetadata: { origin: this.privateUUID } });
    }
    finally {
      kCancelTimeout.abort();
    }
  }

  private async updateDispatcherStateTimeout() {
    try {
      await once(this, "OK", { signal: kCancelTimeout.signal });
      kCancelTask.abort("Timed out on dispatcher retake");
    }
    catch {
      // Ignore
    }
  }

  private async ping() {
    const incomers = await this.incomerStore.getIncomers();
    const pingToResolve = [];
    const concernedIncomers: string[] = [];
    for (const incomer of incomers) {
      if (incomer === null) {
        continue;
      }

      const { providedUUID: uuid } = incomer;

      if (incomer.baseUUID === this.selfProvidedUUID) {
        pingToResolve.push(this.updateIncomerState(uuid));

        continue;
      }

      const incomerChannel = this.incomerChannels.get(uuid) ??
        new Channel({
          name: uuid,
          prefix: incomer.prefix
        });

      const event: Omit<DispatcherPingMessage, "redisMetadata"> & {
        redisMetadata: Omit<DispatcherPingMessage["redisMetadata"], "transactionId">
      } = {
        name: "ping",
        data: null,
        redisMetadata: {
          origin: this.privateUUID,
          incomerName: "dispatcher",
          to: uuid
        }
      };

      concernedIncomers.push(uuid);

      pingToResolve.push(this.publishEvent({
        concernedChannel: incomerChannel,
        transactionMeta: {
          mainTransaction: true,
          eventTransactionId: null,
          relatedTransaction: null,
          resolved: false
        },
        formattedEvent: event
      }));
    }

    await Promise.all(pingToResolve);
    if (concernedIncomers.length > 0) {
      this.logger.info({ incomers: concernedIncomers }, "New Ping events");
    }
  }

  private async removeNonActives(inactiveIncomers: RegisteredIncomer[]) {
    try {
      const toHandle = [];

      for (const inactive of inactiveIncomers) {
        const transactionStore = new TransactionStore({
          prefix: `${inactive.prefix ? `${inactive.prefix}-` : ""}${inactive.providedUUID}`,
          instance: "incomer"
        });

        toHandle.push(Promise.all([
          this.incomerStore.deleteIncomer(inactive.providedUUID),
          this.InactiveIncomerTransactionsResolution({
            incomerToRemove: inactive,
            incomerTransactionStore: transactionStore
          })
        ]));
      }

      await Promise.all(toHandle);
    }
    catch (error) {
      throw new Error(`
        ${{ uuids: [...inactiveIncomers.map((incomer) => incomer?.providedUUID)].join(",") }}, Failed to remove nonactives incomers
      `);
    }
  }

  private async checkLastActivity() {
    const incomers = await this.incomerStore.getIncomers();

    const now = Date.now();

    const nonActives = [...incomers].filter((incomer) => now > incomer.lastActivity + this.idleTime);

    if (nonActives.length === 0) {
      return;
    }

    const toResolve = [];
    let index = 0;
    for (const inactive of nonActives) {
      const transactionStore = new TransactionStore({
        prefix: `${inactive.prefix ? `${inactive.prefix}-` : ""}${inactive.providedUUID}`,
        instance: "incomer"
      });

      const transactions = await transactionStore.getTransactions();
      const recentPingTransactionKeys = Object.keys(await transactionStore.getTransactions())
        .filter((transactionKey) => {
          const transaction = transactions.get(transactionKey);

          return transaction.name === "ping" && now < transaction.aliveSince + this.idleTime;
        });

      if (recentPingTransactionKeys.length > 0) {
        toResolve.push(Promise.all([
          this.updateIncomerState(inactive.providedUUID),
          transactionStore.redis.del(recentPingTransactionKeys)
        ]));

        nonActives.splice(index, 1);
      }

      index++;
    }

    await Promise.all(toResolve);

    await this.removeNonActives(nonActives);

    this.logger.info({ uuids: [...nonActives.map((incomer) => incomer.providedUUID)].join(",") }, "Removed nonactives incomers");
  }

  private async publishEvent(options: {
    concernedStore?: TransactionStore<"incomer">;
    concernedChannel: Channel<
      DispatcherChannelMessages["DispatcherMessages"] |
      (IncomerChannelMessages<T>["DispatcherMessages"] | DistributedEventMessage<T>)
    >;
    transactionMeta: {
      mainTransaction: boolean;
      relatedTransaction: null | string;
      eventTransactionId: null | string;
      resolved: boolean;
    };
    formattedEvent: any;
  }) {
    const {
      concernedChannel,
      transactionMeta,
      formattedEvent
    } = options;
    const {
      mainTransaction,
      relatedTransaction,
      eventTransactionId,
      resolved
    } = transactionMeta;

    const concernedStore = options.concernedStore ?? this.dispatcherTransactionStore;

    const transaction = await concernedStore.setTransaction({
      ...formattedEvent,
      redisMetadata: {
        ...formattedEvent.redisMetadata,
        mainTransaction,
        relatedTransaction,
        resolved
      }
    });

    await concernedChannel.publish({
      ...formattedEvent,
      redisMetadata: {
        ...formattedEvent.redisMetadata,
        eventTransactionId,
        transactionId: transaction.redisMetadata.transactionId
      }
    });
  }

  private async InactiveIncomerTransactionsResolution(options: {
    incomerToRemove: RegisteredIncomer,
    incomerTransactionStore: TransactionStore<"incomer">
  }
  ) {
    const {
      incomerToRemove,
      incomerTransactionStore
    } = options;

    const incomerTransactions = await incomerTransactionStore.getTransactions();
    let dispatcherTransactions = await this.dispatcherTransactionStore.getTransactions();

    const incomers = await this.incomerStore.getIncomers();

    delete incomers[incomerToRemove.providedUUID];

    const incomerMainTransactions = new Map([...incomerTransactions.entries()]
      .filter(([, transaction]) => transaction.redisMetadata.mainTransaction));

    for (const [transactionId, mainTransaction] of incomerMainTransactions.entries()) {
      if (mainTransaction.name === "register") {
        const approvementRelatedTransaction = Object.keys(dispatcherTransactions)
          .find(
            (dispatcherTransactionId) => dispatcherTransactions[dispatcherTransactionId].redisMetadata.relatedTransaction ===
            transactionId
          );

        await Promise.all([
          typeof approvementRelatedTransaction === "undefined" ? () => void 0 :
            this.dispatcherTransactionStore.deleteTransaction(approvementRelatedTransaction),
          incomerTransactionStore.deleteTransaction(transactionId)
        ]);

        continue;
      }

      const concernedMainTransactionIncomer = [...incomers].find(
        (incomer) => incomer.name === incomerToRemove.name && incomer.eventsCast.find(
          (eventCast) => eventCast === mainTransaction.name
        )
      );

      if (!concernedMainTransactionIncomer) {
        const [, newlyTransaction] = await Promise.all([
          incomerTransactionStore.deleteTransaction(transactionId),
          this.backupIncomerTransactionStore.setTransaction({
            ...mainTransaction
          })
        ]);

        this.logger.info(this.standardLogFn(newlyTransaction as any)("Main transaction has been backup"));

        continue;
      }

      const { prefix, providedUUID } = concernedMainTransactionIncomer;

      const concernedIncomerStore = new TransactionStore({
        prefix: `${prefix ? `${prefix}-` : ""}${providedUUID}`,
        instance: "incomer"
      });

      const [newlyIncomerTransaction] = await Promise.all([
        concernedIncomerStore.setTransaction({
          ...mainTransaction,
          redisMetadata: {
            ...mainTransaction.redisMetadata,
            origin: providedUUID
          }
        }),
        incomerTransactionStore.deleteTransaction(transactionId)
      ]);

      const relatedDispatcherTransactions = [...dispatcherTransactions.values()]
        .filter(
          (dispatcherTransaction) => dispatcherTransaction.redisMetadata.relatedTransaction ===
          mainTransaction.redisMetadata.transactionId
        );

      await Promise.all(relatedDispatcherTransactions.map(
        (dispatcherTransaction) => this.dispatcherTransactionStore.updateTransaction(
          dispatcherTransaction.redisMetadata.transactionId,
          {
            ...dispatcherTransaction,
            redisMetadata: {
              ...dispatcherTransaction.redisMetadata,
              mainTransaction: false,
              to: providedUUID,
              relatedTransaction: newlyIncomerTransaction.redisMetadata.transactionId
            }
          }
        )
      ));

      this.logger.info(this.standardLogFn(newlyIncomerTransaction as any)("Main transaction redistributed  to an Incomer"));
    }

    dispatcherTransactions = await this.dispatcherTransactionStore.getTransactions();

    for (const [incomerTransactionId, incomerTransaction] of incomerTransactions.entries()) {
      // Remove possible ping response
      if (incomerTransaction.name === "ping") {
        await Promise.all([
          incomerTransactionStore.deleteTransaction(incomerTransactionId),
          this.dispatcherTransactionStore.deleteTransaction(incomerTransaction.redisMetadata.relatedTransaction)
        ]);

        continue;
      }

      if (incomerTransaction.redisMetadata.mainTransaction) {
        continue;
      }

      if (incomerTransaction.redisMetadata.relatedTransaction) {
        const concernedRelatedTransactionIncomer = [...incomers].find(
          (incomer) => incomer.name === incomerTransaction.redisMetadata.incomerName && incomer.eventsSubscribe.find(
            (subscribedEvent) => subscribedEvent.name === incomerTransaction.name
          )
        );

        if (!concernedRelatedTransactionIncomer) {
          if (incomerTransaction.redisMetadata.resolved) {
            await incomerTransactionStore.deleteTransaction(incomerTransactionId);

            continue;
          }

          const [, newlyTransaction] = await Promise.all([
            incomerTransactionStore.deleteTransaction(incomerTransactionId),
            this.backupIncomerTransactionStore.setTransaction({
              ...incomerTransaction
            })
          ]);

          this.logger.info(this.standardLogFn(newlyTransaction as any)("Spread transaction has been backup"));

          continue;
        }

        const { providedUUID, prefix } = concernedRelatedTransactionIncomer;

        let concernedIncomerChannel = this.incomerChannels.get(providedUUID);

        if (!concernedIncomerChannel) {
          concernedIncomerChannel = new Channel({
            name: providedUUID,
            prefix
          });

          this.incomerChannels.set(providedUUID, concernedIncomerChannel);
        }

        const relatedDispatcherTransaction = dispatcherTransactions.get(incomerTransaction.redisMetadata.relatedTransaction);

        if (!relatedDispatcherTransaction) {
          this.logger.info(this.standardLogFn(incomerTransaction as any)("Couldn't find the related Dispatcher transaction"));

          continue;
        }

        await Promise.all([
          this.publishEvent({
            concernedChannel: concernedIncomerChannel,
            transactionMeta: {
              mainTransaction: incomerTransaction.redisMetadata.mainTransaction,
              relatedTransaction: relatedDispatcherTransaction.redisMetadata.relatedTransaction,
              eventTransactionId: relatedDispatcherTransaction.redisMetadata.relatedTransaction,
              resolved: incomerTransaction.redisMetadata.resolved
            },
            formattedEvent: {
              ...incomerTransaction,
              redisMetadata: {
                origin: relatedDispatcherTransaction.redisMetadata.origin,
                to: providedUUID
              }
            }
          }),
          incomerTransactionStore.deleteTransaction(incomerTransactionId),
          this.dispatcherTransactionStore.deleteTransaction(relatedDispatcherTransaction.redisMetadata.transactionId)
        ]);

        this.logger.info(this.standardLogFn(incomerTransaction as any)("Spread transaction redistributed to an Incomer"));

        continue;
      }
    }

    dispatcherTransactions = await this.dispatcherTransactionStore.getTransactions();

    for (const [dispatcherTransactionId, dispatcherTransaction] of dispatcherTransactions.entries()) {
      if (dispatcherTransaction.redisMetadata.to !== incomerToRemove.providedUUID) {
        continue;
      }

      if (dispatcherTransaction.name === "ping" || dispatcherTransaction.name === "approvement") {
        await this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId);

        continue;
      }

      if (dispatcherTransaction.redisMetadata.relatedTransaction) {
        const concernedIncomer = [...incomers].find(
          (incomer) => dispatcherTransaction.redisMetadata.incomerName === incomer.name && incomer.eventsSubscribe.find(
            (eventSubscribe) => eventSubscribe.name === dispatcherTransaction.name
          )
        );

        if (!concernedIncomer) {
          await Promise.all([
            this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId),
            this.backupDispatcherTransactionStore.setTransaction({ ...dispatcherTransaction })
          ]);

          continue;
        }

        const incomerRelatedTransactionStore = new TransactionStore({
          prefix: `${concernedIncomer.prefix ? `${concernedIncomer.prefix}-` : ""}${concernedIncomer.providedUUID}`,
          instance: "incomer"
        });

        const relatedTransaction = [...(await incomerRelatedTransactionStore.getTransactions()).values()]
          .find((incomerTransaction) => incomerTransaction.redisMetadata.relatedTransaction ===
            dispatcherTransaction.redisMetadata.transactionId
          );

        if (relatedTransaction) {
          continue;
        }

        const { providedUUID, prefix } = concernedIncomer;

        let concernedIncomerChannel = this.incomerChannels.get(providedUUID);

        if (!concernedIncomerChannel) {
          concernedIncomerChannel = new Channel({
            name: providedUUID,
            prefix
          });

          this.incomerChannels.set(providedUUID, concernedIncomerChannel);
        }

        await Promise.all([
          this.publishEvent({
            concernedChannel: concernedIncomerChannel,
            transactionMeta: {
              mainTransaction: dispatcherTransaction.redisMetadata.mainTransaction,
              relatedTransaction: dispatcherTransaction.redisMetadata.relatedTransaction,
              eventTransactionId: dispatcherTransaction.redisMetadata.relatedTransaction,
              resolved: dispatcherTransaction.redisMetadata.resolved
            },
            formattedEvent: {
              ...dispatcherTransaction,
              redisMetadata: {
                origin: this.privateUUID,
                to: concernedIncomer.providedUUID
              }
            }
          }),
          this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId)
        ]);

        this.logger.info(this.standardLogFn(dispatcherTransaction && { redisMetadata: {
          ...dispatcherTransaction.redisMetadata,
          origin: this.privateUUID,
          to: concernedIncomer.providedUUID
        } } as unknown as T)("Redistributed unresolved injected event to an Incomer"));
      }
    }
  }

  private async checkForDistributableTransactions(
    backedUpIncomerTransactions: Transactions<"incomer">,
    backedUpDispatcherTransactions: Transactions<"dispatcher">,
    incomers: Set<RegisteredIncomer>
  ) {
    const toResolve = [];

    for (const [backedUpTransactionId, backupIncomerTransaction] of backedUpIncomerTransactions.entries()) {
      if (backupIncomerTransaction.redisMetadata.mainTransaction) {
        const concernedIncomer = [...incomers.values()].find(
          (incomer) => incomer.name === backupIncomerTransaction.redisMetadata.incomerName && incomer.eventsCast.find(
            (castedEvent) => castedEvent === backupIncomerTransaction.name
          )
        );

        if (!concernedIncomer) {
          continue;
        }

        const concernedIncomerStore = new TransactionStore({
          prefix: `${concernedIncomer.prefix ? `${concernedIncomer.prefix}-` : ""}${concernedIncomer.providedUUID}`,
          instance: "incomer"
        });

        toResolve.push(
          concernedIncomerStore.setTransaction({
            ...backupIncomerTransaction,
            redisMetadata: {
              ...backupIncomerTransaction.redisMetadata,
              origin: concernedIncomer.providedUUID
            }
          }),
          this.backupIncomerTransactionStore.deleteTransaction(backedUpTransactionId)
        );

        continue;
      }

      if (backupIncomerTransaction.redisMetadata.relatedTransaction) {
        const concernedIncomer = [...incomers].find(
          (incomer) => incomer.name === backupIncomerTransaction.redisMetadata.incomerName && incomer.eventsSubscribe.find(
            (subscribedEvent) => subscribedEvent.name === backupIncomerTransaction.name
          )
        );

        if (!concernedIncomer) {
          continue;
        }

        const relatedDispatcherTransactionId = [...backedUpDispatcherTransactions.keys()]
          .find(
            (dispatcherTransactionId) => dispatcherTransactionId === backupIncomerTransaction.redisMetadata.relatedTransaction
          );

        if (!relatedDispatcherTransactionId) {
          continue;
        }

        if (!backupIncomerTransaction.redisMetadata.resolved) {
          const { providedUUID, prefix } = concernedIncomer;

          let concernedIncomerChannel = this.incomerChannels.get(providedUUID);

          if (!concernedIncomerChannel) {
            concernedIncomerChannel = new Channel({
              name: providedUUID,
              prefix
            });

            this.incomerChannels.set(providedUUID, concernedIncomerChannel);
          }

          toResolve.push([
            this.publishEvent({
              concernedChannel: concernedIncomerChannel,
              transactionMeta: {
                mainTransaction: backupIncomerTransaction.redisMetadata.mainTransaction,
                relatedTransaction: backupIncomerTransaction.redisMetadata.relatedTransaction,
                eventTransactionId: null,
                resolved: backupIncomerTransaction.redisMetadata.resolved
              },
              formattedEvent: {
                ...backupIncomerTransaction,
                redisMetadata: {
                  origin: this.privateUUID,
                  to: concernedIncomer.providedUUID
                }
              }
            }),
            this.backupIncomerTransactionStore.deleteTransaction(backedUpTransactionId),
            this.backupDispatcherTransactionStore.deleteTransaction(relatedDispatcherTransactionId)
          ]);

          continue;
        }

        const concernedIncomerStore = new TransactionStore({
          prefix: `${concernedIncomer.prefix ? `${concernedIncomer.prefix}-` : ""}${concernedIncomer.providedUUID}`,
          instance: "incomer"
        });

        toResolve.push(
          concernedIncomerStore.setTransaction({
            ...backupIncomerTransaction,
            redisMetadata: {
              ...backupIncomerTransaction.redisMetadata,
              origin: concernedIncomer.providedUUID
            }
          }),
          this.backupIncomerTransactionStore.deleteTransaction(backedUpTransactionId)
        );
      }
    }

    await Promise.all(toResolve);
    toResolve.length = 0;

    const updatedBackupDispatcherTransactions = await this.backupIncomerTransactionStore.getTransactions();

    for (const [transactionId, backupDispatcherTransaction] of updatedBackupDispatcherTransactions.entries()) {
      const concernedIncomer = [...incomers].find(
        (incomer) => incomer.name === backupDispatcherTransaction.redisMetadata.incomerName && incomer.eventsSubscribe.find(
          (subscribedEvent) => subscribedEvent.name === backupDispatcherTransaction.name
        )
      );

      if (!concernedIncomer) {
        continue;
      }

      const { providedUUID, prefix } = concernedIncomer;

      let concernedIncomerChannel = this.incomerChannels.get(providedUUID);

      if (!concernedIncomerChannel) {
        concernedIncomerChannel = new Channel({
          name: providedUUID,
          prefix
        });

        this.incomerChannels.set(providedUUID, concernedIncomerChannel);
      }

      toResolve.push([
        this.publishEvent({
          concernedChannel: concernedIncomerChannel,
          transactionMeta: {
            mainTransaction: backupDispatcherTransaction.redisMetadata.mainTransaction,
            relatedTransaction: backupDispatcherTransaction.redisMetadata.relatedTransaction,
            eventTransactionId: backupDispatcherTransaction.redisMetadata.relatedTransaction,
            resolved: backupDispatcherTransaction.redisMetadata.resolved
          },
          formattedEvent: {
            ...backupDispatcherTransaction,
            redisMetadata: {
              origin: this.privateUUID,
              to: concernedIncomer.providedUUID,
              incomerName: concernedIncomer.name
            }
          }
        }),
        this.backupDispatcherTransactionStore.deleteTransaction(transactionId)
      ]);
    }

    await Promise.all(toResolve);
  }

  private async resolveDispatcherTransactions(
    dispatcherTransactions: Transactions<"dispatcher">,
    backupDispatcherTransactions: Transactions<"dispatcher">
  ) {
    const incomers = await this.incomerStore.getIncomers();

    const backupIncomerTransactions = await this.backupIncomerTransactionStore.getTransactions();

    await this.checkForDistributableTransactions(backupIncomerTransactions, backupDispatcherTransactions, incomers);

    const toResolve = [];
    const incomerStateToUpdate = new Set<string>();
    for (const [dispatcherTransactionId, dispatcherTransaction] of dispatcherTransactions.entries()) {
      const transactionRecipient = dispatcherTransaction.redisMetadata.to;

      const relatedIncomer = [...incomers].find((incomer) => incomer.providedUUID === transactionRecipient) ||
        [...incomers].find((incomer) => incomer.baseUUID === transactionRecipient);

      if (!relatedIncomer) {
        continue;
      }

      const prefix = relatedIncomer.prefix ?? "";
      const relatedIncomerTransactionStore = new TransactionStore({
        prefix: `${prefix ? `${prefix}-` : ""}${transactionRecipient}`,
        instance: "incomer"
      });

      const relatedIncomerTransactions = await relatedIncomerTransactionStore.getTransactions();

      const relatedIncomerTransactionId = [...relatedIncomerTransactions.keys()].find(((relatedIncomerTransactionId) => {
        const incomerTransaction = relatedIncomerTransactions.get(relatedIncomerTransactionId);
        const { relatedTransaction, resolved } = incomerTransaction.redisMetadata;

        return relatedTransaction === dispatcherTransactionId && resolved;
      }));

      // Event not resolved yet
      if (!relatedIncomerTransactionId) {
        continue;
      }

      if (dispatcherTransaction.redisMetadata.mainTransaction) {
        // Only in case of ping event
        incomerStateToUpdate.add(relatedIncomerTransactions.get(relatedIncomerTransactionId).redisMetadata.origin);
        toResolve.push(Promise.all([
          relatedIncomerTransactionStore.deleteTransaction(relatedIncomerTransactionId),
          this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId)
        ]));

        continue;
      }

      if (dispatcherTransaction.name === "approvement") {
        const transaction = relatedIncomerTransactions.get(relatedIncomerTransactionId);

        if (!transaction || !transaction.redisMetadata.resolved) {
          continue;
        }

        toResolve.push(Promise.all([
          relatedIncomerTransactionStore.deleteTransaction(relatedIncomerTransactionId),
          this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId)
        ]));

        continue;
      }

      dispatcherTransaction.redisMetadata.resolved = true;
      incomerStateToUpdate.add(relatedIncomerTransactions.get(relatedIncomerTransactionId).redisMetadata.to);
      toResolve.push(Promise.all([
        relatedIncomerTransactionStore.deleteTransaction(relatedIncomerTransactionId),
        this.dispatcherTransactionStore.updateTransaction(
          dispatcherTransactionId,
          dispatcherTransaction
        )
      ]));
    }

    toResolve.push([...incomerStateToUpdate.values()].map(
      (incomerId) => this.updateIncomerState(incomerId))
    );

    await Promise.all(toResolve);
  }

  private async resolveIncomerMainTransactions(
    dispatcherTransactions: Transactions<"dispatcher">,
    backupDispatcherTransactions: Transactions<"dispatcher">
  ) {
    const incomers = await this.incomerStore.getIncomers();

    const toResolve = [];
    const incomerStateToUpdate = new Set<string>();
    for (const incomer of incomers) {
      const incomerStore = new TransactionStore({
        prefix: `${incomer.prefix ? `${incomer.prefix}-` : ""}${incomer.providedUUID}`,
        instance: "incomer"
      });

      const incomerTransactions = await incomerStore.getTransactions();

      for (const [incomerTransactionId, incomerTransaction] of incomerTransactions.entries()) {
        if (!incomerTransaction.redisMetadata.mainTransaction) {
          continue;
        }

        const allRelatedDispatcherTransactions: Transaction<"dispatcher">[][] = [
          [...dispatcherTransactions.values()], [...backupDispatcherTransactions.values()]
        ].map(
          (arr) => arr.filter((dispatcherTransaction) => dispatcherTransaction.redisMetadata.relatedTransaction ===
            incomerTransactionId)
        );

        const [relatedDispatcherTransactions, relatedBackupDispatcherTransactions] = allRelatedDispatcherTransactions;

        // Event not resolved yet by the dispatcher
        if (relatedDispatcherTransactions.length === 0 && relatedBackupDispatcherTransactions.length === 0) {
          continue;
        }

        const unResolvedRelatedTransactions = [...relatedDispatcherTransactions.values()].filter(
          (dispatcherTransaction) => !dispatcherTransaction.redisMetadata.resolved
        );

        for (const backupTransaction of relatedBackupDispatcherTransactions.values()) {
          const relatedIncomer = [...incomers.values()]
            .find((incomer) => incomer.name === backupTransaction.redisMetadata.incomerName &&
              incomer.eventsSubscribe.find((event) => event.name === backupTransaction.name));

          if (!relatedIncomer) {
            continue;
          }

          const { providedUUID, prefix } = relatedIncomer;

          let concernedIncomerChannel = this.incomerChannels.get(providedUUID);

          if (!concernedIncomerChannel) {
            concernedIncomerChannel = new Channel({
              name: providedUUID,
              prefix
            });

            this.incomerChannels.set(providedUUID, concernedIncomerChannel);
          }

          toResolve.push(Promise.all([
            this.publishEvent({
              concernedChannel: concernedIncomerChannel,
              transactionMeta: {
                mainTransaction: backupTransaction.redisMetadata.mainTransaction,
                relatedTransaction: backupTransaction.redisMetadata.relatedTransaction,
                eventTransactionId: backupTransaction.redisMetadata.relatedTransaction,
                resolved: backupTransaction.redisMetadata.resolved
              },
              formattedEvent: {
                ...incomerTransaction,
                redisMetadata: {
                  origin: backupTransaction.redisMetadata.origin,
                  to: providedUUID
                }
              }
            }),
            this.backupDispatcherTransactionStore.deleteTransaction(backupTransaction.redisMetadata.transactionId)
          ]));
        }

        // Event not resolved yet by the different incomers
        if (unResolvedRelatedTransactions.length > 0 || relatedBackupDispatcherTransactions.length > 0) {
          continue;
        }

        for (const relatedDispatcherTransaction of relatedDispatcherTransactions.values()) {
          incomerStateToUpdate.add(relatedDispatcherTransaction.redisMetadata.to);

          toResolve.push(
            this.dispatcherTransactionStore.deleteTransaction(relatedDispatcherTransaction.redisMetadata.transactionId)
          );
        }

        toResolve.push(incomerStore.deleteTransaction(incomerTransactionId));
      }
    }

    toResolve.push([...incomerStateToUpdate.values()].map(
      (incomerId) => this.updateIncomerState(incomerId))
    );

    await Promise.all(toResolve);
  }

  private async updateIncomerState(origin: string) {
    try {
      await this.incomerStore.updateIncomerState(origin);
    }
    catch (error) {
      this.logger.error({ uuid: origin, error: error.message }, "Failed to update incomer state");
    }
  }

  private async setAsActiveDispatcher() {
    const incomers = await this.incomerStore.getIncomers();

    for (const incomer of incomers) {
      if (incomer.baseUUID === this.selfProvidedUUID) {
        try {
          await this.incomerStore.updateIncomer({
            ...incomer,
            isDispatcherActiveInstance: true
          });
        }
        catch (error) {
          this.logger.error({ uuid: origin, error: error.message }, "Failed to update incomer state while taking relay");
        }

        break;
      }
    }
  }

  private schemaValidation(message: IncomerRegistrationMessage | EventMessage<T>) {
    const { redisMetadata, ...event } = message;

    const eventValidations = this.eventsValidationFn.get(event.name) as ValidateFunction<Record<string, any>>;
    const redisMetadataValidationFn = this.eventsValidationFn.get("redisMetadata") as ValidateFunction<Record<string, any>>;

    if (!eventValidations) {
      throw new Error(`Unknown Event ${event.name}`);
    }

    if (!redisMetadataValidationFn(redisMetadata)) {
      throw new Error(
        `Malformed redis metadata: [${[...redisMetadataValidationFn.errors]
          .map((error) => `${error.instancePath ? `${error.instancePath}:` : ""} ${error.message}`).join("|")}]`
      );
    }

    if (this.validationCbFn && isIncomerChannelMessage(message) && event.name !== "ping") {
      this.validationCbFn({ ...message });

      return;
    }

    if (!eventValidations(event)) {
      throw new Error(
        `Malformed event: [${[...eventValidations.errors]
          .map((error) => `${error.instancePath ? `${error.instancePath}:` : ""} ${error.message}`).join("|")}]`
      );
    }
  }

  private async handleMessages(channel: string, message: string) {
    if (!message) {
      return;
    }

    const formattedMessage: DispatcherChannelMessages["IncomerMessages"] |
      IncomerChannelMessages<T>["IncomerMessages"] = JSON.parse(message);

    if (!this.isWorking) {
      if (formattedMessage.name === "OK" && formattedMessage.redisMetadata.origin !== this.privateUUID) {
        this.emit("OK");

        return;
      }

      return;
    }

    if (this.isWorking) {
      if (formattedMessage.name === "OK") {
        return;
      }
    }

    try {
      if (!formattedMessage.name || !formattedMessage.redisMetadata) {
        throw new Error("Malformed message");
      }

      // Avoid reacting to his own message
      // Edge case when dispatcher is also an incomer and approve himself
      if (formattedMessage.redisMetadata.origin === this.privateUUID) {
        return;
      }

      this.schemaValidation(formattedMessage);

      if (channel === this.dispatcherChannelName) {
        if (isDispatcherChannelMessage(formattedMessage)) {
          await this.handleDispatcherMessages(channel, formattedMessage);
        }
        else {
          throw new Error("Unknown event on Dispatcher Channel");
        }
      }
      else if (isIncomerChannelMessage(formattedMessage)) {
        await this.handleIncomerMessages(channel, formattedMessage);
      }
    }
    catch (error) {
      this.logger.error({ channel, message: formattedMessage, error: error.message });
    }
  }

  private async handleDispatcherMessages(
    channel: string,
    message: DispatcherChannelMessages["IncomerMessages"]
  ) {
    const { name } = message;

    const logData = {
      channel,
      ...message
    };

    match<DispatcherChannelEvents>({ name })
      .with({ name: "register" }, async() => {
        this.logger.info(logData, "Registration asked");

        if (isIncomerRegistrationMessage(message)) {
          await this.approveIncomer(message);
        }
      })
      .exhaustive()
      .catch((error) => {
        this.logger.error({ channel: "dispatcher", message, error: error.message });
      });
  }

  private async handleIncomerMessages(
    channel: string,
    message: IncomerChannelMessages<T>["IncomerMessages"]
  ) {
    const { redisMetadata, ...event } = message;
    const { name } = event;
    const { transactionId } = redisMetadata;

    const logData = {
      channel,
      ...message
    };

    const senderTransactionStore = new TransactionStore({
      prefix: `${redisMetadata.prefix ? `${redisMetadata.prefix}-` : ""}${redisMetadata.origin}`,
      instance: "incomer"
    });

    const relatedTransaction = await senderTransactionStore.getTransactionById(transactionId);

    if (!relatedTransaction) {
      throw new Error(this.standardLogFn(logData)(`Couldn't find the related main transaction for: ${transactionId}`));
    }

    const incomers = await this.incomerStore.getIncomers();

    const concernedIncomers = [...incomers]
      .filter(
        (incomer) => incomer.eventsSubscribe.find((subscribedEvent) => subscribedEvent.name === name)
      );

    if (concernedIncomers.length === 0) {
      if (name === "ping") {
        this.logger.warn(this.standardLogFn(logData)("No concerned Incomer found"));
      }
      else {
        await Promise.all([
          senderTransactionStore.updateTransaction(transactionId, {
            ...relatedTransaction,
            redisMetadata: {
              ...relatedTransaction.redisMetadata,
              published: true
            }
          }),
          this.backupDispatcherTransactionStore.setTransaction({
            ...event,
            redisMetadata: {
              origin: this.privateUUID,
              to: "",
              mainTransaction: false,
              relatedTransaction: transactionId,
              resolved: false
            }
          } as unknown as PartialTransaction<"dispatcher">)
        ]);

        this.logger.warn(this.standardLogFn(logData)("Backed-up event"));
      }

      return;
    }

    const filteredConcernedIncomers: RegisteredIncomer[] = [];
    for (const incomer of concernedIncomers) {
      const relatedEvent = incomer.eventsSubscribe.find((subscribedEvent) => subscribedEvent.name === name);

      // Prevent publishing an event to multiple instance of a same service if no horizontalScale of the event
      if (!relatedEvent.horizontalScale &&
        filteredConcernedIncomers.find(
          (filteredConcernedIncomer) => filteredConcernedIncomer.eventsSubscribe.find(
            (subscribedEvent) => subscribedEvent.name === relatedEvent.name && filteredConcernedIncomer.name === incomer.name
          )
        ) !== undefined
      ) {
        continue;
      }

      filteredConcernedIncomers.push(incomer);
    }

    const toResolve: Promise<any>[] = [];
    for (const incomer of filteredConcernedIncomers) {
      const { providedUUID, prefix } = incomer;

      let concernedIncomerChannel = this.incomerChannels.get(providedUUID);

      if (!concernedIncomerChannel) {
        concernedIncomerChannel = new Channel({
          name: providedUUID,
          prefix
        });

        this.incomerChannels.set(providedUUID, concernedIncomerChannel);
      }

      const formattedEvent = {
        ...message,
        redisMetadata: {
          origin: this.privateUUID,
          to: providedUUID,
          incomerName: incomer.name
        }
      };

      toResolve.push(this.publishEvent({
        concernedChannel: concernedIncomerChannel,
        transactionMeta: {
          mainTransaction: false,
          relatedTransaction: transactionId,
          eventTransactionId: transactionId,
          resolved: false
        },
        formattedEvent
      }));
    }

    await this.updateIncomerState(redisMetadata.origin);
    await Promise.all([
      ...toResolve,
      senderTransactionStore.updateTransaction(transactionId, {
        ...relatedTransaction,
        redisMetadata: {
          ...relatedTransaction.redisMetadata,
          published: true
        }
      })
    ]);

    this.logger.info(this.standardLogFn(
      Object.assign({}, logData, {
        redisMetadata: {
          ...redisMetadata,
          eventTransactionId: transactionId,
          to: `[${filteredConcernedIncomers.map((incomer) => incomer.providedUUID)}]`
        }
      })
    )("Custom event distributed"));
  }

  private async approveIncomer(message: IncomerRegistrationMessage) {
    const { data, redisMetadata } = message;
    const { prefix, origin, transactionId } = redisMetadata;

    const relatedTransactionStore = new TransactionStore<"incomer">({
      prefix: `${prefix ? `${prefix}-` : ""}${origin}`,
      instance: "incomer"
    });

    const relatedTransaction = await relatedTransactionStore.getTransactionById(transactionId);
    if (!relatedTransaction) {
      throw new Error("No related transaction found next to register event");
    }

    // Get Incomers Tree
    const incomers = await this.incomerStore.getIncomers();

    // Avoid multiple init from a same instance of a incomer
    for (const incomer of incomers) {
      if (incomer.baseUUID === origin) {
        await this.dispatcherTransactionStore.deleteTransaction(transactionId);

        throw new Error("Forbidden multiple registration for a same instance");
      }
    }

    // Update the tree
    const now = Date.now();

    const incomer = Object.assign({}, {
      ...data,
      isDispatcherActiveInstance: origin === this.selfProvidedUUID,
      baseUUID: origin,
      lastActivity: now,
      aliveSince: now,
      prefix
    });

    const providedUUID = await this.incomerStore.setIncomer(incomer);

    // Subscribe to the exclusive service channel
    this.incomerChannels.set(providedUUID, new Channel({
      name: providedUUID,
      prefix
    }));

    await this.subscriber.subscribe(`${prefix ? `${prefix}-` : ""}${providedUUID}`);

    const event: Omit<DispatcherRegistrationMessage, "redisMetadata"> & {
      redisMetadata: Omit<DispatcherRegistrationMessage["redisMetadata"], "transactionId">
    } = {
      name: "approvement",
      data: {
        uuid: providedUUID
      },
      redisMetadata: {
        origin: this.privateUUID,
        incomerName: "dispatcher",
        to: redisMetadata.origin
      }
    };

    // Approve the service & send him info so he can use the dedicated channel
    await this.publishEvent({
      concernedChannel: this.dispatcherChannel as Channel<DispatcherChannelMessages["DispatcherMessages"]>,
      transactionMeta: {
        mainTransaction: false,
        relatedTransaction: transactionId,
        eventTransactionId: null,
        resolved: false
      },
      formattedEvent: event
    });

    this.logger.info("Approved Incomer");
  }
}
