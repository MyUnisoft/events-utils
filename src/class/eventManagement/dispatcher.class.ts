/* eslint-disable max-lines */
// Import Node.js Dependencies
import { randomUUID } from "node:crypto";

// Import Third-party Dependencies
import * as Redis from "@myunisoft/redis";
import * as logger from "pino";
import Ajv, { ValidateFunction } from "ajv";
import { match } from "ts-pattern";

// Import Internal Dependencies
import {
  REDIS_HOST,
  REDIS_PORT,
  channels,
  kIncomerStoreName
} from "../../utils/config";
import {
  Transactions,
  TransactionStore
} from "./transaction.class";
import {
  Prefix,
  EventCast,
  EventSubscribe,
  DispatcherChannelMessages,
  IncomerChannelMessages
} from "../../types/eventManagement/index";
import * as eventsSchema from "../../schema/eventManagement/index";
import { DispatcherRegistrationMessage, IncomerRegistrationMessage } from "../../types/eventManagement/dispatcherChannel";
import { DispatcherPingMessage, DistributedEventMessage, EventMessage } from "../../types/eventManagement/incomerChannel";
import { CustomEventsValidationFunctions } from "../../utils/index";

// CONSTANTS
const ajv = new Ajv();
const kPingInterval = 60_000;
const kIdleTime = 80_000;
const kCheckLastActivityInterval = 90_000;
const kCheckRelatedTransactionInterval = 60_000;
const kBackupTransactionStoreName = "backup";

interface RegisteredIncomer {
  providedUUID: string;
  baseUUID: string;
  name: string;
  lastActivity: number;
  aliveSince: number;
  eventsCast: EventCast[];
  eventsSubscribe: EventSubscribe[];
  prefix?: string;
}

type IncomerStore = Record<string, RegisteredIncomer>;

export type DispatcherOptions<T extends Record<string, any> =
Record<string, any>> = {
  /* Prefix for the channel name, commonly used to distinguish envs */
  prefix?: Prefix;
  eventsValidation?: {
    eventsValidationFn?: Map<string, ValidateFunction<Record<string, any>> | CustomEventsValidationFunctions>;
    validationCbFn?: (event: T) => void;
  },
  pingInterval?: number;
  checkLastActivityInterval?: number;
  checkTransactionInterval?: number;
  idleTime?: number;
}

type DispatcherChannelEvents = { name: "register" };

function isDispatcherChannelMessage(
  value: DispatcherChannelMessages["IncomerMessages"] |
  IncomerChannelMessages["IncomerMessages"]
): value is DispatcherChannelMessages["IncomerMessages"] {
  return value.name === "register";
}

function isIncomerChannelMessage(
  value: DispatcherChannelMessages["IncomerMessages"] |
  IncomerChannelMessages["IncomerMessages"]
): value is IncomerChannelMessages["IncomerMessages"] {
  return value.name !== "register";
}

function isIncomerRegistrationMessage(
  value: DispatcherChannelMessages["IncomerMessages"]
): value is IncomerRegistrationMessage {
  return value.name === "register";
}

export class Dispatcher<T extends Record<string, any> =
Record<string, any>> {
  readonly type = "dispatcher";
  readonly formattedPrefix: string;
  readonly prefix: string;
  readonly treeName: string;
  readonly dispatcherChannelName: string;
  readonly privateUUID = randomUUID();

  protected subscriber: Redis.Redis;

  private dispatcherChannel: Redis.Channel<DispatcherChannelMessages["DispatcherMessages"]>;
  private incomerStore: Redis.KVPeer<IncomerStore>;
  private dispatcherTransactionStore: TransactionStore<"dispatcher">;
  private backupIncomerTransactionStore: TransactionStore<"incomer">;
  private backupDispatcherTransactionStore: TransactionStore<"dispatcher">;

  private logger: logger.Logger;
  private incomerChannels: Map<string,
    Redis.Channel<IncomerChannelMessages["DispatcherMessages"] | DistributedEventMessage>> = new Map();

  private pingInterval: NodeJS.Timer;
  private checkLastActivityInterval: NodeJS.Timer;
  private checkRelatedTransactionInterval: NodeJS.Timer;
  private idleTime: number;

  private eventsValidationFn: Map<string, ValidateFunction<Record<string, any>> | CustomEventsValidationFunctions>;
  private validationCbFn: (event: T) => void = null;

  constructor(options: DispatcherOptions<T> = {}, subscriber?: Redis.Redis) {
    this.prefix = options.prefix ?? "";
    this.formattedPrefix = options.prefix ? `${options.prefix}-` : "";
    this.treeName = this.formattedPrefix + kIncomerStoreName;
    this.dispatcherChannelName = this.formattedPrefix + channels.dispatcher;
    this.idleTime = options.idleTime ?? kIdleTime;

    this.eventsValidationFn = options?.eventsValidation?.eventsValidationFn ?? new Map();
    this.validationCbFn = options?.eventsValidation?.validationCbFn;

    for (const [name, validationSchema] of Object.entries(eventsSchema)) {
      this.eventsValidationFn.set(name, ajv.compile(validationSchema));
    }

    this.logger = logger.pino({
      level: "debug",
      transport: {
        target: "pino-pretty"
      }
    }).child({ dispatcher: this.formattedPrefix + this.type });

    if (subscriber) {
      this.subscriber = subscriber;
    }

    this.incomerStore = new Redis.KVPeer({
      prefix: this.prefix,
      type: "object"
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

    this.dispatcherChannel = new Redis.Channel({
      prefix: this.prefix,
      name: channels.dispatcher
    });

    this.pingInterval = setInterval(async() => {
      try {
        await this.ping();
      }
      catch (error) {
        this.logger.error(error.message);
      }
    }, options.pingInterval ?? kPingInterval).unref();

    this.checkLastActivityInterval = setInterval(async() => {
      try {
        await this.checkLastActivity();
      }
      catch (error) {
        this.logger.error(error.message);
      }
    }, options.checkLastActivityInterval ?? kCheckLastActivityInterval).unref();

    this.checkRelatedTransactionInterval = setInterval(async() => {
      try {
        const dispatcherTransactions = await this.dispatcherTransactionStore.getTransactions();

        // Resolve Dispatcher transactions
        await this.resolveDispatcherTransactions(dispatcherTransactions);

        // Resolve main transactions
        await this.resolveIncomerMainTransactions(dispatcherTransactions);
      }
      catch (error) {
        this.logger.error(error.message);
      }
    }, options.checkTransactionInterval ?? kCheckRelatedTransactionInterval).unref();
  }

  public async initialize() {
    if (!this.subscriber) {
      this.subscriber = await Redis.initRedis({
        port: REDIS_PORT,
        host: REDIS_HOST
      } as any, true);
    }

    await this.subscriber.subscribe(this.dispatcherChannelName);

    this.subscriber.on("message", (channel, message) => this.handleMessages(channel, message));
  }

  public async close() {
    if (this.subscriber) {
      await Redis.closeRedis(this.subscriber);
      this.subscriber = undefined;
    }

    clearInterval(this.pingInterval);
    this.pingInterval = undefined;

    clearInterval(this.checkRelatedTransactionInterval);
    this.checkRelatedTransactionInterval = undefined;

    clearInterval(this.checkLastActivityInterval);
    this.checkLastActivityInterval = undefined;
  }

  private async ping() {
    const tree = await this.getTree();

    for (const [uuid, incomer] of Object.entries(tree)) {
      const incomerChannel = this.incomerChannels.get(uuid) ??
        new Redis.Channel({
          name: uuid,
          prefix: incomer.prefix
        });

      const event: DispatcherPingMessage = {
        name: "ping",
        data: null,
        redisMetadata: {
          origin: this.privateUUID,
          to: uuid
        }
      };

      await this.publishEvent({
        concernedChannel: incomerChannel,
        transactionMeta: {
          mainTransaction: true,
          relatedTransaction: null,
          resolved: false
        },
        formattedEvent: event
      });

      this.logger.info({
        ...event,
        uptime: process.uptime()
      }, "New Ping event");
    }
  }

  private async checkLastActivity() {
    const tree = await this.getTree();

    const now = Date.now();

    for (const [uuid, incomer] of Object.entries(tree)) {
      if (now <= incomer.lastActivity + this.idleTime) {
        continue;
      }

      // Remove the incomer from the tree & update it.
      await this.handleInactiveIncomer(tree, uuid);

      this.logger.info({
        uuid,
        incomer,
        uptime: process.uptime()
      }, "Removed inactive incomer");
    }
  }

  private async publishEvent(options: {
    concernedStore?: TransactionStore<"incomer">;
    concernedChannel: Redis.Channel<
      DispatcherChannelMessages["DispatcherMessages"] |
      (IncomerChannelMessages["DispatcherMessages"] | DistributedEventMessage)
    >;
    transactionMeta: {
      mainTransaction: boolean;
      relatedTransaction: null | string;
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
      resolved
    } = transactionMeta;

    const concernedStore = options.concernedStore ?? this.dispatcherTransactionStore;

    const transactionId = await concernedStore.setTransaction({
      ...formattedEvent,
      mainTransaction,
      relatedTransaction,
      resolved
    });

    await concernedChannel.publish({
      ...formattedEvent,
      redisMetadata: {
        ...formattedEvent.redisMetadata,
        transactionId
      }
    });
  }

  private async InactiveIncomerTransactionsResolution(options: {
    incomers: IncomerStore,
    incomerUUID: string,
    incomerTransactionStore: TransactionStore<"incomer">,
    incomerTransactions: Transactions<"incomer">,
    dispatcherTransactions: Transactions<"dispatcher">
  }
  ) {
    const {
      incomers,
      incomerUUID,
      incomerTransactionStore,
      incomerTransactions,
      dispatcherTransactions
    } = options;

    for (const [incomerTransactionId, incomerTransaction] of incomerTransactions.entries()) {
      // Remove possible ping response
      if (incomerTransaction.name === "ping") {
        await Promise.all([
          incomerTransactionStore.deleteTransaction(incomerTransactionId),
          this.dispatcherTransactionStore.deleteTransaction(incomerTransaction.relatedTransaction)
        ]);

        continue;
      }

      if (incomerTransaction.mainTransaction) {
        if (incomerTransaction.name === "register") {
          const relatedDispatcherTransactionId = Object.keys(dispatcherTransactions)
            .find(
              (dispatcherTransactionId) => dispatcherTransactions[dispatcherTransactionId].relatedTransaction ===
                incomerTransactionId
            );

          if (relatedDispatcherTransactionId) {
            await this.dispatcherTransactionStore.deleteTransaction(relatedDispatcherTransactionId);
          }

          await incomerTransactionStore.deleteTransaction(incomerTransactionId);

          continue;
        }

        const concernedMainTransactionIncomer = Object.values(incomers).find(
          (incomer) => incomer.eventsCast.find(
            (eventCast) => eventCast === incomerTransaction.name
          )
        );

        if (!concernedMainTransactionIncomer) {
          await Promise.all([
            incomerTransactionStore.deleteTransaction(incomerTransactionId),
            this.backupIncomerTransactionStore.setTransaction({
              ...incomerTransaction
            })
          ]);

          continue;
        }

        const { prefix, providedUUID } = concernedMainTransactionIncomer;

        const concernedIncomerStore = new TransactionStore({
          prefix: `${prefix ? `${prefix}-` : ""}${providedUUID}`,
          instance: "incomer"
        });

        await Promise.all([
          concernedIncomerStore.setTransaction({
            ...incomerTransaction,
            redisMetadata: {
              ...incomerTransaction.redisMetadata,
              origin: providedUUID
            }
          }),
          incomerTransactionStore.deleteTransaction(incomerTransactionId)
        ]);

        continue;
      }

      if (incomerTransaction.relatedTransaction) {
        const concernedRelatedTransactionIncomer = Object.values(incomers).find(
          (incomer) => incomer.eventsSubscribe.find(
            (subscribedEvent) => subscribedEvent.name === incomerTransaction.name
          )
        );

        if (!concernedRelatedTransactionIncomer || incomerTransaction.resolved) {
          await Promise.all([
            incomerTransactionStore.deleteTransaction(incomerTransactionId),
            this.backupIncomerTransactionStore.setTransaction({
              ...incomerTransaction
            })
          ]);

          continue;
        }

        const { providedUUID } = concernedRelatedTransactionIncomer;

        const concernedIncomerChannel = this.incomerChannels.get(providedUUID);

        await Promise.all([
          this.publishEvent({
            concernedChannel: concernedIncomerChannel,
            transactionMeta: {
              mainTransaction: incomerTransaction.mainTransaction,
              relatedTransaction: incomerTransaction.relatedTransaction,
              resolved: incomerTransaction.resolved
            },
            formattedEvent: {
              ...incomerTransaction,
              redisMetadata: {
                origin: this.privateUUID,
                to: providedUUID
              }
            }
          }),
          incomerTransactionStore.deleteTransaction(incomerTransactionId)
        ]);

        continue;
      }
    }

    for (const [dispatcherTransactionId, dispatcherTransaction] of dispatcherTransactions.entries()) {
      if (dispatcherTransaction.redisMetadata.to === incomerUUID) {
        if (dispatcherTransaction.name === "ping") {
          await this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId);

          continue;
        }

        if (dispatcherTransaction.relatedTransaction) {
          const concernedIncomer = Object.values(incomers).find(
            (incomer) => incomer.eventsSubscribe.find(
              (eventSubscribe) => eventSubscribe.name === dispatcherTransaction.name
            )
          );

          if (!concernedIncomer) {
            await Promise.all([
              this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId),
              this.backupDispatcherTransactionStore.setTransaction({
                name: dispatcherTransaction.name,
                data: dispatcherTransaction.data,
                ...dispatcherTransaction
              })
            ]);

            continue;
          }

          const concernedIncomerChannel = this.incomerChannels.get(concernedIncomer.providedUUID);

          await Promise.all([
            this.publishEvent({
              concernedChannel: concernedIncomerChannel,
              transactionMeta: {
                mainTransaction: dispatcherTransaction.mainTransaction,
                relatedTransaction: dispatcherTransaction.relatedTransaction,
                resolved: dispatcherTransaction.resolved
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
        }
      }
    }

    this.logger.info("Redistributed injected event");
  }

  private async handleInactiveIncomer(
    incomers: IncomerStore,
    incomerUUID: string
  ) {
    const incomer = incomers[incomerUUID];

    delete incomers[incomerUUID];
    this.incomerChannels.delete(incomerUUID);

    if (Object.entries(incomers).length > 0) {
      await this.incomerStore.setValue({
        key: this.treeName,
        value: incomers
      });
    }
    else {
      await this.incomerStore.deleteValue(this.treeName);
    }

    const incomerTransactionStore = new TransactionStore({
      prefix: `${incomer.prefix ? `${incomer.prefix}-` : ""}${incomerUUID}`,
      instance: "incomer"
    });

    const [incomerTransactions, dispatcherTransactions] = await Promise.all([
      incomerTransactionStore.getTransactions(),
      this.dispatcherTransactionStore.getTransactions()
    ]);

    await this.InactiveIncomerTransactionsResolution({
      incomers,
      incomerUUID,
      incomerTransactionStore,
      incomerTransactions,
      dispatcherTransactions
    });
  }

  private async checkForDistributableIncomerTransactions(backedUpIncomerTransactions: Transactions<"incomer">) {
    const toResolve = [];

    for (const [backedUpTransactionId, backedUpTransaction] of backedUpIncomerTransactions.entries()) {
      const incomers = await this.getTree();

      if (backedUpTransaction.mainTransaction) {
        const concernedIncomer = Object.values(incomers).find(
          (incomer) => incomer.eventsCast.find(
            (castedEvent) => castedEvent === backedUpTransaction.name
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
            ...backedUpTransaction,
            redisMetadata: {
              ...backedUpTransaction.redisMetadata,
              origin: concernedIncomer.providedUUID
            }
          }),
          this.backupIncomerTransactionStore.deleteTransaction(backedUpTransactionId)
        );
      }

      if (backedUpTransaction.relatedTransaction && !backedUpTransaction.resolved) {
        const concernedIncomer = Object.values(incomers).find(
          (incomer) => incomer.eventsSubscribe.find(
            (subscribedEvent) => subscribedEvent.name === backedUpTransaction.name
          )
        );

        if (!concernedIncomer) {
          continue;
        }

        const concernedIncomerChannel = this.incomerChannels.get(concernedIncomer.providedUUID);

        toResolve.push([
          this.publishEvent({
            concernedChannel: concernedIncomerChannel,
            transactionMeta: {
              mainTransaction: backedUpTransaction.mainTransaction,
              relatedTransaction: backedUpTransaction.relatedTransaction,
              resolved: backedUpTransaction.resolved
            },
            formattedEvent: {
              ...backedUpTransaction,
              redisMetadata: {
                origin: this.privateUUID,
                to: concernedIncomer.providedUUID
              }
            }
          }),
          this.backupIncomerTransactionStore.deleteTransaction(backedUpTransactionId)
        ]);
      }
    }

    await Promise.all(toResolve);
  }

  private async checkForDistributableDispatcherTransactions(
    backedUpDispatcherTransactions: Transactions<"dispatcher">,
    incomers: IncomerStore
  ) {
    for (const [backedUpDispatcherTransactionId, backedUpDispatcherTransaction] of backedUpDispatcherTransactions.entries()) {
      const concernedIncomer = Object.values(incomers).find(
        (incomer) => incomer.eventsSubscribe.find(
          (eventSubscribe) => eventSubscribe.name === backedUpDispatcherTransaction.name
        )
      );

      if (!concernedIncomer) {
        continue;
      }

      const concernedIncomerChannel = this.incomerChannels.get(concernedIncomer.providedUUID);

      await Promise.all([
        this.publishEvent({
          concernedChannel: concernedIncomerChannel,
          transactionMeta: {
            mainTransaction: backedUpDispatcherTransaction.mainTransaction,
            relatedTransaction: backedUpDispatcherTransaction.relatedTransaction,
            resolved: backedUpDispatcherTransaction.resolved
          },
          formattedEvent: {
            ...backedUpDispatcherTransaction,
            redisMetadata: {
              origin: this.privateUUID,
              to: concernedIncomer.providedUUID
            }
          }
        }),
        this.backupDispatcherTransactionStore.deleteTransaction(backedUpDispatcherTransactionId)
      ]);
    }
  }

  private async resolveDispatcherTransactions(
    dispatcherTransactions: Transactions<"dispatcher">
  ) {
    const backedUpIncomerTransactions = await this.backupIncomerTransactionStore.getTransactions();
    const backedUpDispatcherTransactions = await this.backupDispatcherTransactionStore.getTransactions();

    const incomers = await this.getTree();

    await this.checkForDistributableIncomerTransactions(backedUpIncomerTransactions);
    await this.checkForDistributableDispatcherTransactions(backedUpDispatcherTransactions, incomers);

    for (const [dispatcherTransactionId, dispatcherTransaction] of dispatcherTransactions.entries()) {
      // If Transaction is already resolved, skip
      if (dispatcherTransaction.resolved) {
        continue;
      }

      const prefix = incomers[dispatcherTransaction.redisMetadata.to].prefix ?? "";
      const relatedIncomerTransactionStore = new TransactionStore({
        prefix: `${prefix ? `${prefix}-` : ""}${dispatcherTransaction.redisMetadata.to}`,
        instance: "incomer"
      });

      const relatedIncomerTransactions = await relatedIncomerTransactionStore.getTransactions();

      const relatedIncomerTransactionId = [...relatedIncomerTransactions.keys()].find(((relatedIncomerTransactionId) => {
        const incomerTransaction = relatedIncomerTransactions.get(relatedIncomerTransactionId);
        const { relatedTransaction, resolved } = incomerTransaction;

        return relatedTransaction === dispatcherTransactionId && resolved;
      }));

      // Event not resolved yet
      if (!relatedIncomerTransactionId) {
        continue;
      }

      if (dispatcherTransaction.mainTransaction) {
        // Only in case of ping event
        await Promise.all([
          this.updateIncomerState(relatedIncomerTransactions.get(relatedIncomerTransactionId).redisMetadata.origin),
          relatedIncomerTransactionStore.deleteTransaction(relatedIncomerTransactionId),
          this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId)
        ]);

        continue;
      }

      dispatcherTransaction.resolved = true;
      await Promise.all([
        this.updateIncomerState(relatedIncomerTransactions.get(relatedIncomerTransactionId).redisMetadata.origin),
        relatedIncomerTransactionStore.deleteTransaction(relatedIncomerTransactionId),
        this.dispatcherTransactionStore.updateTransaction(dispatcherTransactionId, dispatcherTransaction)
      ]);
    }
  }

  private async resolveIncomerMainTransactions(
    dispatcherTransactions: Transactions<"dispatcher">
  ) {
    const incomerTree = await this.getTree();

    for (const incomer of Object.values(incomerTree)) {
      const incomerStore = new TransactionStore({
        prefix: `${incomer.prefix ? `${incomer.prefix}-` : ""}${incomer.providedUUID}`,
        instance: "incomer"
      });

      const incomerTransactions = await incomerStore.getTransactions();

      for (const [incomerTransactionId, incomerTransaction] of incomerTransactions.entries()) {
        if (!incomerTransaction.mainTransaction) {
          continue;
        }

        const relatedDispatcherTransactionsId = [...dispatcherTransactions.keys()].filter(
          (dispatcherTransactionId) => dispatcherTransactions.get(dispatcherTransactionId).relatedTransaction ===
            incomerTransactionId
        );

        // Event not resolved yet by the dispatcher
        if (relatedDispatcherTransactionsId.length === 0) {
          continue;
        }

        const unResolvedRelatedTransactions = relatedDispatcherTransactionsId.filter(
          (transactionId) => !dispatcherTransactions.get(transactionId).resolved
        );

        // Event not resolved yet by the different incomers
        if (unResolvedRelatedTransactions.length > 0) {
          continue;
        }

        const transactionToResolve: Promise<void>[] = [];

        for (const relatedDispatcherTransactionId of relatedDispatcherTransactionsId) {
          transactionToResolve.push(this.updateIncomerState(
            incomerTransactions.get(
              dispatcherTransactions.get(relatedDispatcherTransactionId).relatedTransaction
            ).redisMetadata.origin
          ));
          transactionToResolve.push(this.dispatcherTransactionStore.deleteTransaction(relatedDispatcherTransactionId));
        }

        await Promise.all([
          ...transactionToResolve,
          incomerStore.deleteTransaction(incomerTransactionId)
        ]);
      }
    }
  }

  private async updateIncomerState(origin: string) {
    const tree = await this.getTree();

    if (!tree[origin]) {
      throw new Error("Couldn't find the related incomer");
    }

    tree[origin].lastActivity = Date.now();

    await this.incomerStore.setValue({
      key: this.treeName,
      value: tree
    });
  }

  private async getTree(): Promise<IncomerStore> {
    const tree = await this.incomerStore.getValue(this.treeName);

    return tree ?? {};
  }

  private schemaValidation(message: IncomerRegistrationMessage | EventMessage<T>) {
    const { redisMetadata, ...event } = message;

    const eventValidations = this.eventsValidationFn.get(event.name) as ValidateFunction<Record<string, any>>;
    const redisMetadataValidationFn = this.eventsValidationFn.get("redisMetadata") as ValidateFunction<Record<string, any>>;

    if (!eventValidations) {
      throw new Error("Unknown Event");
    }

    if (!redisMetadataValidationFn(redisMetadata)) {
      throw new Error("Malformed message");
    }

    if (event.name === "register") {
      if (!eventValidations(event)) {
        throw new Error("Malformed message");
      }

      return;
    }

    if (this.validationCbFn && isIncomerChannelMessage(message)) {
      this.validationCbFn({ ...message, redisMetadata: null } as T);

      return;
    }

    if (!eventValidations(event)) {
      throw new Error("Malformed message");
    }
  }

  private async handleMessages(channel: string, message: string) {
    if (!message) {
      return;
    }

    const formattedMessage: DispatcherChannelMessages["IncomerMessages"] |
      IncomerChannelMessages["IncomerMessages"] = JSON.parse(message);

    try {
      if (!formattedMessage.name || !formattedMessage.redisMetadata) {
        throw new Error("Malformed message");
      }

      // Avoid reacting to his own message
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
      ...message,
      uptime: process.uptime()
    };

    match<DispatcherChannelEvents>({ name })
      .with({ name: "register" }, async() => {
        this.logger.info(logData, "New Registration on Dispatcher Channel");

        if (isIncomerRegistrationMessage(message)) {
          await this.approveIncomer(message);
        }
      })
      .exhaustive()
      .catch((error) => {
        this.logger.error({ channel: "dispatcher", error: error.message, message });
      });
  }

  private async handleIncomerMessages(
    channel: string,
    message: IncomerChannelMessages["IncomerMessages"]
  ) {
    const { name, redisMetadata } = message;
    const { origin } = redisMetadata;

    const logData = {
      channel,
      ...message,
      uptime: process.uptime()
    };

    const incomerTree = await this.getTree();
    if (!incomerTree[origin]) {
      throw new Error("Couldn't find the related incomer");
    }

    const concernedIncomers = Object.values(incomerTree)
      .filter(
        (incomer) => incomer.eventsSubscribe.find((subscribedEvent) => subscribedEvent.name === name) &&
        (incomer.providedUUID !== redisMetadata.origin || incomer.name !== "pulsar")
      );

    if (concernedIncomers.length === 0) {
      this.logger.warn(logData, "No concerned Incomer found");

      // Store in recovery state
      return;
    }

    const filteredConcernedIncomers: RegisteredIncomer[] = [];
    for (const incomer of concernedIncomers) {
      const relatedEvent = incomer.eventsSubscribe.find((subscribedEvent) => subscribedEvent.name === name);

      // Prevent publishing an event to multiple instance of a same service if no horizontalScale of the event
      if (!relatedEvent.horizontalScale &&
        filteredConcernedIncomers.find(
          (filteredConcernedIncomer) => filteredConcernedIncomer.eventsSubscribe.find(
            (subscribedEvent) => subscribedEvent.name === relatedEvent.name
          )
        ) !== undefined
      ) {
        continue;
      }

      filteredConcernedIncomers.push(incomer);
    }

    const toResolve: Promise<any>[] = [];
    for (const incomer of filteredConcernedIncomers) {
      const relatedChannel = this.incomerChannels.get(incomer.providedUUID);

      if (!relatedChannel) {
        throw new Error("Channel not found");
      }

      const formattedEvent = {
        ...message,
        redisMetadata: {
          origin: this.privateUUID,
          to: incomer.providedUUID
        }
      };

      toResolve.push(this.publishEvent({
        concernedChannel: relatedChannel,
        transactionMeta: {
          mainTransaction: false,
          relatedTransaction: redisMetadata.transactionId,
          resolved: false
        },
        formattedEvent
      }));
    }

    await this.updateIncomerState(redisMetadata.origin);
    await Promise.all(toResolve);
    this.logger.info(logData, "Distributed injected event");
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

    const providedUUID = randomUUID();

    // Get Incomers Tree
    const relatedIncomerTree = await this.getTree();

    // Avoid multiple init from a same instance of a incomer
    for (const incomer of Object.values(relatedIncomerTree)) {
      if (incomer.baseUUID === origin) {
        await this.dispatcherTransactionStore.deleteTransaction(transactionId);

        throw new Error("Forbidden multiple registration for a same instance");
      }
    }

    // Update the tree
    const now = Date.now();

    const incomer = Object.assign({}, {
      ...data,
      providedUUID,
      baseUUID: origin,
      lastActivity: now,
      aliveSince: now,
      prefix
    });

    relatedIncomerTree[providedUUID] = incomer;

    await this.incomerStore.setValue({
      key: this.treeName,
      value: relatedIncomerTree
    });

    // Subscribe to the exclusive service channel
    this.incomerChannels.set(providedUUID, new Redis.Channel({
      name: providedUUID,
      prefix
    }));

    await this.subscriber.subscribe(`${prefix ? `${prefix}-` : ""}${providedUUID}`);

    const event: DispatcherRegistrationMessage = {
      name: "approvement",
      data: {
        uuid: providedUUID
      },
      redisMetadata: {
        origin: this.privateUUID,
        to: redisMetadata.origin
      }
    };

    // Approve the service & send him info so he can use the dedicated channel
    await Promise.all([
      this.dispatcherChannel.publish(event),
      relatedTransactionStore.deleteTransaction(transactionId),
      this.dispatcherTransactionStore.deleteTransaction(transactionId)
    ]);

    this.logger.info({
      ...event,
      uptime: process.uptime()
    }, "New approvement event");
  }
}
