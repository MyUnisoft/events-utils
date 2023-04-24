// Import Node.js Dependencies
import { randomUUID } from "node:crypto";

// Import Third-party Dependencies
import * as Redis from "@myunisoft/redis";
import * as logger from "pino";
import Ajv, { ValidateFunction } from "ajv";
import { match } from "ts-pattern";

// Import Internal Dependencies
import {
  channels,
  kIncomerStoreName
} from "../../utils/config";
import {
  Transaction,
  Transactions,
  TransactionStore
} from "./transaction.class";
import {
  Prefix,
  EventsCast,
  EventsSubscribe,
  DispatcherChannelMessages,
  IncomerChannelMessages,
  DispatcherTransactionMetadata
} from "../../types/eventManagement/index";
import * as ChannelsMessages from "../../schema/eventManagement/index";
import { DispatcherRegistrationMessage, IncomerRegistrationMessage } from "../../types/eventManagement/dispatcherChannel";
import { DispatcherPingMessage } from "../../types/eventManagement/incomerChannel";
import { CustomEventsValidationFunctions } from "utils";

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
  eventsCast: EventsCast;
  eventsSubscribe: EventsSubscribe;
  prefix?: string;
}

type IncomerStore = Record<string, RegisteredIncomer>;

export interface DispatcherOptions {
  /* Prefix for the channel name, commonly used to distinguish envs */
  prefix?: Prefix;
  eventsValidationFunction?: Map<string, ValidateFunction<Record<string, any>> | CustomEventsValidationFunctions>;
  pingInterval?: number;
  checkLastActivityInterval?: number;
  checkTransactionInterval?: number;
  idleTime?: number;
}

type DispatcherChannelEvents = { name: "register" };

type IncomerCustomChannelMessage = Record<string, any> & {
  name: string;
  data: Record<string, any>;
  redisMetadata: DispatcherTransactionMetadata;
}

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

export class Dispatcher {
  readonly type = "dispatcher";
  readonly prefix: string;
  readonly treeName: string;
  readonly dispatcherChannelName: string;
  readonly dispatcherChannel: Redis.Channel<DispatcherChannelMessages["DispatcherMessages"]>;
  readonly privateUUID = randomUUID();

  readonly incomerStore: Redis.KVPeer<IncomerStore>;
  readonly dispatcherTransactionStore: TransactionStore<"dispatcher">;
  readonly backupIncomerTransactionStore: TransactionStore<"incomer">;

  protected subscriber: Redis.Redis;

  private logger: logger.Logger;
  private incomerChannels: Map<string,
    Redis.Channel<IncomerChannelMessages["DispatcherMessages"] | IncomerCustomChannelMessage>> = new Map();

  private pingInterval: NodeJS.Timer;
  private checkLastActivityInterval: NodeJS.Timer;
  private checkRelatedTransactionInterval: NodeJS.Timer;
  private idleTime: number;

  public eventsValidationFunction: Map<string, ValidateFunction<Record<string, any>> | CustomEventsValidationFunctions>;

  constructor(options: DispatcherOptions = {}, subscriber?: Redis.Redis) {
    this.prefix = options.prefix ? `${options.prefix}-` : "";
    this.treeName = this.prefix + kIncomerStoreName;
    this.dispatcherChannelName = this.prefix + channels.dispatcher;
    this.idleTime = options.idleTime ?? kIdleTime;

    this.eventsValidationFunction = options.eventsValidationFunction ?? new Map();

    for (const [name, validationSchema] of Object.entries(ChannelsMessages)) {
      this.eventsValidationFunction.set(name, ajv.compile(validationSchema));
    }

    this.incomerStore = new Redis.KVPeer({
      prefix: options.prefix,
      type: "object"
    });

    this.backupIncomerTransactionStore = new TransactionStore({
      prefix: this.prefix + kBackupTransactionStoreName,
      instance: "incomer"
    });

    this.dispatcherTransactionStore = new TransactionStore({
      prefix: options.prefix,
      instance: "dispatcher"
    });

    this.logger = logger.pino().child({ incomer: this.prefix + this.type });

    this.dispatcherChannel = new Redis.Channel({
      prefix: options.prefix,
      name: channels.dispatcher
    });

    this.subscriber = subscriber;

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
        port: process.env.REDIS_PORT,
        host: process.env.REDIS_HOST
      } as any, true);
    }

    await this.subscriber.subscribe(this.dispatcherChannelName);

    this.subscriber.on("message", async(channel, message) => await this.handleMessages(channel, message));
  }

  public async close() {
    if (!this.subscriber) {
      return;
    }

    clearInterval(this.pingInterval);
    this.pingInterval = undefined;

    clearInterval(this.checkRelatedTransactionInterval);
    this.checkRelatedTransactionInterval = undefined;

    clearInterval(this.checkLastActivityInterval);
    this.checkLastActivityInterval = undefined;

    await this.subscriber.quit();
    this.subscriber = undefined;
  }

  private async ping() {
    const tree = await this.getTree();

    for (const uuid of Object.keys(tree)) {
      const incomerChannel = this.incomerChannels.get(uuid);

      if (incomerChannel) {
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
      (IncomerChannelMessages["DispatcherMessages"] | IncomerCustomChannelMessage)
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

    const toResolve: Promise<any>[] = [];

    for (const [incomerTransactionId, incomerTransaction] of incomerTransactions.entries()) {
      // Remove possible ping response
      if (incomerTransaction.name === "ping") {
        toResolve.push(
          incomerTransactionStore.deleteTransaction(incomerTransactionId),
          this.dispatcherTransactionStore.deleteTransaction(incomerTransaction.relatedTransaction)
        );

        continue;
      }

      const concernedIncomer = Object.values(incomers).find(
        (incomer) => incomer.eventsCast.find(
          (castedEvent) => castedEvent === incomerTransaction.name
        )
      );

      // Transaction is a relatedTransaction or a main transaction without any incomer casting this event
      // No point to back up the first ones in incomers store
      if (incomerTransaction.relatedTransaction || !concernedIncomer) {
        toResolve.push(
          incomerTransactionStore.deleteTransaction(incomerTransactionId),
          this.backupIncomerTransactionStore.setTransaction({
            ...incomerTransaction
          })
        );

        continue;
      }

      const concernedIncomerStore = new TransactionStore({
        prefix: `${concernedIncomer.prefix ? `${concernedIncomer.prefix}-` : ""}${incomerUUID}`,
        instance: "incomer"
      });

      if (incomerTransaction.mainTransaction) {
        if (incomerTransaction.name === "register") {
          const relatedDispatcherTransactionId = Object.keys(dispatcherTransactions)
            .find(
              (dispatcherTransactionId) => dispatcherTransactions[dispatcherTransactionId].relatedTransaction ===
                incomerTransactionId
            );

          if (relatedDispatcherTransactionId) {
            toResolve.push(this.dispatcherTransactionStore.deleteTransaction(relatedDispatcherTransactionId));
          }

          toResolve.push(incomerTransactionStore.deleteTransaction(incomerTransactionId));

          continue;
        }

        toResolve.push(
          concernedIncomerStore.setTransaction({
            ...incomerTransaction,
            redisMetadata: {
              ...incomerTransaction.redisMetadata,
              origin: concernedIncomer.providedUUID
            }
          }),
          incomerTransactionStore.deleteTransaction(incomerTransactionId)
        );

        continue;
      }
    }

    await Promise.all(toResolve);

    for (const [dispatcherTransactionId, dispatcherTransaction] of Object.entries(dispatcherTransactions)) {
      if (dispatcherTransaction.redisMetadata.to === incomerUUID && dispatcherTransaction.name === "ping") {
        await this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId);
      }
    }
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

  private async checkForDistributableMainTransactions(backedUpTransactions: Transactions<"incomer">) {
    for (const [backedUpTransactionId, backedUpTransaction] of backedUpTransactions.entries()) {
      if (!backedUpTransaction.mainTransaction) {
        continue;
      }

      const incomers = await this.getTree();

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

      await Promise.all([
        concernedIncomerStore.setTransaction({
          ...backedUpTransaction,
          redisMetadata: {
            ...backedUpTransaction.redisMetadata,
            origin: concernedIncomer.providedUUID
          }
        }),
        this.backupIncomerTransactionStore.deleteTransaction(backedUpTransactionId)
      ]);
    }
  }

  private async resolveDispatcherTransactions(
    dispatcherTransactions: Transactions<"dispatcher">
  ) {
    const backedUpTransactions = await this.backupIncomerTransactionStore.getTransactions();

    await this.checkForDistributableMainTransactions(backedUpTransactions);

    for (const [dispatcherTransactionId, dispatcherTransaction] of dispatcherTransactions.entries()) {
      // If Transaction is already resolved, skip
      if (dispatcherTransaction.resolved) {
        continue;
      }

      if (dispatcherTransaction.redisMetadata.to) {
        const tree = await this.getTree();

        if (!tree[dispatcherTransaction.redisMetadata.to]) {
          const relatedTransactionId = Object.keys(backedUpTransactions).find(
            (backedUpTransactionId) => backedUpTransactions[backedUpTransactionId].relatedTransaction ===
              dispatcherTransactionId
          );

          if (!relatedTransactionId) {
            continue;
          }

          dispatcherTransaction.resolved = true;
          await Promise.all([
            this.updateIncomerState(backedUpTransactions[relatedTransactionId]),
            this.backupIncomerTransactionStore.deleteTransaction(relatedTransactionId),
            this.dispatcherTransactionStore.updateTransaction(dispatcherTransactionId, dispatcherTransaction)
          ]);

          continue;
        }

        const prefix = tree[dispatcherTransaction.redisMetadata.to].prefix ?? "";
        const relatedIncomerTransactionStore = new TransactionStore({
          prefix: `${prefix ? `${prefix}-` : ""}${dispatcherTransaction.redisMetadata.to}`,
          instance: "incomer"
        });

        const relatedIncomerTransactions = await relatedIncomerTransactionStore.getTransactions();

        const relatedTransactionId = [...relatedIncomerTransactions.keys()].find(
          (incomerTransactionId) => relatedIncomerTransactions.get(incomerTransactionId).relatedTransaction ===
            dispatcherTransactionId
        );

        // Event not resolved yet
        if (!relatedTransactionId) {
          continue;
        }

        // Only in case of ping event
        if (dispatcherTransaction.mainTransaction) {
          await Promise.all([
            this.updateIncomerState(relatedIncomerTransactions.get(relatedTransactionId)),
            relatedIncomerTransactionStore.deleteTransaction(relatedTransactionId),
            this.dispatcherTransactionStore.deleteTransaction(dispatcherTransactionId)
          ]);

          continue;
        }

        dispatcherTransaction.resolved = true;
        await Promise.all([
          this.updateIncomerState(relatedIncomerTransactions.get(relatedTransactionId)),
          relatedIncomerTransactionStore.deleteTransaction(relatedTransactionId),
          this.dispatcherTransactionStore.updateTransaction(dispatcherTransactionId, dispatcherTransaction)
        ]);
      }
    }
  }

  private async resolveIncomerMainTransactions(
    dispatcherTransactions: Transactions<"dispatcher">
  ) {
    const incomerTree = await this.getTree();

    // If Each related transaction resolved => cast internal event to call resolve on Main transaction with according transaction tree ?
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

        const unResolvedRelatedTransactions = [];
        for (const relatedTransaction of unResolvedRelatedTransactions) {
          if (!dispatcherTransactions.get(relatedTransaction).resolved) {
            unResolvedRelatedTransactions.push(relatedTransaction);
          }
        }

        // Event not resolved yet by the different incomers
        if (unResolvedRelatedTransactions.length > 0) {
          continue;
        }

        const transactionToResolve: Promise<void>[] = [];

        for (const relatedDispatcherTransactionId of relatedDispatcherTransactionsId) {
          transactionToResolve.push(this.updateIncomerState(
            incomerTransactions.get(dispatcherTransactions.get(relatedDispatcherTransactionId).relatedTransaction)
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

  private async updateIncomerState(transaction: Transaction<"incomer">) {
    const { aliveSince, redisMetadata } = transaction;
    const { origin } = redisMetadata;
    const tree = await this.getTree();

    if (!tree[origin]) {
      throw new Error("Couldn't find the related incomer");
    }

    // Based on incomer transaction or dispatcher resolution ?
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

      if (formattedMessage.name === "register") {
        const eventValidationSchema = this.eventsValidationFunction.get(formattedMessage.name) as
          ValidateFunction<Record<string, any>> | null;

        if (!eventValidationSchema) {
          throw new Error("Unknown Event");
        }

        if (!eventValidationSchema(formattedMessage)) {
          throw new Error("Malformed message");
        }
      }
      else {
        const eventValidationSchema = this.eventsValidationFunction.get(formattedMessage.name) as
          CustomEventsValidationFunctions | null;

        if (!eventValidationSchema) {
          throw new Error("Unknown Event");
        }
      }

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
      .filter((incomer) => incomer.eventsSubscribe.find((subscribedEvent) => subscribedEvent.name === name));

    if (concernedIncomers.length === 0) {
      this.logger.warn(logData, "No concerned Incomer found");

      // Store in recovery state
      return;
    }

    const filteredConcernedIncomers: RegisteredIncomer[] = [];
    for (const incomer of concernedIncomers) {
      const relatedEvent = incomer.eventsSubscribe.find((value) => value.name === name);

      // Prevent publishing an event to multiple instance of a same service if no horizontalScale of the event
      if (!relatedEvent.horizontalScale && filteredConcernedIncomers.find((value) => value.name === incomer.name)) {
        continue;
      }

      filteredConcernedIncomers.push(incomer);
    }


    // All or nothing ?
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

      await this.publishEvent({
        concernedChannel: relatedChannel,
        transactionMeta: {
          mainTransaction: false,
          relatedTransaction: redisMetadata.transactionId,
          resolved: false
        },
        formattedEvent
      });

      this.logger.info(logData, "redistributed injected event");
    }
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
