// Import Node.js Dependencies
import assert from "node:assert";
import { describe, before, after, test } from "node:test";
import { randomUUID } from "node:crypto";
import { setTimeout} from "node:timers/promises";

// Import Third-Party Dependencies
import {
  RedisAdapter,
  Redis
} from "@myunisoft/redis";
import pino, { Logger } from "pino";

// Import Internal Dependencies
import {
  TransactionHandler
} from "../../../../../src/class/eventManagement/dispatcher/transaction-handler.class";
import { TransactionStore } from "../../../../../src/class/store/transaction.class";
import { IncomerChannelHandler } from "../../../../../src/class/eventManagement/dispatcher/incomer-channel.class";
import { IncomerStore } from "../../../../../src/class/store/incomer.class";
import { EventsHandler } from "../../../../../src/class/eventManagement/dispatcher/events.class";
import { EventOptions, GenericEvent } from "../../../../../src/types";
import { createResolvedTransactions, createUnresolvedTransactions } from "../../../../utils/transactions";

// CONSTANTS
const kPrefix = "test";
const kDispatcher = "dispatcher";
const kBackupTransactionStoreName = "backup";
const kIdleTime = 60_000;

describe("transactionHandler", () => {
  const redis = new RedisAdapter({
    port: Number(process.env.REDIS_PORT),
    host: process.env.REDIS_HOST
  });
  const foo = new RedisAdapter({
    port: Number(process.env.REDIS_PORT),
    host: process.env.REDIS_HOST
  });
  const subscriber = new RedisAdapter({
    port: Number(process.env.REDIS_PORT),
    host: process.env.REDIS_HOST
  });

  before(async() => {
    await redis.initialize();
    await foo.initialize();
    await subscriber.initialize();

    await redis.flushall();
  });

  after(async() => {
    await redis.close();
    await foo.close();
    await subscriber.close();
  });

  describe("transactionHandler with default options", () => {
    const formattedPrefix: string = `${kPrefix}-`;
    const privateUUID: string = randomUUID();

    const logger: Logger = pino({
      name: formattedPrefix + kDispatcher,
      level: "debug",
      transport: {
        target: "pino-pretty"
      }
    });

    const incomerStore: IncomerStore = new IncomerStore({
      adapter: redis,
      prefix: kPrefix,
      idleTime: kIdleTime
    });

    const dispatcherTransactionStore: TransactionStore<"dispatcher"> = new TransactionStore({
      adapter: redis,
      prefix: kPrefix,
      instance: "dispatcher"
    });

    const backupDispatcherTransactionStore: TransactionStore<"dispatcher"> = new TransactionStore({
      adapter: redis,
      prefix: formattedPrefix + kBackupTransactionStoreName,
      instance: "dispatcher"
    });

    const backupIncomerTransactionStore: TransactionStore<"incomer"> = new TransactionStore({
      adapter: redis,
      prefix: formattedPrefix + kBackupTransactionStoreName,
      instance: "incomer"
    });

    const incomerChannelHandler: IncomerChannelHandler = new IncomerChannelHandler({
      logger
    });

    const eventsHandler: EventsHandler<GenericEvent> = new EventsHandler({
      privateUUID,
      dispatcherChannelName: kDispatcher,
      parentLogger: logger
    })

    const transactionHandler: TransactionHandler = new TransactionHandler({
      redis,
      privateUUID,
      formattedPrefix,
      incomerStore,
      dispatcherTransactionStore,
      backupDispatcherTransactionStore,
      backupIncomerTransactionStore,
      incomerChannelHandler,
      eventsHandler,
      parentLogger: logger
    });

    test("transactionHandler must have property \"logger\" and \"standardLogFn\"", () => {
      assert.ok(transactionHandler["logger"]);
      assert.ok(transactionHandler["standardLogFn"]);
    });

    describe("resolveInactiveIncomerTransactions", () => {
      const connectorEvent: EventOptions<"connector"> = {
        name: "connector",
        scope: {
          schemaId: 1
        },
        operation: "CREATE",
        metadata: {
          agent: "node",
          createdAt: Date.now()
        },
        data: {
          id: "1",
          code: "foo"
        }
      };

      describe("Given a spread transaction resolved by the listener", () => {
        describe("Given no available backup", () => {
          const now = Date.now();

          const publisher = {
            name: "publisher",
            prefix: kPrefix,
            eventsCast: [connectorEvent.name],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const dispatcher = {
            name: "dispatcher",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const listener = {
            name: "listener",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [{ name: connectorEvent.name }],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };

          const publisherTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${publisher.providedUUID}`,
            instance: "incomer"
          });

          const listenerTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${listener.providedUUID}`,
            instance: "incomer"
          });

          let spreadedEvent;

          before(async() => {
            await incomerStore.setIncomer(publisher, publisher.providedUUID);
            await incomerStore.setIncomer(listener, listener.providedUUID);

            spreadedEvent = await createResolvedTransactions({
              publisher: {
                transactionStore: publisherTransactionStore,
                instance: publisher
              },
              dispatcher: {
                transactionStore: dispatcherTransactionStore,
                instance: dispatcher
              },
              event: connectorEvent,
              listener: {
                transactionStore: listenerTransactionStore,
                instance: listener
              }
            });
          });

          after(async() => {
            await redis.flushall();
          });

          test("it should backup the handler transaction", async() => {
            await transactionHandler.resolveInactiveIncomerTransactions(listener);

            const resolvedBackupTransaction = (await backupIncomerTransactionStore.getTransactions())
              .get(spreadedEvent.handlerTransaction.redisMetadata.transactionId);

            assert.equal(resolvedBackupTransaction!.redisMetadata.relatedTransaction, spreadedEvent.spreadTransaction.redisMetadata.transactionId);
            assert.equal(resolvedBackupTransaction!.redisMetadata.incomerName, listener.name);
          });
        });

        describe("Given an available backup", () => {
          const now = Date.now();

          const publisher = {
            name: "publisher",
            prefix: kPrefix,
            eventsCast: [connectorEvent.name],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const dispatcher = {
            name: "dispatcher",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const listener = {
            name: "listener",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [{ name: connectorEvent.name }],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const backupListener = {
            name: "listener",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [{ name: connectorEvent.name }],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };

          const publisherTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${publisher.providedUUID}`,
            instance: "incomer"
          });

          const listenerTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${listener.providedUUID}`,
            instance: "incomer"
          });

          let resolvedEvent;

          before(async() => {
            await incomerStore.setIncomer(publisher, publisher.providedUUID);
            await incomerStore.setIncomer(listener, listener.providedUUID);
            await incomerStore.setIncomer(backupListener, backupListener.providedUUID);

            resolvedEvent = await createResolvedTransactions({
              publisher: {
                transactionStore: publisherTransactionStore,
                instance: publisher
              },
              dispatcher: {
                transactionStore: dispatcherTransactionStore,
                instance: dispatcher
              },
              event: connectorEvent,
              listener: {
                transactionStore: listenerTransactionStore,
                instance: listener
              }
            });
          });

          after(async() => {
            await redis.flushall();
          });

          test("it should backup the handler transaction to the incomerBackupTransactionStore", async() => {
            await transactionHandler.resolveInactiveIncomerTransactions(listener);

            const resolvedBackupTransaction = (await backupIncomerTransactionStore.getTransactions())
              .get(resolvedEvent.handlerTransaction.redisMetadata.transactionId);

            assert.equal(resolvedBackupTransaction!.redisMetadata.relatedTransaction, resolvedEvent.spreadTransaction.redisMetadata.transactionId);
            assert.equal(resolvedBackupTransaction!.redisMetadata.incomerName, listener.name);
          });
        });
      });

      describe("Given a spread transaction unresolved by the listener", () => {
        describe("Given no available backup", () => {
          const now = Date.now();

          const publisher = {
            name: "publisher",
            prefix: kPrefix,
            eventsCast: [connectorEvent.name],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const dispatcher = {
            name: "dispatcher",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const listener = {
            name: "listener",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [{ name: connectorEvent.name }],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };

          const publisherTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${publisher.providedUUID}`,
            instance: "incomer"
          });

          const listenerTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${listener.providedUUID}`,
            instance: "incomer"
          });

          let resolvedEvent;

          before(async() => {
            await incomerStore.setIncomer(publisher, publisher.providedUUID);
            await incomerStore.setIncomer(listener, listener.providedUUID);

            resolvedEvent = await createUnresolvedTransactions({
              publisher: {
                transactionStore: publisherTransactionStore,
                instance: publisher
              },
              dispatcher: {
                transactionStore: dispatcherTransactionStore,
                instance: dispatcher
              },
              event: connectorEvent,
              listener: {
                transactionStore: listenerTransactionStore,
                instance: listener
              }
            });
          });

          after(async() => {
            await redis.flushall();
          });

          test("it should backup the handler transaction to the incomerBackupTransactionStore", async() => {
            await transactionHandler.resolveInactiveIncomerTransactions(listener);

            const resolvedBackupTransaction = (await backupIncomerTransactionStore.getTransactions())
              .get(resolvedEvent.spreadTransaction.redisMetadata.transactionId);

            assert.equal((resolvedBackupTransaction!.redisMetadata as any).eventTransactionId, resolvedEvent.spreadTransaction.redisMetadata.eventTransactionId);
            assert.equal(resolvedBackupTransaction!.redisMetadata.incomerName, listener.name);
          });
        });

        describe("Given an available backup", () => {
          const now = Date.now();

          const publisher = {
            name: "publisher",
            prefix: kPrefix,
            eventsCast: [connectorEvent.name],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const dispatcher = {
            name: "dispatcher",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const listener = {
            name: "listener",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [{ name: connectorEvent.name }],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const backupListener = {
            name: "listener",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [{ name: connectorEvent.name }],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };

          const publisherTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${publisher.providedUUID}`,
            instance: "incomer"
          });

          const listenerTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${listener.providedUUID}`,
            instance: "incomer"
          });

          let backupEvent;

          before(async() => {
            await incomerStore.setIncomer(publisher, publisher.providedUUID);
            await incomerStore.setIncomer(listener, listener.providedUUID);
            await incomerStore.setIncomer(backupListener, backupListener.providedUUID);

            await createUnresolvedTransactions({
              publisher: {
                transactionStore: publisherTransactionStore,
                instance: publisher
              },
              dispatcher: {
                transactionStore: dispatcherTransactionStore,
                instance: dispatcher
              },
              event: connectorEvent,
              listener: {
                transactionStore: listenerTransactionStore,
                instance: listener
              }
            });

            await subscriber.subscribe(`${kPrefix}-${backupListener.providedUUID}`);

            subscriber.on("message", (__: string, message: string) => {
              backupEvent = message;
            });
          });

          after(async() => {
            await redis.flushall();
          });

          test("it should backup the event to the backupListener", async() => {
            await transactionHandler.resolveInactiveIncomerTransactions(listener);

            await setTimeout(1_000);

            assert.ok(backupEvent);
          });
        });
      });

      describe("Given a main transaction", () => {
        describe("Given an available backup", () => {
          const now = Date.now();

          const publisher = {
            name: "publisher",
            prefix: kPrefix,
            eventsCast: [connectorEvent.name],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const backupPublisher = {
            name: "publisher",
            prefix: kPrefix,
            eventsCast: [connectorEvent.name],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const dispatcher = {
            name: "dispatcher",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const listener = {
            name: "listener",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [{ name: connectorEvent.name }],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };

          const publisherTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${publisher.providedUUID}`,
            instance: "incomer"
          });

          const backupPublisherTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${backupPublisher.providedUUID}`,
            instance: "incomer"
          });

          const listenerTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${listener.providedUUID}`,
            instance: "incomer"
          });

          before(async() => {
            await incomerStore.setIncomer(publisher, publisher.providedUUID);
            await incomerStore.setIncomer(listener, listener.providedUUID);
            await incomerStore.setIncomer(backupPublisher, backupPublisher.providedUUID);

            await createResolvedTransactions({
              publisher: {
                transactionStore: publisherTransactionStore,
                instance: publisher
              },
              dispatcher: {
                transactionStore: dispatcherTransactionStore,
                instance: dispatcher
              },
              event: connectorEvent,
              listener: {
                transactionStore: listenerTransactionStore,
                instance: listener
              }
            });
          });

          after(async() => {
            await redis.flushall();
          });

          test("It should backup the main transaction to the backupPublisherTransactionStore", async() => {
            let backupPublisherTransactions = await backupPublisherTransactionStore.getTransactions();

            assert.equal([...backupPublisherTransactions.entries()].length, 0);

            await transactionHandler.resolveInactiveIncomerTransactions(publisher);

            backupPublisherTransactions = await backupPublisherTransactionStore.getTransactions();

            assert.equal([...backupPublisherTransactions.entries()].length, 1);
          });
        });

        describe("Given no available backup", () => {
          const now = Date.now();

          const publisher = {
            name: "publisher",
            prefix: kPrefix,
            eventsCast: [connectorEvent.name],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const dispatcher = {
            name: "dispatcher",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };
          const listener = {
            name: "listener",
            prefix: kPrefix,
            eventsCast: [],
            eventsSubscribe: [{ name: connectorEvent.name }],
            providedUUID: randomUUID(),
            isDispatcherActiveInstance: false,
            baseUUID: randomUUID(),
            lastActivity: now,
            aliveSince: now
          };

          const publisherTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${publisher.providedUUID}`,
            instance: "incomer"
          });

          const listenerTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: `${kPrefix}-${listener.providedUUID}`,
            instance: "incomer"
          });

          before(async() => {
            await incomerStore.setIncomer(publisher, publisher.providedUUID);
            await incomerStore.setIncomer(listener, listener.providedUUID);

            await createResolvedTransactions({
              publisher: {
                transactionStore: publisherTransactionStore,
                instance: publisher
              },
              dispatcher: {
                transactionStore: dispatcherTransactionStore,
                instance: dispatcher
              },
              event: connectorEvent,
              listener: {
                transactionStore: listenerTransactionStore,
                instance: listener
              }
            });
          });

          after(async() => {
            await redis.flushall();
          });

          test("It should backup the main transaction to the backupIncomerTransactionStore", async() => {
            let backupPublisherTransactions = await backupIncomerTransactionStore.getTransactions();

            assert.equal([...backupPublisherTransactions.entries()].length, 0);

            await transactionHandler.resolveInactiveIncomerTransactions(publisher);

            backupPublisherTransactions = await backupIncomerTransactionStore.getTransactions();

            assert.equal([...backupPublisherTransactions.entries()].length, 1);
          });
        });
      })
    });
  });
});
