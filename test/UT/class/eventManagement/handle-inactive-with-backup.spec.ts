// Import Node.js Dependencies
import timers from "node:timers/promises";

// Import Third-party Dependencies
import {
  RedisAdapter,
  Channel
} from "@myunisoft/redis";
import { pino } from "pino";
import { Ok } from "@openally/result";

// Import Internal Dependencies
import {
  Dispatcher,
  Incomer,
  eventsValidationFn,
  type EventOptions,
  type Events,
  validate
} from "../../../../src/index.js";
import { TransactionStore } from "../../../../src/class/store/transaction.class.js";

// Internal Dependencies Mocks
const dispatcherLogger = pino({
  level: "debug"
});
const mockedEventComeBackHandler = jest.fn().mockImplementation(() => Ok({ status: "RESOLVED" }));

describe("Publishing/exploiting a custom event & inactive incomer", () => {
  let dispatcher: Dispatcher<EventOptions<keyof Events>>;

  const redis = new RedisAdapter({
    port: Number(process.env.REDIS_PORT),
    host: process.env.REDIS_HOST
  });
  const subscriber = new RedisAdapter({
    port: Number(process.env.REDIS_PORT),
    host: process.env.REDIS_HOST
  });

  beforeAll(async() => {
    await redis.initialize();
    await subscriber.initialize();

    await redis.flushall();

    dispatcher = new Dispatcher({
      redis,
      subscriber,
      logger: dispatcherLogger,
      pingInterval: 2_000,
      checkLastActivityInterval: 2_000,
      checkTransactionInterval: 2_000,
      idleTime: 5_000,
      eventsValidation: {
        eventsValidationFn,
        customValidationCbFn: validate
      }
     });

    await dispatcher.initialize();
  });

  afterAll(async() => {
    await dispatcher.close();
    await redis.close();
    await subscriber.close();
  });

  afterEach(async() => {
    jest.clearAllMocks();
    await redis.flushdb();
  });

  describe("Inactive incomer with back-up available", () => {
    let concernedIncomer: Incomer;
    let secondConcernedIncomer: Incomer;
    let firstIncomerTransactionStore: TransactionStore<"incomer">;
    let secondIncomerTransactionStore: TransactionStore<"incomer">;
    let handlerTransaction;
    let mockedSetTransaction;

    // Constants
    const event: EventOptions<"accountingFolder"> = {
      name: "accountingFolder",
      operation: "CREATE",
      data: {
        id: "1"
      },
      scope: {
        schemaId: 1,
        firmId: 1
      },
      metadata: {
        agent: "jest",
        createdAt: Date.now()
      }
    };

    let handleApprovementIndex = 0;
    jest.spyOn(Incomer.prototype as any, "handleApprovement")
      .mockImplementation(async(message: any) => {
        const { data } = message;

        if (handleApprovementIndex === 0) {
          Reflect.set(concernedIncomer, "incomerChannelName", data.uuid);
          Reflect.set(concernedIncomer, "providedUUID", data.uuid);

          concernedIncomer["subscriber"]!.subscribe(data.uuid);

          Reflect.set(concernedIncomer, "incomerChannel", new Channel({
            port: Number(process.env.REDIS_PORT),
            host: process.env.REDIS_HOST,
            name: data.uuid
          }));

          firstIncomerTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: data.uuid,
            instance: "incomer"
          });

          Reflect.set(concernedIncomer, "newTransactionStore", firstIncomerTransactionStore);

          concernedIncomer["lastPingDate"] = Date.now();
          concernedIncomer.emit("registered");

          handleApprovementIndex++;
        }
        else {
          Reflect.set(secondConcernedIncomer, "incomerChannelName", data.uuid);
          Reflect.set(secondConcernedIncomer, "providedUUID", data.uuid);

          secondConcernedIncomer["subscriber"]!.subscribe(data.uuid);

          Reflect.set(secondConcernedIncomer, "incomerChannel", new Channel({
            port: Number(process.env.REDIS_PORT),
            host: process.env.REDIS_HOST,
            name: data.uuid
          }));

          secondIncomerTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: data.uuid,
            instance: "incomer"
          });

          Reflect.set(secondConcernedIncomer, "newTransactionStore", secondIncomerTransactionStore);

          mockedSetTransaction = jest.spyOn(secondConcernedIncomer["newTransactionStore"], "setTransaction");

          secondConcernedIncomer["lastPingDate"] = Date.now();
          secondConcernedIncomer.emit("registered");
        }
      });

    beforeAll(async() => {
      concernedIncomer = new Incomer({
        redis,
        subscriber,
        name: "foo",
        eventsCast: ["accountingFolder"],
        eventsSubscribe: [{ name: "accountingFolder" }],
        eventsValidation: {
          eventsValidationFn,
          customValidationCbFn: validate
        },
        eventCallback: mockedEventComeBackHandler
      });

      jest.spyOn(concernedIncomer as any, "customEvent")
        .mockImplementation(async(opts: any) => {
          handlerTransaction = await concernedIncomer["newTransactionStore"].setTransaction({
            ...event,
            redisMetadata: {
              ...opts.message.redisMetadata,
              incomerName: concernedIncomer.name,
              origin: opts.message.redisMetadata.to,
              mainTransaction: false,
              relatedTransaction: opts.message.redisMetadata.transactionId,
              resolved: false
            }
          });
        });

      secondConcernedIncomer = new Incomer({
        redis,
        subscriber,
        name: "foo",
        eventsCast: ["accountingFolder"],
        eventsSubscribe: [{ name: "accountingFolder" }],
        eventCallback: mockedEventComeBackHandler
      });

      await concernedIncomer.initialize();

      await concernedIncomer.publish(event);

      await secondConcernedIncomer.initialize();

      await timers.setTimeout(4_000);
    });

    test("expect the second incomer to have handle the event by retaking the main Transaction", async() => {
      expect(handlerTransaction).toBeDefined();

      await concernedIncomer.close();

      secondConcernedIncomer["subscriber"]!.subscribe(
        secondConcernedIncomer["dispatcherChannelName"], secondConcernedIncomer["incomerChannelName"]
      );
      dispatcher["subscriber"]!.on("message", (channel, message) => dispatcher["handleMessages"](channel, message));
      secondConcernedIncomer["subscriber"]!.on("message", (channel, message) => secondConcernedIncomer["handleMessages"](channel, message));

      await timers.setTimeout(15_000);

      const mockCalls = mockedSetTransaction.mock.calls.flat();

      expect(mockCalls).toEqual(expect.arrayContaining([
        expect.objectContaining({
          ...event,
          redisMetadata: {
            origin: expect.anything(),
            to: expect.anything(),
            eventTransactionId: expect.anything(),
            transactionId: expect.anything(),
            incomerName: concernedIncomer.name,
            mainTransaction: false,
            relatedTransaction: expect.anything(),
            resolved: expect.anything(),
            iteration: expect.any(Number)
          },
          aliveSince: expect.anything()
        })
      ]));

      await secondConcernedIncomer.close();
    });
  });
});
