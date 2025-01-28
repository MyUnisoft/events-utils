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
  validate,
  Dispatcher,
  Incomer,
  eventsValidationFn,
  type EventOptions,
  type Events
} from "../../../../src/index.js";
import { Transaction, TransactionStore } from "../../../../src/class/store/transaction.class.js";

// Internal Dependencies Mocks
const incomerLogger = pino({
  level: "debug"
});
const mockedEventComeBackHandler = jest.fn().mockImplementation(() => Ok({ status: "RESOLVED" }));

async function updateRegisterTransactionState(
  publisherOldTransacStore: TransactionStore<"incomer">,
  approvementTransactionId: string
) {
  const [_, registerTransaction] = [...await publisherOldTransacStore.getTransactions()].find(([id, transac]) => transac.name === "REGISTER")!;

  await publisherOldTransacStore.updateTransaction(registerTransaction.redisMetadata.transactionId!, {
    ...registerTransaction,
    redisMetadata: {
      ...registerTransaction!.redisMetadata,
      relatedTransaction: approvementTransactionId,
      resolved: true
    }
  } as Transaction<"incomer">);
}

interface InitDispatcherInstanceOptions {
  pingInterval?: number;
  checkLastActivityInterval?: number;
  checkTransactionInterval?: number;
  idleTime?: number;
}

const redis = new RedisAdapter({
    port: Number(process.env.REDIS_PORT),
    host: process.env.REDIS_HOST
  });
const subscriber = new RedisAdapter({
  port: Number(process.env.REDIS_PORT),
  host: process.env.REDIS_HOST
});

async function initDispatcherInstance(
  options: InitDispatcherInstanceOptions = {
    pingInterval: 10_000,
    checkLastActivityInterval: 15_000,
    idleTime: 15_000,
    checkTransactionInterval: 1_000
  }
): Promise<Dispatcher<EventOptions<keyof Events>>> {
  const { pingInterval, checkTransactionInterval, idleTime, checkLastActivityInterval } = options;

  const dispatcher = new Dispatcher<EventOptions<keyof Events>>({
    redis,
    subscriber,
    pingInterval,
    checkLastActivityInterval,
    checkTransactionInterval,
    idleTime,
    eventsValidation: {
      eventsValidationFn,
      customValidationCbFn: validate
    }
   });

  await dispatcher.initialize();

  return dispatcher;
}

async function handleRegistration(instance: Incomer, message: any) {
  const { data } = message;

  Reflect.set(instance, "incomerChannelName", data.uuid);
  Reflect.set(instance, "providedUUID", data.uuid);

  instance["subscriber"]!.subscribe(data.uuid);

  Reflect.set(instance, "incomerChannel", new Channel({
    name: data.uuid
  }));

  await updateRegisterTransactionState(
    instance["defaultIncomerTransactionStore"],
    message.redisMetadata.transactionId
  );

  const instanceTransactionStore = new TransactionStore({
    adapter: redis,
    prefix: data.uuid,
    instance: "incomer"
  });

  Reflect.set(instance, "newTransactionStore", instanceTransactionStore);

  return { instanceTransactionStore };
}

beforeAll(async() => {
  await redis.initialize();
    await subscriber.initialize();

    await redis.flushall();
});

afterAll(async() => {
  await redis.close();
    await subscriber.close();
});

afterEach(async() => {
  jest.clearAllMocks();
  await redis.close();
  await subscriber.close();
});

describe("Publishing an event without concerned Incomer", () => {
  let dispatcher: Dispatcher<EventOptions<keyof Events>>;
  let publisher: Incomer;
  let unConcernedIncomer: Incomer;
  let publisherTransactionStore: TransactionStore<"incomer">;
  let unConcernedTransactionStore: TransactionStore<"incomer">;
  let mockedPublisherSetTransaction;
  let mockedUnConcernedSetTransaction;

  // CONSTANTS
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

  afterAll(async() => {
    await publisher.close();
    await unConcernedIncomer.close();

    await dispatcher.close();
  });

  beforeAll(async() => {
    dispatcher = await initDispatcherInstance();

    publisher = new Incomer({
      redis,
      subscriber,
      name: "foo",
      logger: incomerLogger,
      eventsCast: ["accountingFolder"],
      eventsSubscribe: [],
      eventCallback: mockedEventComeBackHandler
    });

    unConcernedIncomer = new Incomer({
      redis,
      subscriber,
      name: "bar",
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [],
      eventCallback: mockedEventComeBackHandler
    });

    let index = 0;
    jest.spyOn(Incomer.prototype as any, "handleApprovement")
      .mockImplementation(async(message: any) => {
        const { data } = message;

        if (index === 0) {
          Reflect.set(publisher, "incomerChannelName", data.uuid);
          Reflect.set(publisher, "providedUUID", data.uuid);

          publisher["subscriber"]!.subscribe(data.uuid);

          Reflect.set(publisher, "incomerChannel", new Channel({
            name: data.uuid
          }));

          publisherTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: data.uuid,
            instance: "incomer"
          });

          mockedPublisherSetTransaction = jest.spyOn(publisherTransactionStore, "setTransaction");

          Reflect.set(publisher, "incomerTransactionStore", publisherTransactionStore);

          publisher.emit("registered");
        }
        else {
          Reflect.set(unConcernedIncomer, "incomerChannelName", data.uuid);
          Reflect.set(unConcernedIncomer, "providedUUID", data.uuid);

          unConcernedIncomer["subscriber"]!.subscribe(data.uuid);

          Reflect.set(unConcernedIncomer, "incomerChannel", new Channel({
            name: data.uuid
          }));

          unConcernedTransactionStore = new TransactionStore({
            adapter: redis,
            prefix: data.uuid,
            instance: "incomer"
          });

          mockedUnConcernedSetTransaction = jest.spyOn(unConcernedTransactionStore, "setTransaction");

          Reflect.set(unConcernedIncomer, "incomerTransactionStore", unConcernedTransactionStore);

          unConcernedIncomer.emit("registered");
        }

        index++
    });

    await publisher.initialize();
    await unConcernedIncomer.initialize();

    await publisher.publish(event);
  });

  test("callback function must not have been call & incomer shouldn't have create the relating transaction", async() => {
    await timers.setTimeout(1_600);

    expect(mockedPublisherSetTransaction).not.toHaveBeenCalledWith({
      ...event,
      redisMetadata: expect.anything(),
      mainTransaction: false,
      resolved: false,
      relatedTransaction: expect.anything()
    });
    expect(mockedUnConcernedSetTransaction).not.toHaveBeenCalledWith({
      ...event,
      redisMetadata: expect.anything(),
      mainTransaction: false,
      resolved: false,
      relatedTransaction: expect.anything()
    });
    expect(mockedEventComeBackHandler).not.toHaveBeenCalled();
  });
});

describe("Event that doesn't scale", () => {
  let dispatcher: Dispatcher<EventOptions<keyof Events>>;
  let publisher: Incomer;
  let concernedIncomer: Incomer;
  let secondConcernedIncomer: Incomer;
  let diffConcernedIncomer: Incomer;
  let publisherTransactionStore: TransactionStore<"incomer">;
  let mockedPublisherSetTransaction;
  let mockedIncomerSetTransaction;
  let mockedSecondIncomerSetTransaction;
  let mockedDiffIncomerSetTransaction;

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
      createdAt: Date.now(),
        origin: {
          endpoint: "/foo",
          method: "POST",
          requestId: "1"
        }
    }
  };

  beforeAll(async() => {
    dispatcher = await initDispatcherInstance();

    publisher = new Incomer({
      redis,
      subscriber,
      name: "foo",
      logger: incomerLogger,
      eventsCast: ["accountingFolder"],
      eventsSubscribe: [],
      eventCallback: mockedEventComeBackHandler
    });

    concernedIncomer = new Incomer({
      redis,
      subscriber,
      name: "bar",
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [{ name: "accountingFolder" }],
      eventCallback: mockedEventComeBackHandler
    });

    secondConcernedIncomer = new Incomer({
      redis,
      subscriber,
      name: "bar",
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [{ name: "accountingFolder" }],
      eventCallback: mockedEventComeBackHandler
    });

    diffConcernedIncomer = new Incomer({
      redis,
      subscriber,
      name: "foo-bar",
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [{ name: "accountingFolder" }],
      eventCallback: mockedEventComeBackHandler
    });

    let index = 0;
    jest.spyOn(Incomer.prototype as any, "handleApprovement")
      .mockImplementation(async(message: any) => {
        if (index === 0) {
          const { instanceTransactionStore } = await handleRegistration(publisher, message);

          publisherTransactionStore = instanceTransactionStore;
          mockedPublisherSetTransaction = jest.spyOn(instanceTransactionStore, "setTransaction");

          publisher.emit("registered");
        }
        else if (index === 1) {
          const { instanceTransactionStore } = await handleRegistration(concernedIncomer, message);
          mockedIncomerSetTransaction = jest.spyOn(instanceTransactionStore, "setTransaction");

          concernedIncomer.emit("registered");
        }
        else if (index === 2) {
          const { instanceTransactionStore } = await handleRegistration(secondConcernedIncomer, message);
          mockedSecondIncomerSetTransaction = jest.spyOn(instanceTransactionStore, "setTransaction");

          secondConcernedIncomer.emit("registered");
        }
        else {
          const { instanceTransactionStore } = await handleRegistration(diffConcernedIncomer, message);
          mockedDiffIncomerSetTransaction = jest.spyOn(instanceTransactionStore, "setTransaction");

          diffConcernedIncomer.emit("registered");
        }

        index++;
    });

    await publisher.initialize();
    await concernedIncomer.initialize();
    await secondConcernedIncomer.initialize();
    await diffConcernedIncomer.initialize();

    await timers.setTimeout(1_600);

    await publisher.publish(event);
  });

  afterAll(async() => {
    await publisher.close();
    await concernedIncomer.close();
    await secondConcernedIncomer.close();
    await diffConcernedIncomer.close();

    await dispatcher.close();
  });

  test("callback function must have been call & both diff incomers should have create the relating transaction", async() => {
    expect(mockedPublisherSetTransaction).toHaveBeenCalledWith({
      ...event,
      redisMetadata: {
        incomerName: publisher.name,
        origin: expect.any(String),
        prefix: publisher.prefix,
        published: false,
        mainTransaction: true,
        resolved: false,
        relatedTransaction: null
      }
    });

    await timers.setTimeout(12_000);

    const mockedEvent = {
      ...event,
      redisMetadata: {
        eventTransactionId: expect.any(String),
        incomerName: expect.any(String),
        origin: expect.any(String),
        prefix: publisher.prefix,
        mainTransaction: false,
        iteration: expect.any(Number),
        resolved: false,
        to: expect.any(String),
        relatedTransaction: expect.any(String),
        transactionId: expect.any(String)
      }
    };

    const mockIncomerSetTransactionCalls = mockedIncomerSetTransaction.mock.calls.flat() as Array<any>;
    const mockSecondIncomerSetTransactionCalls = mockedSecondIncomerSetTransaction.mock.calls.flat() as Array<any>;
    const mockDiffIncomerSetTransactionCalls = mockedDiffIncomerSetTransaction.mock.calls.flat() as Array<any>;

    if (mockIncomerSetTransactionCalls.find((mockedEventCall) => mockedEventCall.name === event.name)) {
      expect(mockIncomerSetTransactionCalls).toEqual(expect.arrayContaining([
        expect.objectContaining(mockedEvent)
      ]));
      expect(mockSecondIncomerSetTransactionCalls).not.toEqual(expect.arrayContaining([
        expect.objectContaining(mockedEvent)
      ]));
    }
    else {
      expect(mockIncomerSetTransactionCalls).not.toEqual(expect.arrayContaining([
        expect.objectContaining(mockedEvent)
      ]));
      expect(mockSecondIncomerSetTransactionCalls).toEqual(expect.arrayContaining([
        expect.objectContaining(mockedEvent)
      ]));
    }

    expect(mockDiffIncomerSetTransactionCalls).toEqual(expect.arrayContaining([
      expect.objectContaining(mockedEvent)
    ]));

    expect(mockedEventComeBackHandler).toHaveBeenCalledTimes(2);
    expect(mockedEventComeBackHandler).toHaveBeenCalledWith({
      ...event,
      eventTransactionId: expect.anything()
    });
  });

  test("Then the dispatcher should have delete the mainTransaction on the publisherTransactionStore", async() => {
    await timers.setTimeout(2_400);

    const publisherTransactions = await publisherTransactionStore.getTransactions();
    expect(publisherTransactions).not.toContain({
      ...event,
      redisMetadata: expect.anything(),
      relatedTransaction: null,
      mainTransaction: true,
      resolved: false
    });
  });
});

describe("Event that scale", () => {
  let dispatcher: Dispatcher<EventOptions<keyof Events>>;
  let publisher: Incomer;
  let concernedIncomer: Incomer;
  let secondConcernedIncomer: Incomer;
  let diffConcernedIncomer: Incomer;
  let publisherTransactionStore: TransactionStore<"incomer">;
  let mockedPublisherSetTransaction: jest.SpyInstance;
  let mockedIncomerSetTransaction;
  let mockedSecondIncomerSetTransaction;
  let mockedDiffIncomerSetTransaction;

  // CONSTANTS
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
  }

  afterAll(async() => {
    await publisher.close();
    await concernedIncomer.close();
    await secondConcernedIncomer.close();
    await diffConcernedIncomer.close();

    await dispatcher.close();
  });

  beforeAll(async() => {
    dispatcher = await initDispatcherInstance();

    publisher = new Incomer({
      redis,
      subscriber,
      name: "foo",
      logger: incomerLogger,
      eventsCast: ["accountingFolder"],
      eventsSubscribe: [],
      eventCallback: mockedEventComeBackHandler
    });

    concernedIncomer = new Incomer({
      redis,
      subscriber,
      name: "bar",
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [{ name: "accountingFolder", horizontalScale: true }],
      eventCallback: mockedEventComeBackHandler
    });

    secondConcernedIncomer = new Incomer({
      redis,
      subscriber,
      name: "bar",
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [{ name: "accountingFolder", horizontalScale: true }],
      eventCallback: mockedEventComeBackHandler
    });

    diffConcernedIncomer = new Incomer({
      redis,
      subscriber,
      name: "foo-bar",
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [{ name: "accountingFolder", horizontalScale: true }],
      eventCallback: mockedEventComeBackHandler
    });

    let index = 0;
    jest.spyOn(Incomer.prototype as any, "handleApprovement")
      .mockImplementation(async(message: any) => {
        if (index === 0) {
          const { instanceTransactionStore } = await handleRegistration(publisher, message);

          publisherTransactionStore = instanceTransactionStore;
          mockedPublisherSetTransaction = jest.spyOn(instanceTransactionStore, "setTransaction");

          publisher.emit("registered");
        }
        else if (index === 1) {
          const { instanceTransactionStore } = await handleRegistration(concernedIncomer, message);
          mockedIncomerSetTransaction = jest.spyOn(instanceTransactionStore, "setTransaction");

          concernedIncomer.emit("registered");
        }
        else if (index === 2) {
          const { instanceTransactionStore } = await handleRegistration(secondConcernedIncomer, message);
          mockedSecondIncomerSetTransaction = jest.spyOn(instanceTransactionStore, "setTransaction");

          secondConcernedIncomer.emit("registered");
        }
        else {
          const { instanceTransactionStore } = await handleRegistration(diffConcernedIncomer, message);
          mockedDiffIncomerSetTransaction = jest.spyOn(instanceTransactionStore, "setTransaction");

          diffConcernedIncomer.emit("registered");
        }

        index++;
    });

    await publisher.initialize();
    await concernedIncomer.initialize();
    await secondConcernedIncomer.initialize();
    await diffConcernedIncomer.initialize();

    await timers.setTimeout(1_000);

    await publisher.publish(event);
  });

  test("callback function must have been call & every incomers should have create the relating transaction", async() => {
    const mockResults = mockedPublisherSetTransaction.mock.calls.flat();

    expect(mockResults).toEqual(expect.arrayContaining([
      expect.objectContaining({
        ...event,
        redisMetadata: {
          incomerName: publisher.name,
          origin: expect.any(String),
          prefix: publisher.prefix,
          published: false,
          mainTransaction: true,
          resolved: false,
          relatedTransaction: null
        }
      })
    ]));

      await timers.setTimeout(5_000);

    const mockedEvent = {
      ...event,
      redisMetadata: {
        eventTransactionId: expect.any(String),
        origin: expect.any(String),
        incomerName: expect.any(String),
        to: expect.any(String),
        transactionId: expect.any(String),
        iteration: expect.any(Number),
        mainTransaction: false,
        resolved: false,
        relatedTransaction: expect.any(String)
      }
    };

    expect(mockedIncomerSetTransaction).toHaveBeenCalledWith(mockedEvent);
    expect(mockedSecondIncomerSetTransaction).toHaveBeenCalledWith(mockedEvent);
    expect(mockedDiffIncomerSetTransaction).toHaveBeenCalledWith(mockedEvent);

    expect(mockedEventComeBackHandler).toHaveBeenCalledTimes(3);
    expect(mockedEventComeBackHandler).toHaveBeenCalledWith({
      ...event,
      eventTransactionId: expect.anything()
    });

    await timers.setTimeout(2_400);

    const SecondPublisherTransactions = await publisherTransactionStore.getTransactions();
    expect(SecondPublisherTransactions.values()).not.toContainEqual({
      ...event,
      redisMetadata: expect.anything(),
      published: true,
      relatedTransaction: null,
      mainTransaction: true,
      resolved: false
    });
  });
});
