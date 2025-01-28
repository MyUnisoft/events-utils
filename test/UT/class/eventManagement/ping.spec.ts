// Import Node.js Dependencies
import { randomUUID } from "node:crypto";
import timers from "node:timers/promises";

// Import Third-party Dependencies
import {
  RedisAdapter
} from "@myunisoft/redis";
import { pino } from "pino";
import { Ok } from "@openally/result";

// Import Internal Dependencies
import {
  Dispatcher,
  Incomer
} from "../../../../src/index.js";

const dispatcherLogger = pino({
  level: "debug"
});
const incomerLogger = pino({
  level: "debug"
});
const mockedIncomerLoggerDebug = jest.spyOn(incomerLogger, "debug");

describe("Ping", () => {
  const eventComeBackHandler = jest.fn().mockImplementation(() => Ok({ status: "RESOLVED" }));

  const redis = new RedisAdapter({
    port: Number(process.env.REDIS_PORT),
    host: process.env.REDIS_HOST
  });
  const subscriber = new RedisAdapter({
    port: Number(process.env.REDIS_PORT),
    host: process.env.REDIS_HOST
  });

  let dispatcher: Dispatcher;
  let incomer: Incomer;

  beforeAll(async() => {
    await redis.initialize();
    await subscriber.initialize();

    await redis.flushall();

    dispatcher = new Dispatcher({
      redis,
      subscriber,
      logger: dispatcherLogger,
      pingInterval: 1_600,
      checkLastActivityInterval: 4_000,
      checkTransactionInterval: 2_400,
      idleTime: 4_000
     });

    Reflect.set(dispatcher, "logger", dispatcherLogger);

    await dispatcher.initialize();

    incomer = new Incomer({
      redis,
      subscriber,
      name: randomUUID(),
      logger: incomerLogger,
      eventsCast: [],
      eventsSubscribe: [],
      eventCallback: eventComeBackHandler
    });

    await incomer.initialize();

    await timers.setTimeout(2_000)
  });

  afterAll(async() => {
    await dispatcher.close();
    await incomer.close();
    await redis.close();
    await subscriber.close();
  });

  afterEach(async() => {
    await redis.flushdb();
  });

  test("Dispatcher should have ping", () => {
    expect(mockedIncomerLoggerDebug.mock.calls[0][0]).toContain("Resolved Ping event");
  });
});


